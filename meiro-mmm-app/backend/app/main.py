from fastapi import FastAPI, BackgroundTasks, UploadFile, File, HTTPException, Query, Body, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import pandas as pd
from pathlib import Path
import io
import json
import os
import time
import requests
from datetime import datetime
from app.utils.token_store import save_token, get_token, delete_token, get_all_connected_platforms
from app.utils.encrypt import encrypt, decrypt
from app.utils import datasource_config as ds_config
from app.utils.taxonomy import load_taxonomy, save_taxonomy, Taxonomy
from app.utils.kpi_config import load_kpi_config, save_kpi_config, KpiConfig, KpiDefinition
from app.mmm_engine import fit_model as mmm_fit_model, engine_info
from app.connectors import meiro_cdp
from app.attribution_engine import (
    run_attribution,
    run_all_models as run_all_attribution,
    run_attribution_campaign,
    compute_channel_performance,
    parse_conversion_paths,
    analyze_paths,
    compute_next_best_action,
    has_any_campaign,
    ATTRIBUTION_MODELS,
)

app = FastAPI(title="Meiro Attribution Dashboard API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== Pydantic Models ====================

class ModelConfig(BaseModel):
    dataset_id: str
    frequency: str = "W"
    kpi_mode: str = "conversions"
    kpi: Optional[str] = None
    spend_channels: List[str]
    covariates: Optional[List[str]] = []
    priors: Optional[Dict[str, Any]] = None
    mcmc: Optional[Dict[str, Any]] = None

    def __init__(self, **data):
        if 'priors' not in data or data['priors'] is None:
            data['priors'] = {
                "adstock": {"alpha_mean": 0.5, "alpha_sd": 0.2},
                "saturation": {"lam_mean": 0.001, "lam_sd": 0.0005}
            }
        if 'mcmc' not in data or data['mcmc'] is None:
            data['mcmc'] = {"draws": 1000, "tune": 1000, "chains": 4, "target_accept": 0.9}
        if 'kpi' not in data or data['kpi'] is None:
            if 'kpi_mode' in data:
                data['kpi'] = {'conversions': 'conversions', 'aov': 'aov', 'profit': 'profit'}.get(data['kpi_mode'], 'conversions')
        super().__init__(**data)

class ChannelConstraint(BaseModel):
    """Optional per-channel constraints for automatic optimization.

    The optimizer works with *relative multipliers* around the current allocation,
    not absolute currency units. A value of 1.0 means "keep at baseline", 2.0
    means "double relative weight", etc.
    """

    min: float | None = None
    max: float | None = None
    locked: bool = False


class OptimizeRequest(BaseModel):
    """Global + per-channel configuration for automatic budget optimization.

    total_budget:
        Interpreted as the *average* multiplier across channels. With N channels,
        the optimizer enforces sum(x_i) = total_budget * N, where x_i are the
        per-channel multipliers around the current mix.
        A value of 1.0 keeps the overall level at baseline.

    min_spend / max_spend:
        Global lower/upper bounds for each channel's multiplier, used as
        defaults when no per-channel constraint is provided.

    channel_constraints:
        Optional fine-grained constraints per channel. For a given channel:
        - if locked=True, the multiplier is fixed at 1.0 (baseline)
        - otherwise, min/max override the global bounds if provided.
    """

    total_budget: float = 1.0
    min_spend: float = 0.5
    max_spend: float = 2.0
    channel_constraints: Dict[str, ChannelConstraint] | None = None

class MeiroCDPConnectRequest(BaseModel):
    api_base_url: str
    api_key: str

class MeiroCDPExportRequest(BaseModel):
    since: str
    until: str
    event_types: Optional[List[str]] = None
    attributes: Optional[List[str]] = None
    segment_id: Optional[str] = None

class ExpenseEntry(BaseModel):
    channel: str
    amount: float
    period: Optional[str] = None  # e.g. "2024-01"
    notes: Optional[str] = None

class AttributionMappingConfig(BaseModel):
    touchpoint_attr: str = "touchpoints"
    value_attr: str = "conversion_value"
    id_attr: str = "customer_id"
    channel_field: str = "channel"
    timestamp_field: str = "timestamp"

# ==================== In-memory State ====================

RUNS: Dict[str, Any] = {}
DATASETS: Dict[str, Dict[str, Any]] = {}
EXPENSES: Dict[str, ExpenseEntry] = {}  # key: "{channel}_{period}"
JOURNEYS: List[Dict] = []  # Current loaded journeys
ATTRIBUTION_RESULTS: Dict[str, Any] = {}  # Cached attribution results

SAMPLE_DIR = Path(__file__).parent / "sample_data"
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


# ==================== Settings ====================


class AttributionSettings(BaseModel):
    """Configurable knobs for attribution models and path analysis."""

    lookback_window_days: int = 30
    use_converted_flag: bool = True
    min_conversion_value: float = 0.0

    time_decay_half_life_days: float = 7.0
    position_first_pct: float = 0.4
    position_last_pct: float = 0.4
    markov_min_paths: int = 5


class MMMSettings(BaseModel):
    """High-level MMM configuration (used as defaults when starting new runs)."""

    frequency: str = "W"  # "W" or "M"


class NBASettings(BaseModel):
    """Next-best-action configuration."""

    min_prefix_support: int = 5
    min_conversion_rate: float = 0.01  # 1%
    max_prefix_depth: int = 5


class Settings(BaseModel):
    attribution: AttributionSettings = AttributionSettings()
    mmm: MMMSettings = MMMSettings()
    nba: NBASettings = NBASettings()


SETTINGS_PATH = DATA_DIR / "settings.json"


def _load_settings() -> Settings:
    if SETTINGS_PATH.exists():
        try:
            data = json.loads(SETTINGS_PATH.read_text())
            return Settings(**data)
        except Exception:
            # Fallback to defaults if file is corrupted
            pass
    settings = Settings()
    SETTINGS_PATH.write_text(settings.model_dump_json(indent=2))
    return settings


SETTINGS: Settings = _load_settings()


def _save_settings() -> None:
    SETTINGS_PATH.write_text(SETTINGS.model_dump_json(indent=2))


# KPI configuration (separate JSON file)
KPI_CONFIG: KpiConfig = load_kpi_config()


def compute_campaign_uplift(journeys: List[Dict]) -> Dict[str, Dict[str, Any]]:
    """
    For each campaign step (channel:campaign or channel), estimate uplift by comparing
    journeys that include the campaign vs. journeys that do not.

    Uplift is purely observational here (not a causal experiment):
      - treatment group: journeys that touched the campaign at least once
      - holdout group: journeys that never touched the campaign
    """
    if not journeys:
        return {}

    total_n = len(journeys)
    total_conv = 0

    treat_stats: Dict[str, Dict[str, Any]] = {}

    for j in journeys:
        converted = j.get("converted", True)
        if converted:
            total_conv += 1
        # Build set of campaign steps in this journey
        steps = set()
        for tp in j.get("touchpoints", []):
            channel = tp.get("channel", "unknown")
            campaign = tp.get("campaign")
            step = f"{channel}:{campaign}" if campaign else channel
            steps.add(step)
        for step in steps:
            st = treat_stats.setdefault(step, {"n": 0, "conv": 0})
            st["n"] += 1
            if converted:
                st["conv"] += 1

    uplift: Dict[str, Dict[str, Any]] = {}
    for step, st in treat_stats.items():
        treat_n = st["n"]
        treat_conv = st["conv"]
        control_n = total_n - treat_n
        control_conv = total_conv - treat_conv

        treat_rate = treat_conv / treat_n if treat_n > 0 else 0.0
        control_rate = control_conv / control_n if control_n > 0 else 0.0
        abs_uplift = treat_rate - control_rate
        rel_uplift = abs_uplift / control_rate if control_rate > 0 else None

        uplift[step] = {
            "treatment_n": treat_n,
            "treatment_conversions": treat_conv,
            "treatment_rate": treat_rate,
            "holdout_n": control_n,
            "holdout_conversions": control_conv,
            "holdout_rate": control_rate,
            "uplift_abs": abs_uplift,
            "uplift_rel": rel_uplift,
        }

    return uplift


def compute_campaign_trends(journeys: List[Dict]) -> Dict[str, Any]:
    """
    Build simple time series per campaign step (channel:campaign or channel):
      - transactions: number of converted journeys attributed to that campaign (last touch)
      - revenue: sum of conversion_value for those journeys

    Conversion date is taken as the timestamp of the last touchpoint.
    """
    if not journeys:
        return {"campaigns": [], "dates": [], "series": {}}

    series: Dict[str, Dict[str, Dict[str, float]]] = {}
    all_dates: set[str] = set()

    for j in journeys:
        if not j.get("converted", True):
            continue
        tps = j.get("touchpoints", [])
        if not tps:
            continue
        last_tp = tps[-1]
        ts = last_tp.get("timestamp")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts)
            date_key = dt.date().isoformat()
        except Exception:
            date_key = str(ts)

        channel = last_tp.get("channel", "unknown")
        campaign = last_tp.get("campaign")
        step = f"{channel}:{campaign}" if campaign else channel

        all_dates.add(date_key)
        step_series = series.setdefault(step, {})
        entry = step_series.setdefault(date_key, {"transactions": 0.0, "revenue": 0.0})
        entry["transactions"] += 1.0
        entry["revenue"] += float(j.get("conversion_value", 0.0) or 0.0)

    sorted_dates = sorted(all_dates)
    campaigns = sorted(series.keys())

    out_series: Dict[str, List[Dict[str, Any]]] = {}
    for step, date_map in series.items():
        points = []
        for d in sorted_dates:
            val = date_map.get(d, {"transactions": 0.0, "revenue": 0.0})
            points.append({"date": d, "transactions": int(val["transactions"]), "revenue": val["revenue"]})
        out_series[step] = points

    return {"campaigns": campaigns, "dates": sorted_dates, "series": out_series}

# Initialize sample datasets
DATASETS["sample-weekly-01"] = {"path": SAMPLE_DIR / "sample-weekly-01.csv", "type": "sales"}
DATASETS["sample-weekly-realistic"] = {"path": SAMPLE_DIR / "sample-weekly-realistic.csv", "type": "sales"}
DATASETS["sample-attribution-weekly"] = {"path": SAMPLE_DIR / "sample-attribution-weekly.csv", "type": "attribution"}
DATASETS["sample-weekly-campaigns"] = {"path": SAMPLE_DIR / "sample-weekly-campaigns.csv", "type": "sales"}

# Load sample expenses
EXPENSES = {
    "google_ads_2024-01": ExpenseEntry(channel="google_ads", amount=12500.00, period="2024-01", notes="Google Ads Jan 2024"),
    "meta_ads_2024-01": ExpenseEntry(channel="meta_ads", amount=8200.00, period="2024-01", notes="Meta Ads Jan 2024"),
    "linkedin_ads_2024-01": ExpenseEntry(channel="linkedin_ads", amount=3500.00, period="2024-01", notes="LinkedIn Ads Jan 2024"),
    "email_2024-01": ExpenseEntry(channel="email", amount=450.00, period="2024-01", notes="Email platform cost Jan 2024"),
    "whatsapp_2024-01": ExpenseEntry(channel="whatsapp", amount=280.00, period="2024-01", notes="WhatsApp Business Jan 2024"),
}

# Load sample journeys at startup
_sample_paths_file = SAMPLE_DIR / "sample-conversion-paths.json"
if _sample_paths_file.exists():
    try:
        with open(_sample_paths_file) as f:
            JOURNEYS = json.load(f)
    except Exception:
        JOURNEYS = []

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173")

# ==================== Settings API ====================


@app.get("/api/settings")
def get_settings():
    """Return current configuration for attribution, MMM, and NBA."""
    return SETTINGS


@app.post("/api/settings")
def update_settings(new_settings: Settings):
    """Update global configuration. Overwrites previous settings."""
    global SETTINGS
    SETTINGS = new_settings
    _save_settings()
    return SETTINGS


# ==================== Taxonomy API ====================


@app.get("/api/taxonomy")
def get_taxonomy():
    """Return current channel/source/medium/campaign taxonomy rules."""
    tax = load_taxonomy()
    return {
        "channel_rules": [
            {
                "name": r.name,
                "channel": r.channel,
                "source_regex": r.source_regex,
                "medium_regex": r.medium_regex,
            }
            for r in tax.channel_rules
        ],
        "source_aliases": tax.source_aliases,
        "medium_aliases": tax.medium_aliases,
    }


@app.post("/api/taxonomy")
def update_taxonomy(payload: Dict[str, Any]):
    """Replace taxonomy rules and aliases."""
    rules = []
    for r in payload.get("channel_rules", []):
        rules.append(
            {
                "name": r.get("name", ""),
                "channel": r.get("channel", ""),
                "source_regex": r.get("source_regex"),
                "medium_regex": r.get("medium_regex"),
            }
        )
    tax = Taxonomy(
        channel_rules=[Taxonomy.default().channel_rules[0]].__class__(  # create empty then replace below
        )
    )
    tax.channel_rules = [Taxonomy.default().channel_rules[0]]
    tax.channel_rules = [
        type(Taxonomy.default().channel_rules[0])(
            name=r["name"],
            channel=r["channel"],
            source_regex=r.get("source_regex"),
            medium_regex=r.get("medium_regex"),
        )
        for r in rules
    ]
    tax.source_aliases = payload.get("source_aliases", {})
    tax.medium_aliases = payload.get("medium_aliases", {})
    save_taxonomy(tax)
    return get_taxonomy()


# ==================== KPI / Conversion API ====================


class KpiDefinitionModel(BaseModel):
    id: str
    label: str
    type: str  # "primary" or "micro"
    event_name: str
    value_field: Optional[str] = None
    weight: float = 1.0
    lookback_days: Optional[int] = None


class KpiConfigModel(BaseModel):
    definitions: List[KpiDefinitionModel]
    primary_kpi_id: Optional[str] = None


@app.get("/api/kpis", response_model=KpiConfigModel)
def get_kpis():
    """Return configured KPI and micro-conversion definitions."""
    cfg = KPI_CONFIG
    return KpiConfigModel(
        definitions=[KpiDefinitionModel(**d.__dict__) for d in cfg.definitions],
        primary_kpi_id=cfg.primary_kpi_id,
    )


@app.post("/api/kpis", response_model=KpiConfigModel)
def update_kpis(cfg: KpiConfigModel):
    """Replace KPI configuration."""
    global KPI_CONFIG
    defs = [KpiDefinition(**d.dict()) for d in cfg.definitions]
    KPI_CONFIG = KpiConfig(definitions=defs, primary_kpi_id=cfg.primary_kpi_id)
    save_kpi_config(KPI_CONFIG)
    return get_kpis()


# ==================== Health ====================

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        **engine_info(),
        "journeys_loaded": len(JOURNEYS),
        "attribution_models": ATTRIBUTION_MODELS,
    }

# ==================== Attribution API ====================

@app.get("/api/attribution/models")
def list_attribution_models():
    """List available attribution models."""
    return {"models": ATTRIBUTION_MODELS}

@app.get("/api/attribution/journeys")
def get_journeys_summary():
    """Get summary of currently loaded conversion journeys."""
    if not JOURNEYS:
        return {"loaded": False, "count": 0}

    converted = [j for j in JOURNEYS if j.get("converted", True)]
    channels: set = set()
    for j in JOURNEYS:
        for tp in j.get("touchpoints", []):
            channels.add(tp.get("channel", "unknown"))

    # KPI breakdown by configured definitions (journey-level kpi_type if present)
    kpi_counts: Dict[str, int] = {}
    for j in JOURNEYS:
        ktype = j.get("kpi_type")
        if isinstance(ktype, str):
            kpi_counts[ktype] = kpi_counts.get(ktype, 0) + 1

    primary_kpi_id = KPI_CONFIG.primary_kpi_id
    primary_kpi_label = None
    if primary_kpi_id:
        for d in KPI_CONFIG.definitions:
            if d.id == primary_kpi_id:
                primary_kpi_label = d.label
                break
    primary_count = kpi_counts.get(primary_kpi_id, len(converted) if primary_kpi_id else len(converted))
    return {
        "loaded": True,
        "count": len(JOURNEYS),
        "converted": len(converted),
        "non_converted": len(JOURNEYS) - len(converted),
        "channels": sorted(channels),
        "total_value": sum(j.get("conversion_value", 0) for j in converted),
        "primary_kpi_id": primary_kpi_id,
        "primary_kpi_label": primary_kpi_label,
        "primary_kpi_count": primary_count,
        "kpi_counts": kpi_counts,
    }

@app.post("/api/attribution/journeys/upload")
async def upload_journeys(file: UploadFile = File(...)):
    """Upload conversion path data as JSON."""
    global JOURNEYS
    content = await file.read()
    try:
        data = json.loads(content)
        if isinstance(data, list):
            JOURNEYS = data
        elif isinstance(data, dict) and "journeys" in data:
            JOURNEYS = data["journeys"]
        else:
            raise ValueError("Expected a JSON array of journeys")
    except (json.JSONDecodeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")
    return {"count": len(JOURNEYS), "message": f"Loaded {len(JOURNEYS)} journeys"}

@app.post("/api/attribution/journeys/from-cdp")
def import_journeys_from_cdp(mapping: AttributionMappingConfig = AttributionMappingConfig()):
    """Parse loaded CDP profiles into attribution journeys using configurable mapping."""
    global JOURNEYS
    cdp_json_path = DATA_DIR / "meiro_cdp_profiles.json"
    cdp_path = DATA_DIR / "meiro_cdp.csv"

    if cdp_json_path.exists():
        with open(cdp_json_path) as f:
            profiles = json.load(f)
    elif cdp_path.exists():
        df = pd.read_csv(cdp_path)
        profiles = df.to_dict(orient="records")
    else:
        raise HTTPException(status_code=404, detail="No CDP data found. Fetch from Meiro CDP first.")

    JOURNEYS = parse_conversion_paths(
        profiles,
        touchpoint_attr=mapping.touchpoint_attr,
        value_attr=mapping.value_attr,
        id_attr=mapping.id_attr,
        channel_field=mapping.channel_field,
        timestamp_field=mapping.timestamp_field,
    )
    return {"count": len(JOURNEYS), "message": f"Parsed {len(JOURNEYS)} journeys from CDP data"}

@app.post("/api/attribution/journeys/load-sample")
def load_sample_journeys():
    """Load the built-in sample conversion paths."""
    global JOURNEYS
    sample_file = SAMPLE_DIR / "sample-conversion-paths.json"
    if not sample_file.exists():
        raise HTTPException(status_code=404, detail="Sample data not found")
    with open(sample_file) as f:
        JOURNEYS = json.load(f)
    return {"count": len(JOURNEYS), "message": f"Loaded {len(JOURNEYS)} sample journeys"}

@app.post("/api/attribution/run")
def run_attribution_model(model: str = "linear"):
    """Run a single attribution model on loaded journeys."""
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded. Upload or import data first.")
    if model not in ATTRIBUTION_MODELS:
        raise HTTPException(status_code=400, detail=f"Unknown model: {model}. Available: {ATTRIBUTION_MODELS}")
    kwargs: Dict[str, Any] = {}
    if model == "time_decay":
        kwargs["half_life_days"] = SETTINGS.attribution.time_decay_half_life_days
    elif model == "position_based":
        kwargs["first_pct"] = SETTINGS.attribution.position_first_pct
        kwargs["last_pct"] = SETTINGS.attribution.position_last_pct
    result = run_attribution(JOURNEYS, model=model, **kwargs)
    ATTRIBUTION_RESULTS[model] = result
    return result

@app.post("/api/attribution/run-all")
def run_all_attribution_models():
    """Run all attribution models on loaded journeys."""
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded. Upload or import data first.")
    results = []
    for model in ATTRIBUTION_MODELS:
        try:
            kwargs: Dict[str, Any] = {}
            if model == "time_decay":
                kwargs["half_life_days"] = SETTINGS.attribution.time_decay_half_life_days
            elif model == "position_based":
                kwargs["first_pct"] = SETTINGS.attribution.position_first_pct
                kwargs["last_pct"] = SETTINGS.attribution.position_last_pct
            result = run_attribution(JOURNEYS, model=model, **kwargs)
            results.append(result)
            ATTRIBUTION_RESULTS[model] = result
        except Exception as exc:
            results.append({"model": model, "error": str(exc)})
    return {"results": results}

@app.get("/api/attribution/results")
def get_attribution_results():
    """Get cached attribution results for all models that have been run."""
    return ATTRIBUTION_RESULTS

@app.get("/api/attribution/results/{model}")
def get_attribution_result(model: str):
    """Get attribution result for a specific model."""
    result = ATTRIBUTION_RESULTS.get(model)
    if not result:
        raise HTTPException(status_code=404, detail=f"No results for model '{model}'. Run it first.")
    return result

@app.get("/api/attribution/paths")
def get_path_analysis():
    """Analyze conversion paths for common patterns and statistics. Includes next-best-action recommendations per path prefix (channel and optionally campaign level)."""
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")
    path_analysis = analyze_paths(JOURNEYS)

    # NBA recommendations (channel level)
    nba_channel = compute_next_best_action(JOURNEYS, level="channel")
    min_support = SETTINGS.nba.min_prefix_support
    min_rate = SETTINGS.nba.min_conversion_rate
    filtered_channel: Dict[str, Any] = {}
    for prefix, recs in nba_channel.items():
        kept = [r for r in recs if r["count"] >= min_support and r["conversion_rate"] >= min_rate]
        if kept:
            filtered_channel[prefix] = kept
    path_analysis["next_best_by_prefix"] = filtered_channel

    # NBA recommendations (campaign level, optional)
    if has_any_campaign(JOURNEYS):
        nba_campaign = compute_next_best_action(JOURNEYS, level="campaign")
        filtered_campaign: Dict[str, Any] = {}
        for prefix, recs in nba_campaign.items():
            kept = [r for r in recs if r["count"] >= min_support and r["conversion_rate"] >= min_rate]
            if kept:
                filtered_campaign[prefix] = kept
        path_analysis["next_best_by_prefix_campaign"] = filtered_campaign
    return path_analysis


def _next_best_action_impl(path_so_far: str = "", level: str = "channel"):
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")
    prefix = path_so_far.strip().replace(",", " > ").replace("  ", " ").strip()
    use_level = "campaign" if level == "campaign" and has_any_campaign(JOURNEYS) else "channel"
    nba = compute_next_best_action(JOURNEYS, level=use_level)
    min_support = SETTINGS.nba.min_prefix_support
    min_rate = SETTINGS.nba.min_conversion_rate
    recs = nba.get(prefix, [])
    filtered = [r for r in recs if r["count"] >= min_support and r["conversion_rate"] >= min_rate][:10]
    return {"path_so_far": prefix or "(start)", "level": use_level, "recommendations": filtered}


@app.get("/api/attribution/next-best-action")
@app.get("/api/attribution/next_best_action")
def get_next_best_action(path_so_far: str = "", level: str = "channel"):
    """
    Given a path prefix (e.g. 'google_ads' or 'google_ads > email'), return recommended next channels
    (or channel:campaign when level=campaign and data has campaign). Use comma or ' > ' to separate steps.
    """
    return _next_best_action_impl(path_so_far=path_so_far, level=level)

@app.get("/api/attribution/performance")
def get_channel_performance(model: str = "linear"):
    """Get channel performance metrics combining attribution with expenses."""
    result = ATTRIBUTION_RESULTS.get(model)
    if not result:
        if not JOURNEYS:
            raise HTTPException(status_code=400, detail="No journeys loaded.")
        kwargs: Dict[str, Any] = {}
        if model == "time_decay":
            kwargs["half_life_days"] = SETTINGS.attribution.time_decay_half_life_days
        elif model == "position_based":
            kwargs["first_pct"] = SETTINGS.attribution.position_first_pct
            kwargs["last_pct"] = SETTINGS.attribution.position_last_pct
        result = run_attribution(JOURNEYS, model=model, **kwargs)
        ATTRIBUTION_RESULTS[model] = result

    expense_by_channel: Dict[str, float] = {}
    for exp in EXPENSES.values():
        expense_by_channel[exp.channel] = expense_by_channel.get(exp.channel, 0) + exp.amount

    performance = compute_channel_performance(result, expense_by_channel)
    return {
        "model": model,
        "channels": performance,
        "total_spend": sum(expense_by_channel.values()),
        "total_attributed_value": result.get("total_value", 0),
        "total_conversions": result.get("total_conversions", 0),
    }


@app.get("/api/attribution/campaign-performance")
def get_campaign_performance(model: str = "linear"):
    """Campaign-level attribution (channel:campaign). Requires touchpoints with campaign. Returns campaigns with attributed value and optional suggested next (NBA)."""
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")
    if not has_any_campaign(JOURNEYS):
        return {
            "model": model,
            "campaigns": [],
            "total_conversions": 0,
            "total_value": 0,
            "message": "No campaign data in touchpoints. Add a 'campaign' field to use campaign performance.",
        }
    kwargs: Dict[str, Any] = {}
    if model == "time_decay":
        kwargs["half_life_days"] = SETTINGS.attribution.time_decay_half_life_days
    elif model == "position_based":
        kwargs["first_pct"] = SETTINGS.attribution.position_first_pct
        kwargs["last_pct"] = SETTINGS.attribution.position_last_pct
    result = run_attribution_campaign(JOURNEYS, model=model, **kwargs)
    expense_by_channel: Dict[str, float] = {}
    for exp in EXPENSES.values():
        expense_by_channel[exp.channel] = expense_by_channel.get(exp.channel, 0) + exp.amount

    campaigns_list = []
    for ch in result.get("channels", []):
        step = ch["channel"]
        channel_name = step.split(":", 1)[0] if ":" in step else step
        campaign_name = step.split(":", 1)[1] if ":" in step else None
        spend = expense_by_channel.get(channel_name, 0)
        attr_val = ch["attributed_value"]
        attr_conv = ch.get("attributed_conversions", 0)
        roi = ((attr_val - spend) / spend) if spend and spend > 0 else None  # ratio, e.g. 1.5 = 150%
        roas = (attr_val / spend) if spend and spend > 0 else None
        cpa = (spend / attr_conv) if spend and attr_conv and attr_conv > 0 else None
        campaigns_list.append({
            "campaign": step,
            "channel": channel_name,
            "campaign_name": campaign_name,
            "attributed_value": ch["attributed_value"],
            "attributed_share": ch["attributed_share"],
            "attributed_conversions": ch.get("attributed_conversions", 0),
            "spend": round(spend, 2),
            "roi": round(roi, 4) if roi is not None else None,
            "roas": round(roas, 2) if roas is not None else None,
            "cpa": round(cpa, 2) if cpa is not None else None,
        })

    # Attach NBA suggestion (campaign-level) and uplift vs synthetic holdout
    nba_campaign = compute_next_best_action(JOURNEYS, level="campaign")
    uplift = compute_campaign_uplift(JOURNEYS)
    for c in campaigns_list:
        recs = nba_campaign.get(c["campaign"], [])
        c["suggested_next"] = recs[0] if recs else None
        u = uplift.get(c["campaign"])
        if u:
            c["treatment_rate"] = u["treatment_rate"]
            c["holdout_rate"] = u["holdout_rate"]
            c["uplift_abs"] = u["uplift_abs"]
            c["uplift_rel"] = u["uplift_rel"]
            c["treatment_n"] = u["treatment_n"]
            c["holdout_n"] = u["holdout_n"]

    return {
        "model": model,
        "campaigns": campaigns_list,
        "total_conversions": result.get("total_conversions", 0),
        "total_value": result.get("total_value", 0),
        "total_spend": sum(expense_by_channel.values()),
    }


@app.get("/api/attribution/campaign-performance/trends")
def get_campaign_performance_trends():
    """
    Time series for campaign performance:
      - transactions: number of converted journeys (last-touch attribution)
      - revenue: sum of conversion_value for those journeys

    Output:
      {
        \"campaigns\": [\"google_ads:Brand\", ...],
        \"dates\": [\"2024-01-01\", ...],
        \"series\": {
          \"google_ads:Brand\": [{\"date\": \"2024-01-01\", \"transactions\": 3, \"revenue\": 540.0}, ...],
          ...
        }
      }
    """
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")
    return compute_campaign_trends(JOURNEYS)


# ==================== Expense Management ====================

@app.get("/api/expenses")
def list_expenses():
    """List all expense entries."""
    return [{"id": k, **v.model_dump()} for k, v in EXPENSES.items()]

@app.post("/api/expenses")
def add_expense(entry: ExpenseEntry):
    """Add or update an expense entry."""
    key = f"{entry.channel}_{entry.period or 'all'}"
    EXPENSES[key] = entry
    return {"id": key, **entry.model_dump()}

@app.delete("/api/expenses/{expense_id}")
def delete_expense(expense_id: str):
    """Delete an expense entry."""
    if expense_id not in EXPENSES:
        raise HTTPException(status_code=404, detail="Expense not found")
    del EXPENSES[expense_id]
    return {"message": f"Deleted expense {expense_id}"}

@app.get("/api/expenses/summary")
def expense_summary():
    """Get aggregated expense summary by channel."""
    by_channel: Dict[str, float] = {}
    for exp in EXPENSES.values():
        by_channel[exp.channel] = by_channel.get(exp.channel, 0) + exp.amount
    return {
        "by_channel": by_channel,
        "total": sum(by_channel.values()),
        "channels": sorted(by_channel.keys()),
    }

# ==================== OAuth Routes ====================

@app.get("/api/auth/status")
def auth_status():
    connected = get_all_connected_platforms()
    if meiro_cdp.is_connected() and "meiro_cdp" not in connected:
        connected.append("meiro_cdp")
    return {"connected": connected}

@app.get("/api/auth/callback/{platform}")
async def oauth_callback(platform: str, code: Optional[str] = Query(None), error: Optional[str] = Query(None)):
    if error:
        return RedirectResponse(url=f"{FRONTEND_URL}/datasources?error={platform}&message={error}")
    if not code:
        return RedirectResponse(url=f"{FRONTEND_URL}/datasources?error={platform}&message=no_code")
    redirect_uri = f"{BASE_URL}/api/auth/callback/{platform}"
    try:
        if platform == "meta":
            client_id = ds_config.get_effective("meta", "app_id")
            client_secret = ds_config.get_effective("meta", "app_secret")
            if not client_id or not client_secret:
                raise HTTPException(status_code=500, detail="Meta OAuth not configured. Set in Administration.")
            response = requests.get("https://graph.facebook.com/v19.0/oauth/access_token",
                params={"client_id": client_id, "client_secret": client_secret, "redirect_uri": redirect_uri, "code": code}, timeout=30)
            response.raise_for_status()
            save_token("meta", response.json())
            return RedirectResponse(url=f"{FRONTEND_URL}/datasources?success=meta")
        elif platform == "google":
            client_id = ds_config.get_effective("google", "client_id")
            client_secret = ds_config.get_effective("google", "client_secret")
            if not client_id or not client_secret:
                raise HTTPException(status_code=500, detail="Google OAuth not configured. Set in Administration.")
            response = requests.post("https://oauth2.googleapis.com/token",
                data={"code": code, "client_id": client_id, "client_secret": client_secret, "redirect_uri": redirect_uri, "grant_type": "authorization_code"}, timeout=30)
            response.raise_for_status()
            save_token("google", response.json())
            return RedirectResponse(url=f"{FRONTEND_URL}/datasources?success=google")
        elif platform == "linkedin":
            client_id = ds_config.get_effective("linkedin", "client_id")
            client_secret = ds_config.get_effective("linkedin", "client_secret")
            if not client_id or not client_secret:
                raise HTTPException(status_code=500, detail="LinkedIn OAuth not configured. Set in Administration.")
            response = requests.post("https://www.linkedin.com/oauth/v2/accessToken",
                data={"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri, "client_id": client_id, "client_secret": client_secret}, timeout=30)
            response.raise_for_status()
            save_token("linkedin", response.json())
            return RedirectResponse(url=f"{FRONTEND_URL}/datasources?success=linkedin")
        else:
            return RedirectResponse(url=f"{FRONTEND_URL}/datasources?error={platform}&message=unknown_platform")
    except requests.RequestException:
        return RedirectResponse(url=f"{FRONTEND_URL}/datasources?error={platform}&message=token_exchange_failed")

@app.delete("/api/auth/{platform}")
def disconnect_platform(platform: str):
    if delete_token(platform):
        return {"message": f"Disconnected {platform}"}
    raise HTTPException(status_code=404, detail=f"No connection found for {platform}")


# ==================== Data Source Administration ====================

class DatasourceCredentialUpdate(BaseModel):
    """Update OAuth credentials for a platform. Only provided fields are updated."""
    platform: str  # "google" | "meta" | "linkedin"
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    app_id: Optional[str] = None      # Meta only
    app_secret: Optional[str] = None   # Meta only


@app.get("/api/admin/datasource-config")
def get_datasource_config_status():
    """Return which platforms have credentials configured (no secret values)."""
    return ds_config.get_status()


@app.post("/api/admin/datasource-config")
def update_datasource_config(body: DatasourceCredentialUpdate):
    """Store OAuth credentials for a platform. Encrypted at rest. Env vars override if set."""
    platform = body.platform.lower()
    if platform not in ("google", "meta", "linkedin"):
        raise HTTPException(status_code=400, detail="platform must be google, meta, or linkedin")
    try:
        if platform == "google":
            if body.client_id is not None:
                ds_config.set_stored("google", client_id=body.client_id)
            if body.client_secret is not None:
                ds_config.set_stored("google", client_secret=body.client_secret)
        elif platform == "meta":
            if body.app_id is not None:
                ds_config.set_stored("meta", app_id=body.app_id)
            if body.app_secret is not None:
                ds_config.set_stored("meta", app_secret=body.app_secret)
        elif platform == "linkedin":
            if body.client_id is not None:
                ds_config.set_stored("linkedin", client_id=body.client_id)
            if body.client_secret is not None:
                ds_config.set_stored("linkedin", client_secret=body.client_secret)
        return {"message": "Credentials updated", "platform": platform, "configured": ds_config.get_platform_configured(platform)}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/auth/{platform}")
def start_oauth(platform: str):
    redirect_uri = f"{BASE_URL}/api/auth/callback/{platform}"
    if platform == "meta":
        client_id = ds_config.get_effective("meta", "app_id")
        if not client_id:
            raise HTTPException(status_code=500, detail="Meta OAuth not configured. Set credentials in Data Sources → Administration.")
        return RedirectResponse(url=f"https://www.facebook.com/v19.0/dialog/oauth?client_id={client_id}&redirect_uri={redirect_uri}&scope=ads_read,ads_management,business_management")
    elif platform == "google":
        client_id = ds_config.get_effective("google", "client_id")
        if not client_id:
            raise HTTPException(status_code=500, detail="Google OAuth not configured. Set credentials in Data Sources → Administration.")
        return RedirectResponse(url=f"https://accounts.google.com/o/oauth2/v2/auth?client_id={client_id}&redirect_uri={redirect_uri}&response_type=code&scope=https://www.googleapis.com/auth/adwords&access_type=offline&prompt=consent")
    elif platform == "linkedin":
        client_id = ds_config.get_effective("linkedin", "client_id")
        if not client_id:
            raise HTTPException(status_code=500, detail="LinkedIn OAuth not configured. Set credentials in Data Sources → Administration.")
        return RedirectResponse(url=f"https://www.linkedin.com/oauth/v2/authorization?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}&scope=r_ads_reporting,r_ads,r_organization_social")
    raise HTTPException(status_code=400, detail=f"Unknown platform: {platform}")

# ==================== Dataset Routes ====================

@app.post("/api/datasets/upload")
async def upload_dataset(file: UploadFile = File(...), dataset_id: Optional[str] = None, type: str = "sales"):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")
    dataset_id = dataset_id or file.filename.replace(".csv", "")
    dest = SAMPLE_DIR / f"{dataset_id}.csv"
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    content = await file.read()
    dest.write_bytes(content)
    DATASETS[dataset_id] = {"path": dest, "type": type}
    df = pd.read_csv(dest).head(5)
    return {"dataset_id": dataset_id, "columns": list(df.columns), "preview_rows": df.to_dict(orient="records"), "path": str(dest), "type": type}

@app.get("/api/datasets")
def list_datasets():
    return [{"dataset_id": k, "path": str(v.get("path", "")), "type": v.get("type", "sales")} for k, v in DATASETS.items()]

@app.get("/api/datasets/{dataset_id}")
def get_dataset(dataset_id: str, preview_only: bool = True):
    dataset_info = DATASETS.get(dataset_id)
    if not dataset_info:
        raise HTTPException(status_code=404, detail="Dataset not found")
    path = dataset_info.get("path")
    if not path or not path.exists():
        raise HTTPException(status_code=404, detail="Dataset not found")
    df = pd.read_csv(path).head(5) if preview_only else pd.read_csv(path)
    return {"dataset_id": dataset_id, "columns": list(df.columns), "preview_rows": df.to_dict(orient="records"), "type": dataset_info.get("type", "sales")}

@app.get("/api/datasets/{dataset_id}/validate")
def validate_dataset(dataset_id: str):
    dataset_info = DATASETS.get(dataset_id)
    if not dataset_info:
        raise HTTPException(status_code=404, detail="Dataset not found")
    path = dataset_info.get("path")
    if not path or not path.exists():
        raise HTTPException(status_code=404, detail="Dataset file not found")
    df = pd.read_csv(path)
    columns = list(df.columns)
    n_rows = len(df)
    col_info = [{"name": col, "dtype": str(df[col].dtype), "missing": int(df[col].isna().sum()), "unique": int(df[col].nunique()), "sample_values": df[col].dropna().head(3).tolist()} for col in columns]
    date_column = None
    date_range = None
    for col in columns:
        try:
            parsed = pd.to_datetime(df[col], errors="coerce")
            if parsed.notna().sum() > len(df) * 0.8:
                date_column = col
                date_range = {"min": str(parsed.min().date()), "max": str(parsed.max().date()), "n_periods": int(parsed.nunique())}
                break
        except Exception:
            continue
    spend_kw = ["spend", "cost", "budget", "investment", "ad_spend"]
    kpi_kw = ["sales", "revenue", "conversions", "orders", "profit", "aov", "clicks"]
    cov_kw = ["holiday", "price", "index", "competitor", "temperature", "season"]
    numeric_types = ["float64", "int64", "float32", "int32"]
    suggested_spend = [c for c in columns if any(k in c.lower() for k in spend_kw) and df[c].dtype in numeric_types]
    suggested_kpi = [c for c in columns if any(k in c.lower() for k in kpi_kw) and df[c].dtype in numeric_types]
    suggested_cov = [c for c in columns if any(k in c.lower() for k in cov_kw) and df[c].dtype in numeric_types and c not in suggested_spend and c not in suggested_kpi]
    is_tall = {"channel", "campaign", "spend"}.issubset(set(columns))
    warnings = []
    if n_rows < 20:
        warnings.append(f"Only {n_rows} rows.")
    if date_column and date_range and date_range["n_periods"] < 20:
        warnings.append(f"Only {date_range['n_periods']} unique dates.")
    for ci in col_info:
        if ci["missing"] > 0 and ci["missing"] / n_rows > 0.1:
            warnings.append(f"Column '{ci['name']}' has {ci['missing']/n_rows*100:.0f}% missing values.")
    return {"dataset_id": dataset_id, "n_rows": n_rows, "n_columns": len(columns), "columns": col_info, "date_column": date_column, "date_range": date_range, "format": "tall" if is_tall else "wide", "suggestions": {"spend_channels": suggested_spend, "kpi_columns": suggested_kpi, "covariates": suggested_cov}, "warnings": warnings}

# ==================== MMM Model Routes ====================

@app.post("/api/models")
def run_model(cfg: ModelConfig, tasks: BackgroundTasks):
    if cfg.dataset_id not in DATASETS:
        raise HTTPException(status_code=404, detail="dataset_id not found")
    # Apply MMM settings default frequency when user leaves it at the default
    if cfg.frequency == "W" and SETTINGS.mmm.frequency != "W":
        cfg.frequency = SETTINGS.mmm.frequency
    kpi_mode = cfg.kpi_mode if hasattr(cfg, 'kpi_mode') else 'conversions'
    run_id = f"{kpi_mode}_{len(RUNS)+1:04d}"
    RUNS[run_id] = {"status": "queued", "config": json.loads(cfg.model_dump_json()), "kpi_mode": kpi_mode}
    tasks.add_task(_fit_model, run_id, cfg)
    return {"run_id": run_id, "status": "queued"}

@app.get("/api/models")
def list_models():
    return [{"run_id": k, **v} for k, v in RUNS.items()]

@app.get("/api/models/compare")
def compare_models():
    if not RUNS:
        return []
    comparison: Dict[str, Any] = {}
    for run_id, run in RUNS.items():
        if run.get("status") != "finished":
            continue
        kpi_mode = run.get("kpi_mode", "conversions")
        for roi_entry in run.get("roi", []):
            ch = roi_entry["channel"]
            comparison.setdefault(ch, {"channel": ch, "roi": {}, "contrib": {}})
            comparison[ch]["roi"][kpi_mode] = roi_entry["roi"]
        for c_entry in run.get("contrib", []):
            ch = c_entry["channel"]
            comparison.setdefault(ch, {"channel": ch, "roi": {}, "contrib": {}})
            comparison[ch]["contrib"][kpi_mode] = c_entry["mean_share"]
    return list(comparison.values())

@app.get("/api/models/{run_id}")
def get_model(run_id: str):
    res = RUNS.get(run_id)
    if not res:
        raise HTTPException(status_code=404, detail="run_id not found")
    return res

@app.get("/api/models/{run_id}/contrib")
def channel_contrib(run_id: str):
    return RUNS.get(run_id, {}).get("contrib", [])

@app.get("/api/models/{run_id}/roi")
def roi(run_id: str):
    return RUNS.get(run_id, {}).get("roi", [])


@app.post("/api/models/{run_id}/what_if")
def what_if_scenario(run_id: str, scenario: Dict[str, float] = Body(..., embed=False)):
    """Simulate a simple what-if scenario using per-channel multipliers.

    The scenario is a mapping from channel -> multiplier, where 1.0 keeps the
    channel at baseline, 2.0 doubles its relative contribution weight, etc.

    This endpoint uses the already-fitted ROI and contribution shares and does
    not re-run the full Bayesian model, so it is fast enough for interactive UI.
    """

    run = RUNS.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Model not found")

    roi_map = {r["channel"]: r["roi"] for r in run.get("roi", [])}
    contrib_map = {c["channel"]: c["mean_share"] for c in run.get("contrib", [])}
    if not roi_map or not contrib_map:
        raise HTTPException(status_code=400, detail="ROI or contribution data not available")

    channels = sorted(roi_map.keys())

    baseline_per_channel = {}
    scenario_per_channel = {}
    baseline_total = 0.0
    scenario_total = 0.0

    for ch in channels:
        roi_val = float(roi_map.get(ch, 0.0))
        share = float(contrib_map.get(ch, 0.0))
        base = roi_val * share
        mult = float(scenario.get(ch, 1.0))
        new_val = base * mult

        baseline_per_channel[ch] = base
        scenario_per_channel[ch] = new_val
        baseline_total += base
        scenario_total += new_val

    uplift_abs = scenario_total - baseline_total
    uplift_pct = (uplift_abs / baseline_total * 100.0) if baseline_total != 0 else 0.0

    return {
        "baseline": {
            "total_kpi": baseline_total,
            "per_channel": baseline_per_channel,
        },
        "scenario": {
            "total_kpi": scenario_total,
            "per_channel": scenario_per_channel,
            "multipliers": scenario,
        },
        "lift": {
            "absolute": uplift_abs,
            "percent": uplift_pct,
        },
    }

@app.get("/api/models/{run_id}/summary/channel")
def get_channel_summary(run_id: str):
    res = RUNS.get(run_id)
    if not res:
        raise HTTPException(status_code=404, detail="Model not found")
    return res.get("channel_summary", [])

@app.get("/api/models/{run_id}/summary/campaign")
def get_campaign_summary(run_id: str):
    res = RUNS.get(run_id)
    if not res:
        raise HTTPException(status_code=404, detail="Model not found")
    return res.get("campaigns", [])

@app.get("/api/models/{run_id}/export.csv")
def export_campaign_plan(run_id: str):
    res = RUNS.get(run_id)
    if not res:
        raise HTTPException(status_code=404, detail="Model not found")
    campaigns = res.get("campaigns", [])
    if not campaigns:
        return "", 200, {"Content-Type": "text/csv"}
    out = io.StringIO()
    out.write("channel,campaign,spend,optimal_spend,roi,expected_conversions\n")
    for row in campaigns:
        spend = float(row.get("mean_spend", 0.0))
        roi_val = float(row.get("roi", 0.0))
        out.write(f"{row.get('channel')},{row.get('campaign')},{spend:.4f},{spend:.4f},{roi_val:.6f},{spend*roi_val:.4f}\n")
    return out.getvalue(), 200, {"Content-Type": "text/csv"}

@app.post("/api/models/{run_id}/optimize")
def optimize_budget(run_id: str, scenario: Dict[str, float]):
    run = RUNS.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Model not found")
    roi_map = {r["channel"]: r["roi"] for r in run.get("roi", [])}
    contrib_map = {c["channel"]: c["mean_share"] for c in run.get("contrib", [])}
    baseline = sum(roi_map.get(ch, 0) * contrib_map.get(ch, 0) for ch in roi_map)
    new_score = sum(roi_map.get(ch, 0) * contrib_map.get(ch, 0) * scenario.get(ch, 1.0) for ch in roi_map)
    uplift = ((new_score - baseline) / baseline * 100) if baseline != 0 else 0
    return {"uplift": uplift, "predicted_kpi": new_score, "baseline": baseline}

@app.post("/api/models/{run_id}/optimize/auto")
def optimize_auto(run_id: str, request: OptimizeRequest = OptimizeRequest()):
    from scipy.optimize import minimize
    import numpy as np
    run = RUNS.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Model not found")
    roi_map = {r["channel"]: r["roi"] for r in run.get("roi", [])}
    contrib_map = {c["channel"]: c["mean_share"] for c in run.get("contrib", [])}
    if not roi_map or not contrib_map:
        raise HTTPException(status_code=400, detail="ROI or contribution data not available")

    channels = list(roi_map.keys())
    n = len(channels)
    roi_values = np.maximum(np.array([roi_map.get(ch, 0) for ch in channels]), 0.01)
    contrib_raw = np.array([contrib_map.get(ch, 0) for ch in channels])
    cs = contrib_raw.sum()
    contrib_values = contrib_raw / cs if cs > 0 else contrib_raw

    # Baseline score corresponds to all multipliers = 1.0
    baseline_score = float(np.sum(roi_values * contrib_values))

    def objective(x: np.ndarray) -> float:
        # We maximize incremental score over baseline; scipy minimizes, so negate.
        return -(np.sum(roi_values * contrib_values * x) - baseline_score)

    # Sum of multipliers constrained to total_budget * n (average = total_budget)
    constraints = ({"type": "eq", "fun": lambda x: np.sum(x) - (request.total_budget * n)},)

    # Build per-channel bounds, falling back to global min/max
    bounds: list[tuple[float, float]] = []
    per_constraints = request.channel_constraints or {}
    for ch in channels:
        c = per_constraints.get(ch)
        if c and c.locked:
            bounds.append((1.0, 1.0))
        else:
            lo = c.min if c and c.min is not None else request.min_spend
            hi = c.max if c and c.max is not None else request.max_spend
            bounds.append((float(lo), float(hi)))
    try:
        # Start at all-ones (baseline) scaled by requested average multiplier
        x0 = np.ones(n) * request.total_budget
        result = minimize(
            objective,
            x0,
            method="SLSQP",
            bounds=bounds,
            constraints=constraints,
            options={"maxiter": 1000},
        )
        if not result.success:
            return {"optimal_mix": {ch: float(request.total_budget) for ch in channels}, "predicted_kpi": baseline_score, "baseline_kpi": baseline_score, "uplift": 0.0, "message": "At baseline"}
        optimal_mix = {ch: float(val) for ch, val in zip(channels, result.x)}
        predicted = float(-result.fun + baseline_score)
        uplift = ((predicted - baseline_score) / baseline_score * 100) if baseline_score > 0 else 0
        return {"optimal_mix": optimal_mix, "predicted_kpi": max(predicted, baseline_score), "baseline_kpi": baseline_score, "uplift": max(uplift, 0), "message": f"Uplift: {max(uplift,0):.1f}%"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Optimization error: {e}")

# ==================== Ad Platform Connectors ====================

@app.get("/api/connectors/status")
def connectors_status():
    sources = [(DATA_DIR / "meta_ads.csv", "Meta"), (DATA_DIR / "google_ads.csv", "Google"), (DATA_DIR / "linkedin_ads.csv", "LinkedIn"), (DATA_DIR / "meiro_cdp.csv", "Meiro CDP"), (DATA_DIR / "unified_ads.csv", "Unified")]
    stats = {}
    for path, name in sources:
        if path.exists():
            try:
                df = pd.read_csv(path)
                stats[name] = {"path": str(path), "rows": int(len(df)), "total_spend": float(df.get("spend", pd.Series([], dtype=float)).fillna(0).sum())}
            except Exception:
                stats[name] = {"path": str(path), "rows": 0}
        else:
            stats[name] = {"path": str(path), "rows": 0}
    return stats

@app.post("/api/connectors/meta")
def fetch_meta(ad_account_id: str, since: str, until: str, avg_aov: float = 0.0, access_token: Optional[str] = None):
    if not access_token:
        token_data = get_token("meta")
        if not token_data or not token_data.get("access_token"):
            raise HTTPException(status_code=401, detail="No Meta access token. Connect your Meta account first.")
        access_token = token_data["access_token"]
    out_path = DATA_DIR / "meta_ads.csv"
    url = f"https://graph.facebook.com/v19.0/{ad_account_id}/ads"
    params = {"access_token": access_token, "fields": "campaign_name,spend,impressions,clicks,actions,updated_time", "time_range": json.dumps({"since": since, "until": until}), "limit": 500}
    rows = []
    try:
        while True:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            for ad in data.get("data", []):
                actions = ad.get("actions", []) or []
                purchases = next((a for a in actions if a.get("action_type") == "purchase"), None)
                conv = float(purchases.get("value", 0)) if purchases else 0.0
                rows.append({"date": since, "channel": "meta_ads", "campaign": ad.get("campaign_name", "unknown"), "spend": float(ad.get("spend", 0) or 0), "impressions": int(ad.get("impressions", 0) or 0), "clicks": int(ad.get("clicks", 0) or 0), "conversions": float(conv), "revenue": float(conv * float(avg_aov))})
            next_url = data.get("paging", {}).get("next")
            if not next_url:
                break
            url = next_url
            params = {}
    except Exception:
        pass
    pd.DataFrame(rows).to_csv(out_path, index=False)
    total_spend = sum(r.get("spend", 0) for r in rows)
    if total_spend > 0:
        EXPENSES[f"meta_ads_{since[:7]}"] = ExpenseEntry(channel="meta_ads", amount=total_spend, period=since[:7], notes="Auto-imported from Meta Ads API")
    return {"rows": len(rows), "path": str(out_path)}

@app.post("/api/connectors/google")
def fetch_google(segments_date_from: str, segments_date_to: str):
    out_path = DATA_DIR / "google_ads.csv"
    if not out_path.exists():
        pd.DataFrame([], columns=["date", "channel", "campaign", "spend", "impressions", "clicks", "conversions", "revenue"]).to_csv(out_path, index=False)
    return {"rows": int(pd.read_csv(out_path).shape[0]), "path": str(out_path)}

@app.post("/api/connectors/linkedin")
def fetch_linkedin(since: str, until: str, access_token: Optional[str] = None):
    if not access_token:
        token_data = get_token("linkedin")
        if not token_data or not token_data.get("access_token"):
            raise HTTPException(status_code=401, detail="No LinkedIn access token. Connect your LinkedIn account first.")
        access_token = token_data["access_token"]
    out_path = DATA_DIR / "linkedin_ads.csv"
    headers = {"Authorization": f"Bearer {access_token}"}
    rows = []
    try:
        r = requests.get("https://api.linkedin.com/v2/adAnalyticsV2", headers=headers, params={"q": "analytics", "pivot": "CAMPAIGN", "timeGranularity": "DAILY"}, timeout=30)
        if r.ok:
            for el in r.json().get("elements", []):
                rows.append({"date": since, "channel": "linkedin_ads", "campaign": (el.get("campaign", {}) or {}).get("name", "unknown"), "spend": float(el.get("costInLocalCurrency", 0) or 0), "impressions": int(el.get("impressions", 0) or 0), "clicks": int(el.get("clicks", 0) or 0), "conversions": float(el.get("conversions", 0) or 0), "revenue": float(el.get("revenueValue", 0) or 0)})
    except Exception:
        pass
    pd.DataFrame(rows).to_csv(out_path, index=False)
    total_spend = sum(r.get("spend", 0) for r in rows)
    if total_spend > 0:
        EXPENSES[f"linkedin_ads_{since[:7]}"] = ExpenseEntry(channel="linkedin_ads", amount=total_spend, period=since[:7], notes="Auto-imported from LinkedIn Ads API")
    return {"rows": len(rows), "path": str(out_path)}

@app.post("/api/connectors/merge")
def merge_ads():
    sources = [DATA_DIR / "meta_ads.csv", DATA_DIR / "google_ads.csv", DATA_DIR / "linkedin_ads.csv", DATA_DIR / "meiro_cdp.csv"]
    frames = [pd.read_csv(p) for p in sources if p.exists()]
    unified = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame([], columns=["date", "channel", "campaign", "spend", "impressions", "clicks", "conversions", "revenue"])
    for col in ["spend", "impressions", "clicks", "conversions", "revenue"]:
        unified[col] = pd.to_numeric(unified.get(col, 0), errors='coerce').fillna(0)
    unified["date"] = pd.to_datetime(unified.get("date", pd.to_datetime([])), errors='coerce').dt.date.astype(str)
    unified.drop_duplicates(subset=["date", "channel", "campaign"], keep='last', inplace=True)
    out_path = DATA_DIR / "unified_ads.csv"
    unified.to_csv(out_path, index=False)
    return {"rows": int(len(unified)), "path": str(out_path)}

# ==================== Meiro CDP Connector Routes ====================

@app.post("/api/connectors/meiro/connect")
def meiro_connect(req: MeiroCDPConnectRequest):
    result = meiro_cdp.test_connection(req.api_base_url, req.api_key)
    if result.get("ok"):
        meiro_cdp.save_config(req.api_base_url, req.api_key)
        return {"message": "Connected to Meiro CDP", "connected": True}
    raise HTTPException(status_code=400, detail=result.get("message", "Connection failed"))

@app.delete("/api/connectors/meiro")
def meiro_disconnect():
    if meiro_cdp.disconnect():
        return {"message": "Disconnected from Meiro CDP"}
    raise HTTPException(status_code=404, detail="No Meiro CDP connection found")

@app.get("/api/connectors/meiro/status")
def meiro_status():
    return {"connected": meiro_cdp.is_connected()}

@app.get("/api/connectors/meiro/attributes")
def meiro_attributes():
    if not meiro_cdp.is_connected():
        raise HTTPException(status_code=401, detail="Meiro CDP not connected")
    return meiro_cdp.list_attributes()

@app.get("/api/connectors/meiro/events")
def meiro_events():
    if not meiro_cdp.is_connected():
        raise HTTPException(status_code=401, detail="Meiro CDP not connected")
    return meiro_cdp.list_events()

@app.get("/api/connectors/meiro/segments")
def meiro_segments():
    if not meiro_cdp.is_connected():
        raise HTTPException(status_code=401, detail="Meiro CDP not connected")
    return meiro_cdp.list_segments()

@app.post("/api/connectors/meiro/fetch")
def meiro_fetch(req: MeiroCDPExportRequest):
    if not meiro_cdp.is_connected():
        raise HTTPException(status_code=401, detail="Meiro CDP not connected")
    try:
        df = meiro_cdp.fetch_and_transform(since=req.since, until=req.until, event_types=req.event_types, attributes=req.attributes, segment_id=req.segment_id)
        out_path = DATA_DIR / "meiro_cdp.csv"
        df.to_csv(out_path, index=False)
        DATASETS["meiro-cdp-export"] = {"path": out_path, "type": "sales"}
        return {"rows": len(df), "path": str(out_path), "dataset_id": "meiro-cdp-export"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/connectors/meiro/profiles")
def meiro_profiles_status():
    """Return count of profiles stored from Meiro CDP webhook and the URL Meiro should POST to."""
    out_path = DATA_DIR / "meiro_cdp_profiles.json"
    count = 0
    if out_path.exists():
        try:
            data = json.loads(out_path.read_text())
            count = len(data) if isinstance(data, list) else 0
        except Exception:
            pass
    webhook_url = f"{BASE_URL}/api/connectors/meiro/profiles"
    return {"stored_count": count, "webhook_url": webhook_url}


@app.post("/api/connectors/meiro/profiles")
async def meiro_receive_profiles(
    request: Request,
    x_meiro_webhook_secret: Optional[str] = Header(None, alias="X-Meiro-Webhook-Secret"),
):
    """
    Webhook endpoint for Meiro CDP to push customer profiles.

    Meiro CDP can POST profiles here (e.g. from a flow or export). Stored profiles
    are used when you click "Import from CDP" in Data Sources.

    Body (JSON):
      - Array of profile objects: [ { "customer_id": "...", "touchpoints": [...], ... }, ... ]
      - Or object: { "profiles": [ ... ], "replace": true }
        - replace: true (default) = replace stored profiles with this payload
        - replace: false = append to existing profiles

    Optional security:
      - Set env MEIRO_WEBHOOK_SECRET; then Meiro must send header X-Meiro-Webhook-Secret with that value.
    """
    webhook_secret = os.getenv("MEIRO_WEBHOOK_SECRET", "").strip()
    if webhook_secret:
        if not x_meiro_webhook_secret or x_meiro_webhook_secret != webhook_secret:
            raise HTTPException(status_code=401, detail="Invalid or missing X-Meiro-Webhook-Secret")

    try:
        body = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    if isinstance(body, list):
        profiles = body
        replace = True
    elif isinstance(body, dict):
        profiles = body.get("profiles", body.get("data", []))
        replace = body.get("replace", True)
        if not isinstance(profiles, list):
            raise HTTPException(status_code=400, detail="Body must be an array of profiles or { 'profiles': [...] }")
    else:
        raise HTTPException(status_code=400, detail="Body must be JSON array or object with 'profiles' key")

    out_path = DATA_DIR / "meiro_cdp_profiles.json"
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if replace or not out_path.exists():
        to_store = profiles
    else:
        try:
            existing = json.loads(out_path.read_text())
            to_store = (existing if isinstance(existing, list) else []) + list(profiles)
        except Exception:
            to_store = profiles

    out_path.write_text(json.dumps(to_store, indent=2))
    return {"received": len(profiles), "stored_total": len(to_store), "message": "Profiles saved. Use Import from CDP in Data Sources to load into attribution."}


# ==================== Model Fitting Background Task ====================

def _fit_model(run_id: str, cfg: ModelConfig):
    dataset_info = DATASETS.get(cfg.dataset_id)
    if not dataset_info:
        RUNS[run_id] = {"status": "error", "detail": "Dataset not found"}
        return
    csv_path = dataset_info.get("path")
    df = pd.read_csv(csv_path, parse_dates=["date"])
    if cfg.kpi not in df.columns:
        RUNS[run_id] = {"status": "error", "detail": f"Column '{cfg.kpi}' missing"}
        return
    is_tall = {"channel", "campaign", "spend"}.issubset(set(df.columns))
    if not is_tall:
        for c in cfg.spend_channels:
            if c not in df.columns:
                RUNS[run_id] = {"status": "error", "detail": f"Column '{c}' missing"}
                return
    priors = cfg.priors or {}
    adstock_cfg = {"l_max": 8, "alpha_mean": priors.get("adstock", {}).get("alpha_mean", 0.5), "alpha_sd": priors.get("adstock", {}).get("alpha_sd", 0.2)}
    saturation_cfg = {"lam_mean": priors.get("saturation", {}).get("lam_mean", 0.001), "lam_sd": priors.get("saturation", {}).get("lam_sd", 0.0005)}
    mcmc_cfg = cfg.mcmc or {"draws": 1000, "tune": 1000, "chains": 4, "target_accept": 0.9}
    try:
        RUNS[run_id]["status"] = "running"
        result = mmm_fit_model(df=df, target_column=cfg.kpi, channel_columns=cfg.spend_channels, control_columns=cfg.covariates or [], date_column="date", adstock_cfg=adstock_cfg, saturation_cfg=saturation_cfg, mcmc_cfg=mcmc_cfg)
        RUNS[run_id] = {"status": "finished", "r2": result["r2"], "contrib": result["contrib"], "roi": result["roi"], "engine": result.get("engine", "unknown"), "config": RUNS[run_id]["config"], "kpi_mode": cfg.kpi_mode if hasattr(cfg, "kpi_mode") else "conversions"}
        for key in ("campaigns", "channel_summary", "adstock_params", "saturation_params", "diagnostics"):
            if key in result:
                RUNS[run_id][key] = result[key]
    except Exception as exc:
        RUNS[run_id] = {"status": "error", "detail": str(exc), "config": RUNS[run_id].get("config", {}), "kpi_mode": cfg.kpi_mode if hasattr(cfg, "kpi_mode") else "conversions"}
