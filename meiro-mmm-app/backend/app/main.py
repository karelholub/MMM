from fastapi import FastAPI, BackgroundTasks, UploadFile, File, HTTPException, Query, Body, Request, Header, Depends
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
import uuid
import requests
from datetime import datetime
from app.utils.token_store import save_token, get_token, delete_token, get_all_connected_platforms
from app.utils.encrypt import encrypt, decrypt
from app.utils import datasource_config as ds_config
from app.utils.taxonomy import load_taxonomy, save_taxonomy, Taxonomy
from app.utils.kpi_config import load_kpi_config, save_kpi_config, KpiConfig, KpiDefinition
from app.db import Base, engine, get_db
from app.models_config_dq import (
    ModelConfig as ORMModelConfig,
    ModelConfigAudit,
    DQSnapshot,
    DQAlertRule,
    DQAlert,
    Experiment,
    ExperimentAssignment,
    ExperimentExposure,
    ExperimentOutcome,
    ExperimentResult,
)
from app.services_model_config import (
    create_draft_config,
    clone_config,
    update_draft_config,
    activate_config,
    archive_config,
    get_default_config_id,
)
from app.services_data_quality import compute_dq_snapshots, evaluate_alert_rules
from app.services_conversions import (
    apply_model_config_to_journeys,
    load_journeys_from_db,
    persist_journeys_as_conversion_paths,
)
from app.services_quality import (
    load_config_and_meta,
    get_latest_quality_for_scope,
    summarize_config_changes,
    compute_overall_quality_from_dq,
)
from app.services_paths import compute_path_archetypes, compute_path_anomalies
from app.services_mmm_platform import build_mmm_dataset_from_platform
from app.services_mmm_mapping import build_smart_suggestions, validate_mapping
from app.services_incrementality import (
    assign_profiles_deterministic,
    record_exposure,
    record_exposures_batch,
    record_outcome,
    record_outcomes_batch,
    compute_experiment_results,
    estimate_sample_size,
    run_nightly_report,
    get_experiment_time_series,
    auto_assign_from_conversion_paths,
    compute_experiment_health,
)
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
    _step_string,
)

# Create DB tables if a real database is configured. This is idempotent and cheap
# for SQLite; in PostgreSQL you should run proper migrations from backend/migrations.
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Meiro Attribution Dashboard API", version="0.3.0")

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
    use_adstock: bool = True
    use_saturation: bool = True
    holdout_weeks: Optional[int] = 8
    random_seed: Optional[int] = None
    channel_display_names: Optional[Dict[str, str]] = None  # channel id -> human-readable name for MMM outputs

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
    # Core classification
    channel: str
    cost_type: str = "Media Spend"  # Media Spend, Platform Fees, etc.

    # Amounts & currencies
    amount: float  # original amount
    currency: str = "USD"  # original currency code
    reporting_currency: str = "USD"
    converted_amount: Optional[float] = None
    fx_rate: Optional[float] = None
    fx_date: Optional[str] = None  # label only, e.g. "2024-01-15"

    # Periods
    period: Optional[str] = None  # legacy field kept for compatibility (YYYY-MM)
    service_period_start: Optional[str] = None  # ISO date
    service_period_end: Optional[str] = None  # ISO date
    entry_date: Optional[str] = None  # invoice/entry date (ISO date)

    # References & notes
    invoice_ref: Optional[str] = None
    external_link: Optional[str] = None
    notes: Optional[str] = None

    # Provenance & status
    source_type: str = "manual"  # manual | import
    source_name: Optional[str] = None  # e.g. "google_ads", "meta_ads"
    status: str = "active"  # active | deleted

    # Audit trail (shallow, no user system)
    created_at: Optional[str] = None  # ISO timestamp
    updated_at: Optional[str] = None  # ISO timestamp
    deleted_at: Optional[str] = None  # ISO timestamp
    actor_type: str = "manual"  # manual | system | import
    change_note: Optional[str] = None


class ExpenseChangeEvent(BaseModel):
    expense_id: str
    timestamp: str
    event_type: str  # created | updated | deleted | restored
    actor_type: str  # manual | system | import
    note: Optional[str] = None

class AttributionMappingConfig(BaseModel):
    touchpoint_attr: str = "touchpoints"
    value_attr: str = "conversion_value"
    id_attr: str = "customer_id"
    channel_field: str = "channel"
    timestamp_field: str = "timestamp"


class BuildFromPlatformRequest(BaseModel):
    """Request to build an MMM weekly dataset from platform journeys and expenses."""
    date_start: str  # ISO date
    date_end: str    # ISO date
    kpi_target: str = "sales"  # sales | attribution (marketing-driven conversions)
    spend_channels: List[str]  # e.g. ["google_ads", "meta_ads", "linkedin_ads"]
    covariates: Optional[List[str]] = None
    currency: str = "USD"
    attribution_model: Optional[str] = None  # e.g. "linear"; when kpi_target=attribution, which model was used
    attribution_config_id: Optional[str] = None  # optional model config id


class ValidateMappingRequest(BaseModel):
    """Request to validate an MMM column mapping before running the model."""
    date_column: str
    kpi: str
    spend_channels: List[str]
    covariates: Optional[List[str]] = None
    kpi_target: Optional[str] = None  # sales | attribution; used for suggestion context only

# ==================== In-memory State ====================

RUNS: Dict[str, Any] = {}
DATASETS: Dict[str, Dict[str, Any]] = {}
EXPENSES: Dict[str, ExpenseEntry] = {}  # key: arbitrary unique id
EXPENSE_AUDIT_LOG: List[ExpenseChangeEvent] = []

# Import health & reconciliation (per-source sync state)
IMPORT_SYNC_STATE: Dict[str, Dict[str, Any]] = {}  # source -> { last_success_at, last_attempt_at, status, last_error, action_hint, records_imported, platform_total, period_start, period_end }
SYNC_IN_PROGRESS: set = set()  # source names currently syncing

JOURNEYS: List[Dict] = []  # Current loaded journeys
ATTRIBUTION_RESULTS: Dict[str, Any] = {}  # Cached attribution results
# Lightweight in-memory cache for path archetypes, keyed by view state.
PATH_ARCHETYPES_CACHE: Dict[tuple, Dict[str, Any]] = {}

SAMPLE_DIR = Path(__file__).parent / "sample_data"
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MMM_PLATFORM_DIR = DATA_DIR / "mmm_platform"
MMM_PLATFORM_DIR.mkdir(parents=True, exist_ok=True)
RUNS_FILE = DATA_DIR / "mmm_runs.json"


def _load_runs():
    """Load run registry from disk (idempotent)."""
    global RUNS
    if RUNS_FILE.exists():
        try:
            raw = RUNS_FILE.read_text(encoding="utf-8")
            RUNS.update(json.loads(raw))
        except Exception:
            pass


def _save_runs():
    """Persist run registry. Only serializable fields (no Path, etc.)."""
    try:
        out = {}
        for rid, r in RUNS.items():
            out[rid] = {k: v for k, v in r.items() if k in ("status", "config", "kpi_mode", "created_at", "updated_at", "dataset_id", "r2", "contrib", "roi", "engine", "detail", "uplift", "campaigns", "channel_summary", "adstock_params", "saturation_params", "diagnostics", "attribution_model", "attribution_config_id")}
        RUNS_FILE.parent.mkdir(parents=True, exist_ok=True)
        RUNS_FILE.write_text(json.dumps(out, indent=0), encoding="utf-8")
    except Exception:
        pass


_load_runs()


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


# ==================== Model Config Versioning API ====================


class ModelConfigPayload(BaseModel):
    """Payload for creating/updating a versioned attribution ModelConfig."""

    name: str
    config_json: Dict[str, Any]
    change_note: Optional[str] = None
    created_by: str = "system"


class ModelConfigUpdatePayload(BaseModel):
    config_json: Dict[str, Any]
    change_note: Optional[str] = None
    actor: str = "system"


class ModelConfigActivatePayload(BaseModel):
    actor: str = "system"
    set_as_default: bool = True


@app.get("/api/model-configs")
def list_model_configs(db=Depends(get_db)):
    """List all model configs (draft/active/archived)."""
    rows = (
        db.query(ORMModelConfig)
        .order_by(ORMModelConfig.name.asc(), ORMModelConfig.version.desc())
        .all()
    )
    return [
        {
            "id": r.id,
            "name": r.name,
            "status": r.status,
            "version": r.version,
            "parent_id": r.parent_id,
            "created_at": r.created_at,
            "updated_at": r.updated_at,
            "created_by": r.created_by,
            "change_note": r.change_note,
            "activated_at": r.activated_at,
        }
        for r in rows
    ]


@app.post("/api/model-configs")
def create_model_config(payload: ModelConfigPayload, db=Depends(get_db)):
    """Create a new draft ModelConfig."""
    cfg = create_draft_config(
        db=db,
        name=payload.name,
        config_json=payload.config_json,
        created_by=payload.created_by,
        change_note=payload.change_note,
    )
    return {"id": cfg.id, "status": cfg.status, "version": cfg.version}


@app.post("/api/model-configs/{cfg_id}/clone")
def clone_model_config(cfg_id: str, actor: str = "system", db=Depends(get_db)):
    """Clone an existing ModelConfig into a new draft."""
    try:
        cfg = clone_config(db=db, source_id=cfg_id, actor=actor)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"id": cfg.id, "status": cfg.status, "version": cfg.version, "parent_id": cfg.parent_id}


@app.patch("/api/model-configs/{cfg_id}")
def edit_model_config(cfg_id: str, payload: ModelConfigUpdatePayload, db=Depends(get_db)):
    """Edit a draft ModelConfig."""
    try:
        cfg = update_draft_config(
            db=db,
            cfg_id=cfg_id,
            new_config_json=payload.config_json,
            actor=payload.actor,
            change_note=payload.change_note,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"id": cfg.id, "status": cfg.status, "version": cfg.version}


@app.post("/api/model-configs/{cfg_id}/activate")
def activate_model_config(cfg_id: str, payload: ModelConfigActivatePayload, db=Depends(get_db)):
    """Activate a ModelConfig and (optionally) set as default for reports."""
    try:
        cfg = activate_config(
            db=db, cfg_id=cfg_id, actor=payload.actor, set_as_default=payload.set_as_default
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"id": cfg.id, "status": cfg.status, "version": cfg.version, "activated_at": cfg.activated_at}


@app.post("/api/model-configs/{cfg_id}/archive")
def archive_model_config(cfg_id: str, actor: str = "system", db=Depends(get_db)):
    """Archive a ModelConfig (cannot be activated again)."""
    try:
        cfg = archive_config(db=db, cfg_id=cfg_id, actor=actor)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"id": cfg.id, "status": cfg.status, "version": cfg.version}


@app.get("/api/model-configs/{cfg_id}")
def get_model_config_detail(cfg_id: str, db=Depends(get_db)):
    cfg = db.get(ORMModelConfig, cfg_id)
    if not cfg:
        raise HTTPException(status_code=404, detail="Config not found")
    return {
        "id": cfg.id,
        "name": cfg.name,
        "status": cfg.status,
        "version": cfg.version,
        "parent_id": cfg.parent_id,
        "created_at": cfg.created_at,
        "updated_at": cfg.updated_at,
        "created_by": cfg.created_by,
        "change_note": cfg.change_note,
        "activated_at": cfg.activated_at,
        "config_json": cfg.config_json,
    }


@app.get("/api/model-configs/{cfg_id}/audit")
def get_model_config_audit(cfg_id: str, db=Depends(get_db)):
    cfg = db.get(ORMModelConfig, cfg_id)
    if not cfg:
        raise HTTPException(status_code=404, detail="Config not found")
    audits = (
        db.query(ModelConfigAudit)
        .filter(ModelConfigAudit.model_config_id == cfg_id)
        .order_by(ModelConfigAudit.created_at.desc())
        .all()
    )
    return [
        {
            "id": a.id,
            "actor": a.actor,
            "action": a.action,
            "diff_json": a.diff_json,
            "created_at": a.created_at,
        }
        for a in audits
    ]

# Initialize sample datasets
DATASETS["sample-weekly-01"] = {"path": SAMPLE_DIR / "sample-weekly-01.csv", "type": "sales"}
DATASETS["sample-weekly-realistic"] = {"path": SAMPLE_DIR / "sample-weekly-realistic.csv", "type": "sales"}
DATASETS["sample-attribution-weekly"] = {"path": SAMPLE_DIR / "sample-attribution-weekly.csv", "type": "attribution"}
DATASETS["sample-weekly-campaigns"] = {"path": SAMPLE_DIR / "sample-weekly-campaigns.csv", "type": "sales"}

def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _default_reporting_currency() -> str:
    # For now, we treat USD as the default reporting currency.
    # This could later be made configurable without changing the API shape.
    return "USD"


def _with_converted_amount(entry: ExpenseEntry) -> ExpenseEntry:
    """
    Ensure converted_amount is populated.
    For now we assume reporting_currency == currency and fx_rate == 1.0 by default.
    This keeps behaviour predictable while surfacing FX metadata in the UI.
    """
    if entry.reporting_currency is None:
        entry.reporting_currency = _default_reporting_currency()
    if entry.converted_amount is None:
        # If no explicit FX information, assume 1:1 conversion for now.
        entry.fx_rate = entry.fx_rate or 1.0
        entry.converted_amount = entry.amount * (entry.fx_rate or 1.0)
    return entry


# Load sample expenses
EXPENSES = {
    "google_ads_2024-01": _with_converted_amount(
        ExpenseEntry(
            channel="google_ads",
            cost_type="Media Spend",
            amount=12500.00,
            currency="USD",
            reporting_currency=_default_reporting_currency(),
            period="2024-01",
            service_period_start="2024-01-01",
            service_period_end="2024-01-31",
            notes="Google Ads Jan 2024",
            source_type="import",
            source_name="google_ads",
            actor_type="import",
            created_at=_now_iso(),
        )
    ),
    "meta_ads_2024-01": _with_converted_amount(
        ExpenseEntry(
            channel="meta_ads",
            cost_type="Media Spend",
            amount=8200.00,
            currency="USD",
            reporting_currency=_default_reporting_currency(),
            period="2024-01",
            service_period_start="2024-01-01",
            service_period_end="2024-01-31",
            notes="Meta Ads Jan 2024",
            source_type="import",
            source_name="meta_ads",
            actor_type="import",
            created_at=_now_iso(),
        )
    ),
    "linkedin_ads_2024-01": _with_converted_amount(
        ExpenseEntry(
            channel="linkedin_ads",
            cost_type="Media Spend",
            amount=3500.00,
            currency="USD",
            reporting_currency=_default_reporting_currency(),
            period="2024-01",
            service_period_start="2024-01-01",
            service_period_end="2024-01-31",
            notes="LinkedIn Ads Jan 2024",
            source_type="import",
            source_name="linkedin_ads",
            actor_type="import",
            created_at=_now_iso(),
        )
    ),
    "email_2024-01": _with_converted_amount(
        ExpenseEntry(
            channel="email",
            cost_type="Tools/Software",
            amount=450.00,
            currency="USD",
            reporting_currency=_default_reporting_currency(),
            period="2024-01",
            service_period_start="2024-01-01",
            service_period_end="2024-01-31",
            notes="Email platform cost Jan 2024",
            source_type="manual",
            source_name=None,
            actor_type="manual",
            created_at=_now_iso(),
        )
    ),
    "whatsapp_2024-01": _with_converted_amount(
        ExpenseEntry(
            channel="whatsapp",
            cost_type="Tools/Software",
            amount=280.00,
            currency="USD",
            reporting_currency=_default_reporting_currency(),
            period="2024-01",
            service_period_start="2024-01-01",
            service_period_end="2024-01-31",
            notes="WhatsApp Business Jan 2024",
            source_type="manual",
            source_name=None,
            actor_type="manual",
            created_at=_now_iso(),
        )
    ),
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


@app.post("/api/taxonomy/validate-utm")
def validate_utm_endpoint(params: Dict[str, Any]):
    """
    Validate UTM parameters and return normalized values with warnings/errors.
    
    Example: {"utm_source": "google", "utm_medium": "cpc", "utm_campaign": "brand"}
    """
    from app.services_taxonomy import validate_utm_params
    
    result = validate_utm_params(params)
    return {
        "is_valid": result.is_valid,
        "warnings": result.warnings,
        "errors": result.errors,
        "normalized": result.normalized,
        "confidence": result.confidence,
    }


@app.post("/api/taxonomy/map-channel")
def map_channel_endpoint(body: Dict[str, Any]):
    """
    Map source/medium to channel with confidence score.
    
    Example: {"source": "google", "medium": "cpc"}
    """
    from app.services_taxonomy import map_to_channel
    
    source = body.get("source")
    medium = body.get("medium")
    
    mapping = map_to_channel(source, medium)
    return {
        "channel": mapping.channel,
        "matched_rule": mapping.matched_rule,
        "confidence": mapping.confidence,
        "source": mapping.source,
        "medium": mapping.medium,
        "fallback_reason": mapping.fallback_reason,
    }


@app.get("/api/taxonomy/unknown-share")
def get_unknown_share(limit: int = 20, db=Depends(get_db)):
    """
    Get unknown/unmapped traffic report.
    
    Returns unknown share, breakdowns by source/medium, and sample unmapped touchpoints.
    """
    from app.services_taxonomy import compute_unknown_share
    from app.services_conversions import load_journeys_from_db
    
    journeys = load_journeys_from_db(db, limit=10000)
    if not journeys:
        return {
            "total_touchpoints": 0,
            "unknown_count": 0,
            "unknown_share": 0.0,
            "by_source": {},
            "by_medium": {},
            "top_unmapped_patterns": [],
            "sample_unmapped": [],
        }
    
    report = compute_unknown_share(journeys, sample_size=limit)
    
    # Convert tuple keys to strings for JSON
    top_patterns = sorted(
        [(k, v) for k, v in report.by_source_medium.items()],
        key=lambda x: -x[1]
    )[:limit]
    
    return {
        "total_touchpoints": report.total_touchpoints,
        "unknown_count": report.unknown_count,
        "unknown_share": report.unknown_share,
        "by_source": report.by_source,
        "by_medium": report.by_medium,
        "top_unmapped_patterns": [
            {"source": s, "medium": m, "count": count}
            for (s, m), count in top_patterns
        ],
        "sample_unmapped": report.sample_unmapped,
    }


@app.get("/api/taxonomy/coverage")
def get_taxonomy_coverage(db=Depends(get_db)):
    """
    Get taxonomy coverage report.
    
    Returns channel distribution, source/medium coverage, rule usage, and top unmapped patterns.
    """
    from app.services_taxonomy import compute_taxonomy_coverage
    from app.services_conversions import load_journeys_from_db
    
    journeys = load_journeys_from_db(db, limit=10000)
    if not journeys:
        return {
            "channel_distribution": {},
            "source_coverage": 0.0,
            "medium_coverage": 0.0,
            "rule_usage": {},
            "top_unmapped_patterns": [],
        }
    
    coverage = compute_taxonomy_coverage(journeys)
    return coverage


@app.get("/api/taxonomy/channel-confidence")
def get_channel_confidence(channel: str, db=Depends(get_db)):
    """
    Get confidence metrics for a specific channel.
    
    Returns mean confidence, touchpoint count, low-confidence share, and samples.
    """
    from app.services_taxonomy import compute_channel_confidence
    from app.services_conversions import load_journeys_from_db
    
    journeys = load_journeys_from_db(db, limit=10000)
    if not journeys:
        return {
            "mean_confidence": 0.0,
            "touchpoint_count": 0,
            "low_confidence_count": 0,
            "low_confidence_share": 0.0,
            "sample_low_confidence": [],
        }
    
    confidence = compute_channel_confidence(journeys, channel)
    return confidence


@app.post("/api/taxonomy/compute-dq")
def compute_taxonomy_dq(db=Depends(get_db)):
    """
    Compute and persist taxonomy-specific DQ snapshots.
    
    Metrics: unknown_channel_share, mean_touchpoint_confidence, mean_journey_confidence,
    source_coverage, medium_coverage, low_confidence_touchpoint_share.
    """
    from app.services_taxonomy import persist_taxonomy_dq_snapshots
    from app.services_conversions import load_journeys_from_db
    
    journeys = load_journeys_from_db(db, limit=10000)
    if not journeys:
        return {"computed": 0, "message": "No journeys found"}
    
    snapshots = persist_taxonomy_dq_snapshots(db, journeys)
    
    return {
        "computed": len(snapshots),
        "metrics": [
            {
                "source": s.source,
                "metric_key": s.metric_key,
                "metric_value": s.metric_value,
                "ts_bucket": s.ts_bucket,
            }
            for s in snapshots
        ],
    }


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
def get_journeys_summary(db=Depends(get_db)):
    """Get summary of currently loaded conversion journeys."""
    global JOURNEYS
    if not JOURNEYS:
        # Attempt to hydrate in-memory journeys from the normalised store.
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        return {"loaded": False, "count": 0}

    converted = [j for j in JOURNEYS if j.get("converted", True)]
    channels: set = set()
    # Derive simple date range from touchpoint timestamps for measurement context.
    first_ts = None
    last_ts = None
    for j in JOURNEYS:
        for tp in j.get("touchpoints", []):
            channels.add(tp.get("channel", "unknown"))
            ts = tp.get("timestamp")
            if not ts:
                continue
            try:
                dt = pd.to_datetime(ts, errors="coerce")
            except Exception:
                dt = None
            if dt is None or pd.isna(dt):
                continue
            if first_ts is None or dt < first_ts:
                first_ts = dt
            if last_ts is None or dt > last_ts:
                last_ts = dt

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
        "date_min": first_ts.isoformat() if first_ts is not None else None,
        "date_max": last_ts.isoformat() if last_ts is not None else None,
    }

@app.post("/api/attribution/journeys/upload")
async def upload_journeys(file: UploadFile = File(...), db=Depends(get_db)):
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
    # Persist into normalised conversion path storage for reuse by DQ and other APIs.
    persist_journeys_as_conversion_paths(db, JOURNEYS, replace=True)
    return {"count": len(JOURNEYS), "message": f"Loaded {len(JOURNEYS)} journeys"}

@app.post("/api/attribution/journeys/from-cdp")
def import_journeys_from_cdp(
    mapping: AttributionMappingConfig = AttributionMappingConfig(),
    config_id: Optional[str] = None,
    db=Depends(get_db),
):
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

    journeys = parse_conversion_paths(
        profiles,
        touchpoint_attr=mapping.touchpoint_attr,
        value_attr=mapping.value_attr,
        id_attr=mapping.id_attr,
        channel_field=mapping.channel_field,
        timestamp_field=mapping.timestamp_field,
    )
    # Apply model config if provided (windows + conversion key annotation)
    effective_config_id = config_id or get_default_config_id()
    if effective_config_id:
        cfg = db.get(ORMModelConfig, effective_config_id)
        if cfg:
            journeys = apply_model_config_to_journeys(journeys, cfg.config_json or {})

    JOURNEYS = journeys
    # Persist parsed journeys into the normalised ConversionPath table.
    persist_journeys_as_conversion_paths(db, JOURNEYS, replace=True)
    return {"count": len(JOURNEYS), "message": f"Parsed {len(JOURNEYS)} journeys from CDP data"}

@app.post("/api/attribution/journeys/load-sample")
def load_sample_journeys(db=Depends(get_db)):
    """Load the built-in sample conversion paths."""
    global JOURNEYS
    sample_file = SAMPLE_DIR / "sample-conversion-paths.json"
    if not sample_file.exists():
        raise HTTPException(status_code=404, detail="Sample data not found")
    with open(sample_file) as f:
        JOURNEYS = json.load(f)
    # Persist sample journeys so downstream APIs can operate on a consistent store.
    persist_journeys_as_conversion_paths(db, JOURNEYS, replace=True)
    return {"count": len(JOURNEYS), "message": f"Loaded {len(JOURNEYS)} sample journeys"}

@app.post("/api/attribution/run")
def run_attribution_model(model: str = "linear", config_id: Optional[str] = None, db=Depends(get_db)):
    """Run a single attribution model on loaded journeys.

    Optional config_id applies time windows and conversion keys before attribution.
    """
    global JOURNEYS
    if not JOURNEYS:
        # Lazy-load from normalised storage if nothing is in memory yet.
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded. Upload, import, or persist data first.")
    if model not in ATTRIBUTION_MODELS:
        raise HTTPException(status_code=400, detail=f"Unknown model: {model}. Available: {ATTRIBUTION_MODELS}")
    resolved_cfg, meta = load_config_and_meta(db, config_id)
    journeys_for_model = JOURNEYS
    if resolved_cfg:
        journeys_for_model = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})

    kwargs: Dict[str, Any] = {}
    if model == "time_decay":
        kwargs["half_life_days"] = SETTINGS.attribution.time_decay_half_life_days
    elif model == "position_based":
        kwargs["first_pct"] = SETTINGS.attribution.position_first_pct
        kwargs["last_pct"] = SETTINGS.attribution.position_last_pct
    result = run_attribution(journeys_for_model, model=model, **kwargs)
    # Attach config metadata for measurement context if available
    if meta:
        result["config"] = meta
    ATTRIBUTION_RESULTS[model] = result
    return result

@app.post("/api/attribution/run-all")
def run_all_attribution_models(config_id: Optional[str] = None, db=Depends(get_db)):
    """Run all attribution models on loaded journeys."""
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded. Upload, import, or persist data first.")
    resolved_cfg, meta = load_config_and_meta(db, config_id)
    journeys_for_model = JOURNEYS
    if resolved_cfg:
        journeys_for_model = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})

    results = []
    for model in ATTRIBUTION_MODELS:
        try:
            kwargs: Dict[str, Any] = {}
            if model == "time_decay":
                kwargs["half_life_days"] = SETTINGS.attribution.time_decay_half_life_days
            elif model == "position_based":
                kwargs["first_pct"] = SETTINGS.attribution.position_first_pct
                kwargs["last_pct"] = SETTINGS.attribution.position_last_pct
            result = run_attribution(journeys_for_model, model=model, **kwargs)
            if meta:
                result["config"] = meta
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


def _attribution_weekly_series(
    journeys: List[Dict],
    model: str,
    date_start: str,
    date_end: str,
    config_id: Optional[str] = None,
    db=None,
) -> List[Dict[str, Any]]:
    """Return list of { date, attributed_value } for each week by running attribution on journeys that converted that week."""
    import pandas as pd
    WEEK_FREQ = "W-MON"
    ds = pd.to_datetime(date_start, errors="coerce")
    de = pd.to_datetime(date_end, errors="coerce")
    if pd.isna(ds) or pd.isna(de) or ds > de:
        return []
    date_start_ts = ds.normalize().to_period(WEEK_FREQ).start_time
    date_end_ts = de.normalize().to_period(WEEK_FREQ).start_time
    week_range = pd.date_range(start=date_start_ts, end=date_end_ts, freq=WEEK_FREQ)

    def _conversion_week(j):
        tps = j.get("touchpoints") or []
        last_ts = None
        for tp in tps:
            ts = tp.get("timestamp")
            if not ts:
                continue
            try:
                t = pd.to_datetime(ts, errors="coerce")
                if pd.notna(t):
                    last_ts = t if last_ts is None else max(last_ts, t)
            except Exception:
                continue
        if last_ts is None:
            return None
        return last_ts.normalize().to_period(WEEK_FREQ).start_time

    resolved_cfg, meta = load_config_and_meta(db, config_id) if db else (None, None)
    journeys_for_model = apply_model_config_to_journeys(journeys, (resolved_cfg.config_json or {}) if resolved_cfg else {}) if resolved_cfg else journeys
    kwargs = {}
    if model == "time_decay" and SETTINGS.attribution.time_decay_half_life_days:
        kwargs["half_life_days"] = SETTINGS.attribution.time_decay_half_life_days
    if model == "position_based":
        kwargs["first_pct"] = SETTINGS.attribution.position_first_pct
        kwargs["last_pct"] = SETTINGS.attribution.position_last_pct

    out = []
    for w in week_range:
        week_journeys = [
            j for j in journeys_for_model
            if j.get("converted", True) and _conversion_week(j) == w
        ]
        if not week_journeys:
            out.append({"date": w.strftime("%Y-%m-%d"), "attributed_value": 0.0})
            continue
        try:
            res = run_attribution(week_journeys, model=model, **kwargs)
            out.append({"date": w.strftime("%Y-%m-%d"), "attributed_value": float(res.get("total_value", 0) or 0)})
        except Exception:
            out.append({"date": w.strftime("%Y-%m-%d"), "attributed_value": 0.0})
    return out


@app.get("/api/attribution/weekly")
def get_attribution_weekly(
    model: str = Query(..., description="Attribution model id"),
    date_start: str = Query(..., description="ISO date start"),
    date_end: str = Query(..., description="ISO date end"),
    config_id: Optional[str] = Query(None),
    db=Depends(get_db),
):
    """Return attributed value (or conversions) per week for reconciliation with MMM KPI series."""
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db, limit=50000)
    series = _attribution_weekly_series(JOURNEYS, model, date_start, date_end, config_id, db)
    return {"model": model, "config_id": config_id, "series": series}


@app.get("/api/attribution/paths")
def get_path_analysis(
    config_id: Optional[str] = None,
    direct_mode: str = "include",
    path_scope: str = "converted",
    db=Depends(get_db),
):
    """Analyze conversion paths for common patterns and statistics. Includes next-best-action recommendations per path prefix (channel and optionally campaign level).

    Optional config_id pins to a specific model config; response includes config metadata.
    """
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")

    resolved_cfg, meta = load_config_and_meta(db, config_id)
    journeys_for_analysis = JOURNEYS
    if resolved_cfg:
        journeys_for_analysis = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})

    # Apply lightweight view filters that affect only the analysis, not the
    # underlying stored journeys.
    direct_mode_normalized = (direct_mode or "include").lower()
    if direct_mode_normalized not in ("include", "exclude"):
        direct_mode_normalized = "include"

    if direct_mode_normalized == "exclude":
        filtered_journeys = []
        for j in journeys_for_analysis:
            tps = j.get("touchpoints", [])
            kept = [tp for tp in tps if tp.get("channel", "").lower() != "direct"]
            if not kept:
                # If all touchpoints were direct, drop the journey from this view
                continue
            j2 = dict(j)
            j2["touchpoints"] = kept
            filtered_journeys.append(j2)
        journeys_for_analysis = filtered_journeys

    include_non_converted = (path_scope or "converted").lower() in ("all", "all_journeys", "include_non_converted")

    path_analysis = analyze_paths(journeys_for_analysis, include_non_converted=include_non_converted)

    # NBA recommendations (channel level)
    nba_channel = compute_next_best_action(journeys_for_analysis, level="channel")
    min_support = SETTINGS.nba.min_prefix_support
    min_rate = SETTINGS.nba.min_conversion_rate
    filtered_channel: Dict[str, Any] = {}
    for prefix, recs in nba_channel.items():
        kept = [r for r in recs if r["count"] >= min_support and r["conversion_rate"] >= min_rate]
        if kept:
            filtered_channel[prefix] = kept
    path_analysis["next_best_by_prefix"] = filtered_channel

    # NBA recommendations (campaign level, optional)
    if has_any_campaign(journeys_for_analysis):
        nba_campaign = compute_next_best_action(journeys_for_analysis, level="campaign")
        filtered_campaign: Dict[str, Any] = {}
        for prefix, recs in nba_campaign.items():
            kept = [r for r in recs if r["count"] >= min_support and r["conversion_rate"] >= min_rate]
            if kept:
                filtered_campaign[prefix] = kept
        path_analysis["next_best_by_prefix_campaign"] = filtered_campaign
    path_analysis["config"] = meta
    path_analysis["view_filters"] = {
        "direct_mode": direct_mode_normalized,
        "path_scope": "all" if include_non_converted else "converted",
    }
    path_analysis["nba_config"] = {
        "min_prefix_support": SETTINGS.nba.min_prefix_support,
        "min_conversion_rate": SETTINGS.nba.min_conversion_rate,
    }
    return path_analysis


@app.get("/api/paths/details")
def get_path_details(
    path: str,
    config_id: Optional[str] = None,
    direct_mode: str = "include",
    path_scope: str = "converted",
    db=Depends(get_db),
):
    """
    Return drilldown details for a single conversion path.

    This is a read-only view that reuses the same journey set and view filters
    as /api/attribution/paths so that counts, shares, and diagnostics line up
    with the main table.
    """
    if not path:
        raise HTTPException(status_code=400, detail="path is required")

    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")

    resolved_cfg, meta = load_config_and_meta(db, config_id)
    journeys_for_analysis = JOURNEYS
    if resolved_cfg:
        journeys_for_analysis = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})

    direct_mode_normalized = (direct_mode or "include").lower()
    if direct_mode_normalized not in ("include", "exclude"):
        direct_mode_normalized = "include"

    if direct_mode_normalized == "exclude":
        filtered_journeys = []
        for j in journeys_for_analysis:
            tps = j.get("touchpoints", [])
            kept = [tp for tp in tps if tp.get("channel", "").lower() != "direct"]
            if not kept:
                continue
            j2 = dict(j)
            j2["touchpoints"] = kept
            filtered_journeys.append(j2)
        journeys_for_analysis = filtered_journeys

    include_non_converted = (path_scope or "converted").lower() in ("all", "all_journeys", "include_non_converted")

    # Build basic universe stats for share calculations
    journeys_universe = journeys_for_analysis if include_non_converted else [j for j in journeys_for_analysis if j.get("converted", True)]
    if not journeys_universe:
        raise HTTPException(status_code=400, detail="No journeys for this view.")

    target_steps = [s for s in path.split(" > ") if s]

    matching_journeys = []
    for j in journeys_universe:
        steps = [_step_string(tp, "channel") for tp in j.get("touchpoints", [])]
        if " > ".join(steps) == path:
            matching_journeys.append(j)

    total_in_view = len(journeys_universe)
    count = len(matching_journeys)

    # Summary metrics
    avg_len = 0.0
    times: list[float] = []
    from datetime import datetime as _dt
    for j in matching_journeys:
        tps = j.get("touchpoints", [])
        if tps:
            avg_len += len(tps)
        if j.get("converted", True) and len(tps) >= 2:
            try:
                first_ts = pd.Timestamp(tps[0].get("timestamp", ""))
                last_ts = pd.Timestamp(tps[-1].get("timestamp", ""))
                if pd.notna(first_ts) and pd.notna(last_ts):
                    delta = (last_ts - first_ts).total_seconds() / 86400.0
                    if delta >= 0:
                        times.append(delta)
            except Exception:
                pass

    avg_len = avg_len / count if count else 0.0
    avg_time = sum(times) / len(times) if times else None

    # Step breakdown: for each position, estimate drop-off (journeys that stop after this step)
    step_breakdown = []
    if target_steps and count:
        step_count = len(target_steps)
        for idx, step in enumerate(target_steps):
            pos = idx + 1
            prefix = target_steps[:pos]
            prefix_matches = 0
            stops_here = 0
            for j in journeys_universe:
                steps = [_step_string(tp, "channel") for tp in j.get("touchpoints", [])]
                if len(steps) < pos:
                    continue
                if steps[:pos] == prefix:
                    prefix_matches += 1
                    if len(steps) == pos:
                        stops_here += 1
            dropoff_share = stops_here / prefix_matches if prefix_matches else 0.0
            step_breakdown.append(
                {
                    "step": step,
                    "position": pos,
                    "dropoff_share": round(dropoff_share, 4),
                    "prefix_journeys": prefix_matches,
                }
            )

    # Variant paths: top 5 most similar variants by shared prefix length
    from collections import defaultdict as _dd

    variant_counts: dict[str, int] = _dd(int)
    for j in journeys_universe:
        steps = [_step_string(tp, "channel") for tp in j.get("touchpoints", [])]
        candidate = " > ".join(steps)
        if candidate == path:
            continue
        variant_counts[candidate] += 1

    def _similarity_score(other: str) -> int:
        o_steps = [s for s in other.split(" > ") if s]
        score = 0
        for a, b in zip(target_steps, o_steps):
            if a == b:
                score += 2
            else:
                break
        # small bonus if same length, small penalty otherwise
        if len(o_steps) == len(target_steps):
            score += 1
        elif abs(len(o_steps) - len(target_steps)) == 1:
            score += 0
        else:
            score -= 1
        return score

    variants = sorted(
        [
            {
                "path": pth,
                "count": c,
                "share": round(c / total_in_view, 4),
            }
            for pth, c in variant_counts.items()
        ],
        key=lambda x: (_similarity_score(x["path"]), x["count"]),
        reverse=True,
    )[:5]

    # Data health for this path
    direct_unknown_touches = 0
    total_touches = 0
    journeys_ending_direct = 0
    for j in matching_journeys:
        tps = j.get("touchpoints", [])
        for tp in tps:
            ch = tp.get("channel", "unknown")
            total_touches += 1
            if ch.lower() in ("direct", "unknown"):
                direct_unknown_touches += 1
        if tps:
            last_ch = tps[-1].get("channel", "unknown")
            if last_ch and last_ch.lower() == "direct":
                journeys_ending_direct += 1

    direct_unknown_share = (
        direct_unknown_touches / total_touches if total_touches else 0.0
    )
    ending_direct_share = journeys_ending_direct / count if count else 0.0

    confidence = None
    if meta and meta.get("conversion_key"):
        snap = get_latest_quality_for_scope(
            db=db,
            scope="channel",
            scope_id=None,
            conversion_key=meta["conversion_key"],
        )
        if snap is not None:
            confidence = {
                "score": float(snap.confidence_score),
                "label": snap.confidence_label,
                "components": snap.components_json or {},
            }

    return {
        "path": path,
        "summary": {
            "count": count,
            "share": round(count / total_in_view, 4) if total_in_view else 0.0,
            "avg_touchpoints": round(avg_len, 2),
            "avg_time_to_convert_days": round(avg_time, 2) if avg_time is not None else None,
        },
        "step_breakdown": step_breakdown,
        "variants": variants,
        "data_health": {
            "direct_unknown_touch_share": round(direct_unknown_share, 4),
            "journeys_ending_direct_share": round(ending_direct_share, 4),
            "confidence": confidence,
        },
    }


@app.get("/api/paths/archetypes")
def get_path_archetypes(
    conversion_key: Optional[str] = None,
    config_id: Optional[str] = None,
    k_mode: str = "auto",
    k: Optional[int] = None,
    k_min: int = 3,
    k_max: int = 10,
    direct_mode: str = "include",
    compare_previous: bool = False,
    recompute: bool = False,
    db=Depends(get_db),
):
    """Return simple path archetypes for current journeys."""
    global JOURNEYS, PATH_ARCHETYPES_CACHE
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")
    resolved_cfg, _meta = load_config_and_meta(db, config_id)
    journeys_for_analysis = JOURNEYS
    if resolved_cfg:
        journeys_for_analysis = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})
    # Apply direct handling as a view filter, similar to /api/attribution/paths.
    direct_mode_normalized = (direct_mode or "include").lower()
    if direct_mode_normalized not in ("include", "exclude"):
        direct_mode_normalized = "include"

    if direct_mode_normalized == "exclude":
        filtered_journeys = []
        for j in journeys_for_analysis:
            tps = j.get("touchpoints", [])
            kept = [tp for tp in tps if tp.get("channel", "").lower() != "direct"]
            if not kept:
                continue
            j2 = dict(j)
            j2["touchpoints"] = kept
            filtered_journeys.append(j2)
        journeys_for_analysis = filtered_journeys

    cache_key = (
        conversion_key or "",
        config_id or "",
        k_mode,
        int(k) if k is not None else None,
        int(k_min),
        int(k_max),
        direct_mode_normalized,
        bool(compare_previous),
        len(journeys_for_analysis),
    )

    if not recompute and cache_key in PATH_ARCHETYPES_CACHE:
        return PATH_ARCHETYPES_CACHE[cache_key]

    result = compute_path_archetypes(
        journeys_for_analysis,
        conversion_key,
        k_mode=k_mode,
        k=k,
        k_min=k_min,
        k_max=k_max,
        enable_stability=True,
        enable_compare_previous=bool(compare_previous),
    )
    # Attach view filters metadata for the frontend.
    result.setdefault("diagnostics", {})
    result["diagnostics"]["view_filters"] = {
        "direct_mode": direct_mode_normalized,
    }
    PATH_ARCHETYPES_CACHE[cache_key] = result
    return result


@app.get("/api/paths/anomalies")
def get_path_anomalies(conversion_key: Optional[str] = None, config_id: Optional[str] = None, db=Depends(get_db)):
    """Return simple anomaly hints for current journeys' paths."""
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        return {"anomalies": []}
    resolved_cfg, _meta = load_config_and_meta(db, config_id)
    journeys_for_analysis = JOURNEYS
    if resolved_cfg:
        journeys_for_analysis = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})
    anomalies = compute_path_anomalies(journeys_for_analysis, conversion_key)
    return {"anomalies": anomalies}


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

    # Best-effort "why" samples: top continuation paths that start with the
    # requested prefix, summarised from raw journeys.
    why_samples = []
    if prefix:
        from collections import defaultdict as _dd

        prefix_steps = [s for s in prefix.split(" > ") if s]
        path_counts: dict[str, int] = _dd(int)
        direct_unknown_counts: dict[str, int] = _dd(int)
        total_for_prefix = 0

        for j in JOURNEYS:
            steps = [_step_string(tp, use_level) for tp in j.get("touchpoints", [])]
            if len(steps) < len(prefix_steps):
                continue
            if steps[: len(prefix_steps)] != prefix_steps:
                continue
            full_path = " > ".join(steps)
            path_counts[full_path] += 1
            total_for_prefix += 1
            for s in steps:
                if s.split(":", 1)[0].lower() in ("direct", "unknown"):
                    direct_unknown_counts[full_path] += 1

        if total_for_prefix:
            def _dk_share(p: str) -> float:
                dk = direct_unknown_counts.get(p, 0)
                length = len(p.split(" > ")) or 1
                return dk / length

            why_samples = sorted(
                [
                    {
                        "path": pth,
                        "count": cnt,
                        "share": round(cnt / total_for_prefix, 4),
                        "direct_unknown_share": round(_dk_share(pth), 4),
                    }
                    for pth, cnt in path_counts.items()
                ],
                key=lambda x: (x["count"], -x["direct_unknown_share"]),
                reverse=True,
            )[:3]

    return {
        "path_so_far": prefix or "(start)",
        "level": use_level,
        "recommendations": filtered,
        "why_samples": why_samples,
        "nba_config": {
            "min_prefix_support": SETTINGS.nba.min_prefix_support,
            "min_conversion_rate": SETTINGS.nba.min_conversion_rate,
        },
    }


@app.get("/api/attribution/next-best-action")
@app.get("/api/attribution/next_best_action")
def get_next_best_action(path_so_far: str = "", level: str = "channel", db=Depends(get_db)):
    """
    Given a path prefix (e.g. 'google_ads' or 'google_ads > email'), return recommended next channels
    (or channel:campaign when level=campaign and data has campaign). Use comma or ' > ' to separate steps.
    """
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")
    return _next_best_action_impl(path_so_far=path_so_far, level=level)

@app.get("/api/attribution/performance")
def get_channel_performance(model: str = "linear", config_id: Optional[str] = None, db=Depends(get_db)):
    """Get channel performance metrics combining attribution with expenses.

    Optional config_id pins to a specific model config; response includes config metadata.
    """
    # Resolve config (for now only surfaced in metadata, attribution math unchanged)
    resolved_cfg, meta = load_config_and_meta(db, config_id)
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    journeys_for_model = JOURNEYS
    if resolved_cfg:
        journeys_for_model = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})

    result = ATTRIBUTION_RESULTS.get(model)
    if not result:
        if not JOURNEYS:
            JOURNEYS = load_journeys_from_db(db)
        if not JOURNEYS:
            raise HTTPException(status_code=400, detail="No journeys loaded.")
        kwargs: Dict[str, Any] = {}
        if model == "time_decay":
            kwargs["half_life_days"] = SETTINGS.attribution.time_decay_half_life_days
        elif model == "position_based":
            kwargs["first_pct"] = SETTINGS.attribution.position_first_pct
            kwargs["last_pct"] = SETTINGS.attribution.position_last_pct
        result = run_attribution(journeys_for_model, model=model, **kwargs)
        ATTRIBUTION_RESULTS[model] = result

    expense_by_channel: Dict[str, float] = {}
    for exp in EXPENSES.values():
        if getattr(exp, "status", "active") == "deleted":
            continue
        converted = exp.converted_amount if getattr(exp, "converted_amount", None) is not None else exp.amount
        expense_by_channel[exp.channel] = expense_by_channel.get(exp.channel, 0) + converted

    performance = compute_channel_performance(result, expense_by_channel)

    # Attach per-channel confidence from latest quality snapshot (if available)
    for row in performance:
        snap = get_latest_quality_for_scope(
            db,
            scope="channel",
            scope_id=row["channel"],
            conversion_key=meta["conversion_key"] if meta else None,
        )
        if snap:
            row["confidence"] = {
                "score": snap.confidence_score,
                "label": snap.confidence_label,
                "components": snap.components_json,
            }
    return {
        "model": model,
        "channels": performance,
        "total_spend": sum(expense_by_channel.values()),
        "total_attributed_value": result.get("total_value", 0),
        "total_conversions": result.get("total_conversions", 0),
        "config": meta,
    }


@app.get("/api/attribution/campaign-performance")
def get_campaign_performance(
    model: str = "linear",
    config_id: Optional[str] = None,
    conversion_key: Optional[str] = None,
    db=Depends(get_db),
):
    """Campaign-level attribution (channel:campaign). Requires touchpoints with campaign. Returns campaigns with attributed value and optional suggested next (NBA)."""
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
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
    # Resolve config and apply to journeys
    resolved_cfg, meta = load_config_and_meta(db, config_id)
    journeys_for_model = JOURNEYS
    if resolved_cfg:
        journeys_for_model = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})

    # Optional filter by conversion key (kpi_type on journeys).
    # If no explicit conversion_key is provided, fall back to model config's primary conversion when present.
    effective_conv = conversion_key or (meta.get("conversion_key") if meta else None)
    if effective_conv:
        journeys_for_model = [
            j for j in journeys_for_model if j.get("kpi_type") == effective_conv
        ]

    kwargs: Dict[str, Any] = {}
    if model == "time_decay":
        kwargs["half_life_days"] = SETTINGS.attribution.time_decay_half_life_days
    elif model == "position_based":
        kwargs["first_pct"] = SETTINGS.attribution.position_first_pct
        kwargs["last_pct"] = SETTINGS.attribution.position_last_pct
    result = run_attribution_campaign(journeys_for_model, model=model, **kwargs)
    expense_by_channel: Dict[str, float] = {}
    for exp in EXPENSES.values():
        if getattr(exp, "status", "active") == "deleted":
            continue
        converted = exp.converted_amount if getattr(exp, "converted_amount", None) is not None else exp.amount
        expense_by_channel[exp.channel] = expense_by_channel.get(exp.channel, 0) + converted

    # Mapping coverage: how much spend / value is mapped to known campaigns
    total_spend = sum(expense_by_channel.values())
    total_attributed_value = float(result.get("total_value", 0) or 0.0)
    mapped_spend = 0.0
    mapped_value = 0.0

    campaigns_list = []
    for ch in result.get("channels", []):
        step = ch["channel"]
        channel_name = step.split(":", 1)[0] if ":" in step else step
        campaign_name = step.split(":", 1)[1] if ":" in step else None
        spend = expense_by_channel.get(channel_name, 0)
        mapped_spend += spend
        attr_val = ch["attributed_value"]
        attr_conv = ch.get("attributed_conversions", 0)
        mapped_value += attr_val
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
    uplift = compute_campaign_uplift(journeys_for_model)
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

    # Attach campaign-level confidence where available
    for c in campaigns_list:
        snap = get_latest_quality_for_scope(
            db,
            scope="campaign",
            scope_id=c["campaign"],
            conversion_key=meta["conversion_key"] if meta else None,
        )
        if snap:
            c["confidence"] = {
                "score": snap.confidence_score,
                "label": snap.confidence_label,
                "components": snap.components_json,
            }
            c["confidence_score"] = snap.confidence_score

    coverage_spend_pct = (mapped_spend / total_spend * 100.0) if total_spend > 0 else 0.0
    coverage_value_pct = (mapped_value / total_attributed_value * 100.0) if total_attributed_value > 0 else 0.0

    return {
        "model": model,
        "campaigns": campaigns_list,
        "total_conversions": result.get("total_conversions", 0),
        "total_value": total_attributed_value,
        "total_spend": total_spend,
        "config": meta,
        "mapping_coverage": {
            "spend_mapped_pct": coverage_spend_pct,
            "value_mapped_pct": coverage_value_pct,
            "spend_mapped": mapped_spend,
            "spend_total": total_spend,
            "value_mapped": mapped_value,
            "value_total": total_attributed_value,
        },
    }


@app.get("/api/attribution/campaign-performance/trends")
def get_campaign_performance_trends(db=Depends(get_db)):
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
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")
    return compute_campaign_trends(JOURNEYS)


# ==================== Expense Management ====================

@app.get("/api/expenses")
def list_expenses(
    include_deleted: bool = Query(False),
    channel: Optional[str] = None,
    cost_type: Optional[str] = None,
    source_type: Optional[str] = None,
    currency: Optional[str] = None,
    status: Optional[str] = None,
    service_period_start: Optional[str] = None,
    service_period_end: Optional[str] = None,
    search: Optional[str] = None,
):
    """
    List expense entries with basic filtering.
    Soft-deleted records are excluded by default unless include_deleted=true.
    """
    items = []
    for expense_id, exp in EXPENSES.items():
        if not include_deleted and exp.status == "deleted":
            continue
        if channel and exp.channel != channel:
            continue
        if cost_type and exp.cost_type != cost_type:
            continue
        if source_type and exp.source_type != source_type:
            continue
        if currency and exp.currency != currency:
            continue
        if status and exp.status != status:
            continue
        if service_period_start and exp.service_period_start and exp.service_period_start < service_period_start:
            continue
        if service_period_end and exp.service_period_end and exp.service_period_end > service_period_end:
            continue
        if search:
            haystack = " ".join(
                filter(
                    None,
                    [
                        exp.notes,
                        exp.invoice_ref,
                        exp.external_link,
                    ],
                )
            ).lower()
            if search.lower() not in haystack:
                continue
        items.append({"id": expense_id, **exp.model_dump()})
    return items


@app.post("/api/expenses")
def add_expense(entry: ExpenseEntry):
    """
    Create a new expense entry.
    Optional change_note can be set on the entry for the audit trail.
    """
    expense_id = str(uuid.uuid4())
    now = _now_iso()
    entry.created_at = now
    entry.updated_at = now
    entry.actor_type = entry.actor_type or "manual"
    change_note = entry.change_note
    entry = _with_converted_amount(entry)
    EXPENSES[expense_id] = entry

    EXPENSE_AUDIT_LOG.append(
        ExpenseChangeEvent(
            expense_id=expense_id,
            timestamp=now,
            event_type="created",
            actor_type=entry.actor_type,
            note=change_note,
        )
    )
    return {"id": expense_id, **entry.model_dump()}


@app.patch("/api/expenses/{expense_id}")
def update_expense(expense_id: str, entry: ExpenseEntry):
    """
    Update an existing expense entry while preserving its audit history.
    Optional change_note can be set on the entry.
    """
    if expense_id not in EXPENSES:
        raise HTTPException(status_code=404, detail="Expense not found")
    existing = EXPENSES[expense_id]
    now = _now_iso()
    # Preserve created_at if already set
    entry.created_at = existing.created_at or now
    entry.updated_at = now
    entry.actor_type = entry.actor_type or "manual"
    change_note = entry.change_note
    entry = _with_converted_amount(entry)
    EXPENSES[expense_id] = entry

    EXPENSE_AUDIT_LOG.append(
        ExpenseChangeEvent(
            expense_id=expense_id,
            timestamp=now,
            event_type="updated",
            actor_type=entry.actor_type,
            note=change_note,
        )
    )
    return {"id": expense_id, **entry.model_dump()}


@app.delete("/api/expenses/{expense_id}")
def delete_expense(expense_id: str, change_note: Optional[str] = Query(None)):
    """
    Soft-delete an expense entry.
    """
    if expense_id not in EXPENSES:
        raise HTTPException(status_code=404, detail="Expense not found")
    entry = EXPENSES[expense_id]
    if entry.status == "deleted":
        return {"id": expense_id, **entry.model_dump()}

    now = _now_iso()
    entry.status = "deleted"
    entry.deleted_at = now
    entry.updated_at = now
    entry.change_note = change_note
    EXPENSES[expense_id] = entry

    EXPENSE_AUDIT_LOG.append(
        ExpenseChangeEvent(
            expense_id=expense_id,
            timestamp=now,
            event_type="deleted",
            actor_type=entry.actor_type or "manual",
            note=change_note,
        )
    )
    return {"id": expense_id, **entry.model_dump()}


@app.post("/api/expenses/{expense_id}/restore")
def restore_expense(expense_id: str, change_note: Optional[str] = Query(None)):
    """
    Restore a soft-deleted expense entry.
    """
    if expense_id not in EXPENSES:
        raise HTTPException(status_code=404, detail="Expense not found")
    entry = EXPENSES[expense_id]
    if entry.status != "deleted":
        return {"id": expense_id, **entry.model_dump()}

    now = _now_iso()
    entry.status = "active"
    entry.deleted_at = None
    entry.updated_at = now
    entry.change_note = change_note
    EXPENSES[expense_id] = entry

    EXPENSE_AUDIT_LOG.append(
        ExpenseChangeEvent(
            expense_id=expense_id,
            timestamp=now,
            event_type="restored",
            actor_type=entry.actor_type or "manual",
            note=change_note,
        )
    )
    return {"id": expense_id, **entry.model_dump()}


@app.get("/api/expenses/{expense_id}/audit")
def get_expense_audit(expense_id: str):
    """
    Return audit trail events for a single expense.
    """
    events = [e.model_dump() for e in EXPENSE_AUDIT_LOG if e.expense_id == expense_id]
    # Sort by timestamp ascending for timeline display
    events.sort(key=lambda e: e["timestamp"])
    return events


@app.get("/api/expenses/summary")
def expense_summary(
    include_deleted: bool = Query(False),
    service_period_start: Optional[str] = None,
    service_period_end: Optional[str] = None,
):
    """
    Get aggregated expense summary by channel.
    Uses converted_amount in reporting currency and can be filtered by service period.
    """
    by_channel: Dict[str, float] = {}
    total_manual = 0.0
    total_imported = 0.0
    total_unknown = 0.0
    reporting_currency = _default_reporting_currency()

    for exp in EXPENSES.values():
        if not include_deleted and getattr(exp, "status", "active") == "deleted":
            continue
        if service_period_start and exp.service_period_start and exp.service_period_start < service_period_start:
            continue
        if service_period_end and exp.service_period_end and exp.service_period_end > service_period_end:
            continue

        converted = exp.converted_amount if exp.converted_amount is not None else exp.amount
        by_channel[exp.channel] = by_channel.get(exp.channel, 0.0) + converted

        source_type = getattr(exp, "source_type", "manual")
        if source_type == "import":
            total_imported += converted
        elif source_type == "manual":
            total_manual += converted
        else:
            total_unknown += converted

    total = sum(by_channel.values())
    imported_share_pct = (total_imported / total * 100.0) if total > 0 else 0.0

    return {
        "by_channel": by_channel,
        "total": total,
        "channels": sorted(by_channel.keys()),
        "reporting_currency": reporting_currency,
        "imported_share_pct": imported_share_pct,
        "manual_total": total_manual,
        "imported_total": total_imported,
        "unknown_total": total_unknown,
    }


# ==================== Import Health & Reconciliation ====================

# Known expense sources that can be synced (aligned with connectors)
IMPORT_SOURCES = ["google_ads", "meta_ads", "linkedin_ads"]


def _import_status_from_state(state: Dict[str, Any]) -> str:
    """Derive Healthy / Stale / Broken / Partial from sync state."""
    if not state:
        return "unknown"
    status = state.get("status") or "unknown"
    if status in ("Healthy", "Stale", "Broken", "Partial"):
        return status
    # Infer from last success / attempt
    last_ok = state.get("last_success_at")
    last_attempt = state.get("last_attempt_at")
    err = state.get("last_error")
    if err:
        return "Broken"
    if last_ok:
        return "Healthy"
    if last_attempt:
        return "Partial"
    return "unknown"


@app.get("/api/imports/health")
def get_import_health():
    """
    Return import health per source: status, last sync times, records, error hint.
    Overall freshness is computed from worst source status.
    """
    result = []
    for source in IMPORT_SOURCES:
        state = IMPORT_SYNC_STATE.get(source, {})
        status = _import_status_from_state(state)
        result.append({
            "source": source,
            "status": status,
            "last_success_at": state.get("last_success_at"),
            "last_attempt_at": state.get("last_attempt_at"),
            "records_imported": state.get("records_imported"),
            "period_start": state.get("period_start"),
            "period_end": state.get("period_end"),
            "last_error": state.get("last_error"),
            "action_hint": state.get("action_hint"),
            "syncing": source in SYNC_IN_PROGRESS,
        })
    # Overall freshness: worst status among sources that have been attempted
    status_order = {"Broken": 3, "Stale": 2, "Partial": 1, "Healthy": 0, "unknown": -1}
    attempted = [r for r in result if r["last_attempt_at"]]
    overall = "Healthy"
    if attempted:
        worst = max(attempted, key=lambda r: status_order.get(r["status"], -1))
        overall = worst["status"] if worst["status"] != "unknown" else "Stale"
    return {"sources": result, "overall_freshness": overall}


@app.post("/api/imports/sync/{source}")
def trigger_sync(
    source: str,
    since: Optional[str] = Query(None),
    until: Optional[str] = Query(None),
):
    """
    Trigger a manual sync for the given source. Uses default date range if not provided.
    Returns 409 if a sync for this source is already running.
    """
    if source not in IMPORT_SOURCES:
        raise HTTPException(status_code=404, detail=f"Unknown source: {source}")
    if source in SYNC_IN_PROGRESS:
        raise HTTPException(status_code=409, detail="Sync already in progress for this source")

    from datetime import timedelta
    today = datetime.utcnow().date()
    if not since:
        since = (today - timedelta(days=30)).isoformat()
    if not until:
        until = today.isoformat()

    SYNC_IN_PROGRESS.add(source)
    now_iso = _now_iso()
    IMPORT_SYNC_STATE.setdefault(source, {})["last_attempt_at"] = now_iso
    IMPORT_SYNC_STATE[source]["period_start"] = since
    IMPORT_SYNC_STATE[source]["period_end"] = until
    IMPORT_SYNC_STATE[source]["last_error"] = None
    IMPORT_SYNC_STATE[source]["action_hint"] = None

    try:
        if source == "meta_ads":
            token_data = get_token("meta")
            access_token = token_data.get("access_token") if token_data else None
            if not access_token:
                raise HTTPException(status_code=401, detail="No Meta access token. Connect your Meta account first.")
            # Need ad_account_id - use first from config or a placeholder
            ad_account_id = ds_config.get_effective("meta", "ad_account_id") or "me"
            r = fetch_meta(ad_account_id=ad_account_id, since=since, until=until, access_token=access_token)
        elif source == "google_ads":
            r = fetch_google(segments_date_from=since, segments_date_to=until)
        elif source == "linkedin_ads":
            token_data = get_token("linkedin")
            access_token = token_data.get("access_token") if token_data else None
            if not access_token:
                raise HTTPException(status_code=401, detail="No LinkedIn access token. Connect your account first.")
            r = fetch_linkedin(since=since, until=until, access_token=access_token)
        else:
            r = {"rows": 0, "path": ""}

        rows = r.get("rows", 0)
        IMPORT_SYNC_STATE[source]["last_success_at"] = _now_iso()
        IMPORT_SYNC_STATE[source]["status"] = "Healthy"
        IMPORT_SYNC_STATE[source]["records_imported"] = rows
        platform_total = 0.0
        csv_name = {"meta_ads": "meta_ads.csv", "google_ads": "google_ads.csv", "linkedin_ads": "linkedin_ads.csv"}.get(source)
        if csv_name and DATA_DIR.joinpath(csv_name).exists():
            try:
                df = pd.read_csv(DATA_DIR / csv_name)
                platform_total = float(df["spend"].sum()) if "spend" in df.columns else 0.0
            except Exception:
                pass
        IMPORT_SYNC_STATE[source]["platform_total"] = platform_total
        return {"source": source, "status": "success", "rows": rows, "platform_total": platform_total}
    except HTTPException:
        raise
    except Exception as e:
        IMPORT_SYNC_STATE[source]["status"] = "Broken"
        IMPORT_SYNC_STATE[source]["last_error"] = str(e)
        IMPORT_SYNC_STATE[source]["action_hint"] = "Reconnect credentials or check connection."
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        SYNC_IN_PROGRESS.discard(source)


@app.get("/api/imports/reconciliation")
def get_reconciliation(
    service_period_start: Optional[str] = Query(None),
    service_period_end: Optional[str] = Query(None),
):
    """
    Per-source reconciliation for the selected service period: platform total, app normalized total, delta, status.
    """
    period_start = service_period_start or ""
    period_end = service_period_end or ""

    rows = []
    for source in IMPORT_SOURCES:
        state = IMPORT_SYNC_STATE.get(source, {})
        platform_total = state.get("platform_total") or 0.0
        # App normalized total = sum of expenses from this source in period (converted amount, active only)
        app_total = 0.0
        for exp in EXPENSES.values():
            if getattr(exp, "status", "active") == "deleted":
                continue
            if getattr(exp, "source_name", None) != source:
                continue
            if period_start and getattr(exp, "service_period_start", None) and exp.service_period_start < period_start:
                continue
            if period_end and getattr(exp, "service_period_end", None) and exp.service_period_end > period_end:
                continue
            app_total += exp.converted_amount if getattr(exp, "converted_amount", None) is not None else exp.amount

        delta = platform_total - app_total
        delta_pct = (delta / platform_total * 100.0) if platform_total else 0.0
        # Status: OK if delta within 1%, Warning 1-5%, Critical >5%
        if abs(delta_pct) <= 1.0:
            rec_status = "OK"
        elif abs(delta_pct) <= 5.0:
            rec_status = "Warning"
        else:
            rec_status = "Critical"

        rows.append({
            "source": source,
            "platform_total": platform_total,
            "app_normalized_total": app_total,
            "delta": delta,
            "delta_pct": delta_pct,
            "status": rec_status,
        })
    return {"period_start": period_start, "period_end": period_end, "rows": rows}


@app.get("/api/imports/reconciliation/drilldown")
def get_reconciliation_drilldown(
    source: str,
    service_period_start: Optional[str] = Query(None),
    service_period_end: Optional[str] = Query(None),
):
    """
    Lightweight drilldown: top missing days or campaigns (from stored CSV if available).
    """
    if source not in IMPORT_SOURCES:
        raise HTTPException(status_code=404, detail="Unknown source")
    out = {"source": source, "missing_days": [], "missing_campaigns": []}
    csv_name = f"{source}.csv".replace("google_ads", "google_ads").replace("meta_ads", "meta_ads").replace("linkedin_ads", "linkedin_ads")
    path = DATA_DIR / csv_name
    if path.exists():
        try:
            df = pd.read_csv(path)
            if "date" in df.columns and service_period_start and service_period_end:
                in_range = df[df["date"].astype(str).between(service_period_start, service_period_end)]
                if not in_range.empty:
                    out["missing_days"] = in_range["date"].astype(str).unique().tolist()[:10]
            if "campaign" in df.columns:
                out["missing_campaigns"] = df["campaign"].dropna().unique().tolist()[:20]
        except Exception:
            pass
    return out


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


# ==================== Data Quality API ====================


@app.post("/api/data-quality/run")
def run_data_quality(db=Depends(get_db)):
    """Compute a new DQ snapshot bucket and evaluate alert rules."""
    started = time.time()
    snaps = compute_dq_snapshots(db)
    alerts = evaluate_alert_rules(db)
    # Derive coarse confidence snapshots for channel/campaign scopes from latest DQ metrics
    quality_snaps = compute_overall_quality_from_dq(db)
    duration_ms = int((time.time() - started) * 1000)
    return {
        "snapshots_created": len(snaps),
        "alerts_created": len(alerts),
        "latest_bucket": snaps[0].ts_bucket.isoformat() if snaps else None,
        "quality_snapshots_created": len(quality_snaps),
        "duration_ms": duration_ms,
    }


@app.get("/api/data-quality/last-run")
def get_data_quality_last_run(db=Depends(get_db)):
    """Return last DQ run metadata (derived from latest snapshot bucket)."""
    row = db.query(DQSnapshot.ts_bucket).order_by(DQSnapshot.ts_bucket.desc()).first()
    if not row:
        return {"last_bucket": None, "has_data": False}
    return {"last_bucket": row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0]), "has_data": True}


# Drilldown definitions and recommended actions per metric
_DQ_DRILLDOWN = {
    "freshness_lag_minutes": {
        "definition": "How delayed the most recent event is per source. High lag means attribution windows may miss recent touchpoints.",
        "recommended_actions": [
            "Check Meiro event ingestion delay in Data Sources",
            "Verify ad platform connector sync schedule",
            "Review Expenses reconciliation timing",
        ],
    },
    "missing_profile_pct": {
        "definition": "Share of journeys without a profile/customer ID. Without IDs, paths cannot be joined to conversions.",
        "recommended_actions": [
            "Verify gclid/fbclid capture and consent on landing pages",
            "Check Meiro identity resolution and profile stitching",
            "Ensure event tracking includes customer_id or equivalent",
        ],
    },
    "missing_timestamp_pct": {
        "definition": "Share of journeys with no touchpoint timestamps. Without timestamps, windowing and path ordering break.",
        "recommended_actions": [
            "Ensure all events include timestamp or event_time",
            "Check UTM taxonomy mapping for server-side events",
            "Verify event enrichment pipelines add timestamps",
        ],
    },
    "duplicate_id_pct": {
        "definition": "Share of duplicate profile IDs across journeys. Duplicates can cause double-counting in attribution.",
        "recommended_actions": [
            "Review identity resolution and deduplication rules",
            "Check for multiple event streams emitting same profile_id",
            "Verify Conversion Paths deduplication logic",
        ],
    },
    "conversion_attributable_pct": {
        "definition": "Share of conversions with at least one eligible touchpoint. Low values mean many conversions cannot be attributed.",
        "recommended_actions": [
            "Review eligible touchpoints config in Model Configurator",
            "Fix UTM taxonomy mapping for new sources in Taxonomy",
            "Check Connectors for missing channel mapping",
        ],
    },
}


@app.get("/api/data-quality/drilldown")
def get_data_quality_drilldown(
    metric_key: str,
    source: Optional[str] = None,
    limit: int = 10,
    db=Depends(get_db),
):
    """Return drilldown: definition, breakdown by source, top offenders (if available), recommended actions."""
    info = _DQ_DRILLDOWN.get(metric_key, {})
    definition = info.get("definition", f" metric: {metric_key}")
    recommended_actions = info.get("recommended_actions", [])

    q = db.query(DQSnapshot).filter(DQSnapshot.metric_key == metric_key).order_by(DQSnapshot.ts_bucket.desc())
    if source:
        q = q.filter(DQSnapshot.source == source)
    rows = q.limit(limit * 3).all()
    by_source: Dict[str, float] = {}
    top_offenders: List[Dict[str, Any]] = []
    seen_sources = set()
    for r in rows:
        if r.source not in seen_sources:
            seen_sources.add(r.source)
            by_source[r.source] = r.metric_value
        if r.meta_json and isinstance(r.meta_json, dict):
            offenders = r.meta_json.get("top_offenders") or r.meta_json.get("top_campaigns")
            if offenders and not top_offenders:
                for i, o in enumerate(offenders[:10]):
                    if isinstance(o, dict):
                        top_offenders.append(o)
                    elif isinstance(o, (str, int, float)):
                        top_offenders.append({"key": str(o), "value": None})
    breakdown = [{"source": s, "value": v} for s, v in sorted(by_source.items(), key=lambda x: -x[1])[:limit]]
    return {
        "metric_key": metric_key,
        "definition": definition,
        "breakdown": breakdown,
        "top_offenders": top_offenders[:10] if top_offenders else [],
        "recommended_actions": recommended_actions,
    }


@app.get("/api/data-quality/snapshots")
def list_data_quality_snapshots(
    metric_key: Optional[str] = None,
    source: Optional[str] = None,
    ts_bucket_since: Optional[str] = None,
    ts_bucket_until: Optional[str] = None,
    limit: int = 200,
    db=Depends(get_db),
):
    q = db.query(DQSnapshot).order_by(DQSnapshot.ts_bucket.desc())
    if metric_key:
        q = q.filter(DQSnapshot.metric_key == metric_key)
    if source:
        q = q.filter(DQSnapshot.source == source)
    if ts_bucket_since:
        try:
            since = datetime.fromisoformat(ts_bucket_since.replace("Z", "+00:00"))
            q = q.filter(DQSnapshot.ts_bucket >= since)
        except ValueError:
            pass
    if ts_bucket_until:
        try:
            until = datetime.fromisoformat(ts_bucket_until.replace("Z", "+00:00"))
            q = q.filter(DQSnapshot.ts_bucket <= until)
        except ValueError:
            pass
    rows = q.limit(limit).all()
    return [
        {
            "id": r.id,
            "ts_bucket": r.ts_bucket.isoformat() if hasattr(r.ts_bucket, "isoformat") else str(r.ts_bucket),
            "source": r.source,
            "metric_key": r.metric_key,
            "metric_value": r.metric_value,
            "meta": r.meta_json,
        }
        for r in rows
    ]


@app.get("/api/data-quality/alerts")
def list_data_quality_alerts(
    status: Optional[str] = None,
    severity: Optional[str] = None,
    source: Optional[str] = None,
    limit: int = 100,
    db=Depends(get_db),
):
    q = db.query(DQAlert).order_by(DQAlert.triggered_at.desc())
    if status:
        q = q.filter(DQAlert.status == status)
    rows = q.limit(limit).all()
    rule_by_id = {
        r.id: r
        for r in db.query(DQAlertRule)
        .filter(DQAlertRule.id.in_({a.rule_id for a in rows}))
        .all()
    }
    out = []
    for a in rows:
        rule = rule_by_id.get(a.rule_id)
        if severity and rule and rule.severity != severity:
            continue
        if source and rule and rule.source != source:
            continue
        out.append(
            {
                "id": a.id,
                "rule_id": a.rule_id,
                "triggered_at": a.triggered_at.isoformat() if hasattr(a.triggered_at, "isoformat") else str(a.triggered_at),
                "ts_bucket": a.ts_bucket.isoformat() if hasattr(a.ts_bucket, "isoformat") else str(a.ts_bucket),
                "metric_value": a.metric_value,
                "baseline_value": a.baseline_value,
                "status": a.status,
                "message": a.message,
                "note": getattr(a, "note", None),
                "rule": {
                    "name": rule.name if rule else None,
                    "metric_key": rule.metric_key if rule else None,
                    "source": rule.source if rule else None,
                    "severity": rule.severity if rule else None,
                }
                if rule
                else None,
            }
        )
    return out


class AlertStatusUpdate(BaseModel):
    status: str  # open/acked/resolved


class AlertNoteUpdate(BaseModel):
    note: str


@app.post("/api/data-quality/alerts/{alert_id}/status")
def update_alert_status(alert_id: int, body: AlertStatusUpdate, db=Depends(get_db)):
    alert = db.get(DQAlert, alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    alert.status = body.status
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return {"id": alert.id, "status": alert.status}


@app.post("/api/data-quality/alerts/{alert_id}/note")
def update_alert_note(alert_id: int, body: AlertNoteUpdate, db=Depends(get_db)):
    alert = db.get(DQAlert, alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    alert.note = body.note
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return {"id": alert.id, "note": alert.note}


# ==================== Explainability API ====================


class ExplainabilityDriver(BaseModel):
    metric: str
    delta: float
    current_value: float
    previous_value: float
    top_contributors: List[Dict[str, Any]] = []
    contribution_pct: Optional[float] = None  # For attributed_value: this driver's share of total change


class FeatureImportanceItem(BaseModel):
    id: str
    name: str
    importance: float  # 0–1 share in current period
    share_pct: float
    delta: float
    direction: str  # "up" | "down" | "neutral"


class NarrativeBlock(BaseModel):
    summary: str
    period_notes: List[str] = []
    config_notes: List[str] = []
    data_notes: List[str] = []


class ChangeDecompositionBucket(BaseModel):
    key: str  # "volume" | "mix" | "rate"
    label: str
    contribution: float
    pct_of_total: Optional[float] = None
    is_estimated: bool = False


class ChangeDecomposition(BaseModel):
    total_delta: float
    buckets: List[ChangeDecompositionBucket] = []
    basis: str = "journeys"  # journeys / conversions
    is_estimated: bool = True


class ComparabilityWarning(BaseModel):
    severity: str  # info | warn | critical
    message: str
    action: Optional[str] = None


class ComparabilitySummary(BaseModel):
    rating: str  # high | medium | low
    warnings: List[ComparabilityWarning] = []


class ExplainabilitySummary(BaseModel):
    period: Dict[str, Any]
    drivers: List[ExplainabilityDriver]
    data_health: Dict[str, Any]
    config: Dict[str, Any]
    mechanics: Dict[str, Any]
    feature_importance: List[FeatureImportanceItem] = []
    narrative: Optional[NarrativeBlock] = None
    change_decomposition: Optional[ChangeDecomposition] = None
    comparability: Optional[ComparabilitySummary] = None
    config_diff: Dict[str, Any] = {}
    data_quality_delta: Dict[str, Any] = {}
    timeline: List[Dict[str, Any]] = []
    channel_breakdowns: Dict[str, Any] = {}


def _filter_journeys_by_period(journeys: List[Dict[str, Any]], start: datetime, end: datetime) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for j in journeys:
        tps = j.get("touchpoints", [])
        if not tps:
            continue
        ts = tps[-1].get("timestamp")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts)
        except Exception:
            try:
                dt = datetime.fromisoformat(str(ts))
            except Exception:
                continue
        if start <= dt <= end:
            out.append(j)
    return out


def _build_explainability_narrative(
    scope: str,
    drivers: List[ExplainabilityDriver],
    curr_j_count: int,
    prev_j_count: int,
    config_info: Dict[str, Any],
    data_health: Dict[str, Any],
    mechanics: Dict[str, Any],
) -> NarrativeBlock:
    """Build human-readable narrative tied to config and data changes."""
    period_notes: List[str] = []
    if prev_j_count > 0 and curr_j_count != prev_j_count:
        pct = ((curr_j_count - prev_j_count) / prev_j_count) * 100.0
        period_notes.append(
            f"Journey count: current period {curr_j_count:,}, previous {prev_j_count:,} ({pct:+.1f}% change). "
            "Large volume shifts can affect attribution comparability."
        )
    elif curr_j_count == 0:
        period_notes.append("No journeys in the current period; metrics are not comparable.")
    else:
        period_notes.append(f"Current period: {curr_j_count:,} journeys; previous: {prev_j_count:,}.")

    config_notes: List[str] = []
    cfg_id = config_info.get("config_id")
    version = config_info.get("version")
    changes = config_info.get("changes") or []
    if cfg_id and version is not None:
        config_notes.append(f"Using measurement config '{cfg_id}' (version {version}).")
        if changes:
            config_notes.append(
                f"{len(changes)} config change(s) in the comparison window – "
                "recent changes to windows or conversion keys may affect period comparability."
            )
    else:
        config_notes.append("No versioned model config; global attribution settings apply.")

    windows = (mechanics.get("windows") or {}) or {}
    if windows.get("click_lookback_days") is not None:
        config_notes.append(f"Click lookback: {windows['click_lookback_days']} days.")

    data_notes: List[str] = []
    conf = (data_health or {}).get("confidence")
    if conf:
        data_notes.append(f"Data confidence: {conf.get('label', '—')} ({conf.get('score', 0):.0f}/100).")
    for note in (data_health or {}).get("notes") or []:
        data_notes.append(note)

    summary_parts: List[str] = []
    if scope in ("channel", "campaign") and drivers:
        d = drivers[0]
        delta_val = d.delta
        curr_val = d.current_value
        prev_val = d.previous_value
        if prev_val != 0:
            pct_change = (delta_val / prev_val) * 100.0
            summary_parts.append(
                f"Attributed value {'increased' if delta_val >= 0 else 'decreased'} by "
                f"{abs(delta_val):,.0f} ({pct_change:+.1f}%) from the previous period "
                f"(from {prev_val:,.0f} to {curr_val:,.0f})."
            )
        else:
            summary_parts.append(f"Attributed value is {curr_val:,.0f} (previous period had no value).")
        if getattr(d, "top_contributors", None) and len(d.top_contributors) > 0:
            top = d.top_contributors[0]
            summary_parts.append(f" Top positive driver: {top.get('id', '—')} (+{top.get('delta', 0):,.0f}).")
    elif scope == "paths" and drivers:
        parts = []
        for d in drivers:
            if abs(d.delta) > 0.01:
                parts.append(f"{d.metric}: {d.delta:+.2f} (now {d.current_value:.2f})")
        if parts:
            summary_parts.append("Path metrics changed: " + "; ".join(parts) + ".")
        else:
            summary_parts.append("Path metrics are stable between periods.")
    else:
        summary_parts.append("Compare the two periods above; narrative is based on journey volume and config.")

    return NarrativeBlock(
        summary=" ".join(summary_parts).strip() or "No summary available.",
        period_notes=period_notes,
        config_notes=config_notes,
        data_notes=data_notes,
    )


def _compute_change_decomposition(
    curr_val: float,
    prev_val: float,
    curr_j_count: int,
    prev_j_count: int,
) -> Optional[ChangeDecomposition]:
    """
    Best-effort decomposition of attributed value delta into
    volume vs mix vs rate/value effects.

    We treat journey count as the volume proxy and value per journey
    as the rate/value proxy. Mix is the residual.
    """
    delta_val = curr_val - prev_val
    if abs(delta_val) < 1e-9:
        return ChangeDecomposition(
            total_delta=0.0,
            buckets=[],
            basis="journeys",
            is_estimated=True,
        )

    # If we do not have both journey counts, we cannot decompose reliably.
    if curr_j_count <= 0 or prev_j_count <= 0:
        bucket = ChangeDecompositionBucket(
            key="rate",
            label="Rate / value",
            contribution=delta_val,
            pct_of_total=100.0,
            is_estimated=True,
        )
        return ChangeDecomposition(
            total_delta=delta_val,
            buckets=[bucket],
            basis="journeys",
            is_estimated=True,
        )

    prev_avg = prev_val / prev_j_count if prev_j_count > 0 else 0.0
    curr_avg = curr_val / curr_j_count if curr_j_count > 0 else 0.0

    # Simple two-factor volume × rate decomposition, treat mix as residual.
    volume_effect = (curr_j_count - prev_j_count) * prev_avg
    rate_effect = curr_j_count * (curr_avg - prev_avg)
    mix_effect = delta_val - volume_effect - rate_effect

    buckets = [
        ChangeDecompositionBucket(
            key="volume",
            label="Volume (journeys / conversions)",
            contribution=round(volume_effect, 2),
            is_estimated=True,
        ),
        ChangeDecompositionBucket(
            key="mix",
            label="Mix (channel share shift)",
            contribution=round(mix_effect, 2),
            is_estimated=True,
        ),
        ChangeDecompositionBucket(
            key="rate",
            label="Rate / value per journey",
            contribution=round(rate_effect, 2),
            is_estimated=True,
        ),
    ]

    # Compute contribution % of each bucket.
    for b in buckets:
        b.pct_of_total = (b.contribution / delta_val * 100.0) if abs(delta_val) > 1e-9 else None

    return ChangeDecomposition(
        total_delta=round(delta_val, 2),
        buckets=buckets,
        basis="journeys",
        is_estimated=True,
    )


@app.get("/api/explainability/summary", response_model=ExplainabilitySummary)
def explainability_summary(
    scope: str = Query(..., pattern="^(channel|campaign|paths)$"),
    scope_id: Optional[str] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    config_id: Optional[str] = None,
    conversion_key: Optional[str] = None,
    model: str = "linear",
    db=Depends(get_db),
):
    """
    Explainability: period-over-period drivers, feature importance, and narrative.

    - scope=channel: attributed value by channel; feature importance = share of value + delta.
    - scope=campaign: attributed value by campaign; same.
    - scope=paths: path length and time-to-convert drivers.
    Narrative ties explanations to config and data changes.
    """
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")

    # Parse period
    def _parse_iso(val: str) -> datetime:
        # Support both plain ISO and ISO with trailing 'Z', and normalize to naive UTC.
        v = val.replace("Z", "+00:00")
        dt = datetime.fromisoformat(v)
        # Drop tzinfo so we consistently compare naive datetimes everywhere.
        if dt.tzinfo is not None:
            dt = dt.astimezone(tz=None).replace(tzinfo=None)
        return dt

    try:
        if to:
            end = _parse_iso(to)
        else:
            end = datetime.utcnow()
        if from_:
            start = _parse_iso(from_)
        else:
            # Default to last 30 days
            from datetime import timedelta

            start = end - timedelta(days=30)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid from/to: {exc}")

    # Previous period of same length
    delta = end - start
    prev_end = start
    prev_start = start - delta

    resolved_cfg, meta = load_config_and_meta(db, config_id)
    journeys = JOURNEYS
    if resolved_cfg:
        journeys = apply_model_config_to_journeys(journeys, resolved_cfg.config_json or {})

    curr_j = _filter_journeys_by_period(journeys, start, end)
    prev_j = _filter_journeys_by_period(journeys, prev_start, prev_end)

    drivers: List[ExplainabilityDriver] = []
    mechanics: Dict[str, Any] = {
        "model": model,
        "windows": meta["time_window"] if meta else None,
        "eligibility": {
            "uses_model_config": resolved_cfg is not None,
        },
    }

    curr_res: Dict[str, Any] = {}
    prev_res: Dict[str, Any] = {}
    contrib_deltas: List[Dict[str, Any]] = []
    channel_breakdowns: Dict[str, Any] = {}

    if scope in ("channel", "campaign"):
        # Use attribution engine to get totals, but be defensive if there are no journeys in the period.
        if not curr_j and not prev_j:
            curr_val = 0.0
            prev_val = 0.0
        else:
            try:
                if scope == "channel":
                    curr_res = run_attribution(curr_j, model=model) if curr_j else {"total_value": 0, "channels": []}
                    prev_res = run_attribution(prev_j, model=model) if prev_j else {"total_value": 0, "channels": []}
                else:
                    curr_res = (
                        run_attribution_campaign(curr_j, model=model)
                        if curr_j
                        else {"total_value": 0, "channels": []}
                    )
                    prev_res = (
                        run_attribution_campaign(prev_j, model=model)
                        if prev_j
                        else {"total_value": 0, "channels": []}
                    )
                curr_val = float(curr_res.get("total_value", 0.0) or 0.0)
                prev_val = float(prev_res.get("total_value", 0.0) or 0.0)

                # Per-entity driver breakdown: all channels/campaigns with deltas
                def _to_map(res: Dict[str, Any]) -> Dict[str, float]:
                    m: Dict[str, float] = {}
                    for ch in res.get("channels", []):
                        key = ch.get("channel")
                        if key:
                            m[key] = float(ch.get("attributed_value", 0.0) or 0.0)
                    return m

                curr_map = _to_map(curr_res)
                prev_map = _to_map(prev_res)
                contrib_deltas = []
                keys = set(curr_map.keys()) | set(prev_map.keys())
                for k in keys:
                    cv = curr_map.get(k, 0.0)
                    pv = prev_map.get(k, 0.0)
                    contrib_deltas.append(
                        {"id": k, "delta": cv - pv, "current_value": cv, "previous_value": pv}
                    )
                contrib_deltas.sort(key=lambda x: x["delta"], reverse=True)
            except Exception:
                curr_val = 0.0
                prev_val = 0.0
                contrib_deltas = []

        delta_val = curr_val - prev_val
        # Contribution % of each top contributor to total change (for narrative)
        contribution_pct: Optional[float] = None
        if abs(delta_val) > 1e-9 and contrib_deltas:
            top_delta = contrib_deltas[0].get("delta", 0.0)
            contribution_pct = (top_delta / delta_val) * 100.0

        drivers.append(
            ExplainabilityDriver(
                metric="attributed_value",
                delta=delta_val,
                current_value=curr_val,
                previous_value=prev_val,
                top_contributors=contrib_deltas[:10],
                contribution_pct=contribution_pct,
            )
        )

        # Build per-channel breakdowns for drill-down (scope=channel only).
        if scope == "channel":
            # Helper maps for quick lookup.
            curr_channels: Dict[str, Any] = {ch.get("channel"): ch for ch in curr_res.get("channels", []) if ch.get("channel")}
            prev_channels: Dict[str, Any] = {ch.get("channel"): ch for ch in prev_res.get("channels", []) if ch.get("channel")}
            contrib_map: Dict[str, Any] = {c["id"]: c for c in contrib_deltas}

            # Simple expense aggregation by period for spend deltas (best-effort, marked estimated in UI).
            def _parse_date(d: Optional[str]) -> Optional[datetime]:
                if not d:
                    return None
                try:
                    return datetime.fromisoformat(d)
                except Exception:
                    try:
                        return datetime.fromisoformat(d.split("T")[0])
                    except Exception:
                        return None

            curr_spend: Dict[str, float] = {}
            prev_spend: Dict[str, float] = {}
            for exp in EXPENSES.values():
                if getattr(exp, "status", "active") == "deleted":
                    continue
                ch_name = exp.channel
                # Prefer service period if available, fall back to entry_date.
                d_start = _parse_date(exp.service_period_start or exp.entry_date or exp.period)
                if not d_start:
                    continue
                amount = exp.converted_amount if exp.converted_amount is not None else exp.amount
                if start.date() <= d_start.date() <= end.date():
                    curr_spend[ch_name] = curr_spend.get(ch_name, 0.0) + float(amount or 0.0)
                elif prev_start.date() <= d_start.date() <= prev_end.date():
                    prev_spend[ch_name] = prev_spend.get(ch_name, 0.0) + float(amount or 0.0)

            all_channels = set(curr_channels.keys()) | set(prev_channels.keys()) | set(contrib_map.keys())
            for cid in all_channels:
                c_curr = curr_channels.get(cid, {})
                c_prev = prev_channels.get(cid, {})
                contrib = contrib_map.get(cid, {})

                curr_val_ch = float(c_curr.get("attributed_value", 0.0) or 0.0)
                prev_val_ch = float(c_prev.get("attributed_value", 0.0) or 0.0)
                curr_conv_ch = float(c_curr.get("attributed_conversions", 0.0) or 0.0)
                prev_conv_ch = float(c_prev.get("attributed_conversions", 0.0) or 0.0)
                curr_spend_ch = float(curr_spend.get(cid, 0.0) or 0.0)
                prev_spend_ch = float(prev_spend.get(cid, 0.0) or 0.0)

                def _safe_ratio(num: float, den: float) -> Optional[float]:
                    return num / den if den > 0 else None

                curr_roas = _safe_ratio(curr_val_ch, curr_spend_ch)
                prev_roas = _safe_ratio(prev_val_ch, prev_spend_ch)
                curr_cpa = _safe_ratio(curr_spend_ch, curr_conv_ch)
                prev_cpa = _safe_ratio(prev_spend_ch, prev_conv_ch)

                channel_breakdowns[cid] = {
                    "channel": cid,
                    "spend": {
                        "current": round(curr_spend_ch, 2),
                        "previous": round(prev_spend_ch, 2),
                        "delta": round(curr_spend_ch - prev_spend_ch, 2),
                    },
                    "conversions": {
                        "current": round(curr_conv_ch, 2),
                        "previous": round(prev_conv_ch, 2),
                        "delta": round(curr_conv_ch - prev_conv_ch, 2),
                    },
                    "attributed_value": {
                        "current": round(curr_val_ch, 2),
                        "previous": round(prev_val_ch, 2),
                        "delta": round(contrib.get("delta", curr_val_ch - prev_val_ch), 2),
                    },
                    "roas": {
                        "current": round(curr_roas, 4) if curr_roas is not None else None,
                        "previous": round(prev_roas, 4) if prev_roas is not None else None,
                        "delta": round((curr_roas - prev_roas), 4) if curr_roas is not None and prev_roas is not None else None,
                    },
                    "cpa": {
                        "current": round(curr_cpa, 4) if curr_cpa is not None else None,
                        "previous": round(prev_cpa, 4) if prev_cpa is not None else None,
                        "delta": round((curr_cpa - prev_cpa), 4) if curr_cpa is not None and prev_cpa is not None else None,
                    },
                }
    elif scope == "paths":
        # Summarise average path length and time-to-convert using existing analyze_paths,
        # but be defensive: any error should degrade gracefully to zeros instead of 500.
        curr_len = 0.0
        prev_len = 0.0
        curr_time_f = 0.0
        prev_time_f = 0.0
        try:
            curr_pa = analyze_paths(curr_j) if curr_j else {}
            prev_pa = analyze_paths(prev_j) if prev_j else {}
            curr_len = float(curr_pa.get("avg_path_length") or 0.0)
            prev_len = float(prev_pa.get("avg_path_length") or 0.0)
            curr_time = curr_pa.get("avg_time_to_conversion_days")
            prev_time = prev_pa.get("avg_time_to_conversion_days")
            curr_time_f = float(curr_time or 0.0)
            prev_time_f = float(prev_time or 0.0)
        except Exception:
            # If path analysis fails for any reason, fall back to neutral deltas.
            curr_len = prev_len = 0.0
            curr_time_f = prev_time_f = 0.0

        drivers.append(
            ExplainabilityDriver(
                metric="avg_path_length",
                delta=curr_len - prev_len,
                current_value=curr_len,
                previous_value=prev_len,
                top_contributors=[],
            )
        )
        drivers.append(
            ExplainabilityDriver(
                metric="avg_time_to_conversion_days",
                delta=curr_time_f - prev_time_f,
                current_value=curr_time_f,
                previous_value=prev_time_f,
                top_contributors=[],
            )
        )

    # Data health: confidence + notes
    health_notes: List[str] = []
    conf = None
    if scope == "channel":
        conf_row = get_latest_quality_for_scope(db, "channel", scope_id, meta["conversion_key"] if meta else None)
        if conf_row:
            conf = {
                "score": conf_row.confidence_score,
                "label": conf_row.confidence_label,
                "components": conf_row.components_json,
            }
            if conf_row.confidence_label == "low":
                health_notes.append("Low confidence in channel attribution – check identity match and tracking completeness.")
    elif scope == "campaign":
        conf_row = get_latest_quality_for_scope(db, "campaign", scope_id, meta["conversion_key"] if meta else None)
        if conf_row:
            conf = {
                "score": conf_row.confidence_score,
                "label": conf_row.confidence_label,
                "components": conf_row.components_json,
            }
            if conf_row.confidence_label == "low":
                health_notes.append("Low confidence in campaign attribution – possible mapping or tracking gaps.")

    data_health = {
        "confidence": conf,
        "notes": health_notes,
    }

    # Data quality delta (current vs previous snapshot) for comparability & table.
    dq_delta: Dict[str, Any] = {"metrics": []}
    try:
        # For now, use global channel scope (scope_id=None) as the backbone for deltas.
        q = (
            db.query(AttributionQualitySnapshot)
            .filter(AttributionQualitySnapshot.scope == "channel")
            .order_by(AttributionQualitySnapshot.ts_bucket.desc())
            .limit(2)
            .all()
        )
        if q:
            current_snap = q[0]
            prev_snap = q[1] if len(q) > 1 else None
            comp_curr = current_snap.components_json or {}
            comp_prev = prev_snap.components_json if prev_snap else {}

            def _metric_row(key: str, label: str, unit: str = "") -> Dict[str, Any]:
                curr_v = None
                prev_v = None
                if key == "confidence_score":
                    curr_v = current_snap.confidence_score
                    prev_v = prev_snap.confidence_score if prev_snap else None
                else:
                    curr_v = comp_curr.get(key)
                    prev_v = comp_prev.get(key) if prev_snap else None
                delta_v = None
                if curr_v is not None and prev_v is not None:
                    delta_v = curr_v - prev_v
                return {
                    "key": key,
                    "label": label,
                    "unit": unit,
                    "current": curr_v,
                    "previous": prev_v,
                    "delta": delta_v,
                }

            dq_delta["metrics"] = [
                _metric_row("confidence_score", "Confidence score", "/100"),
                _metric_row("join_rate", "Join rate", ""),
                _metric_row("missing_rate", "Missing IDs / UTMs rate", ""),
                _metric_row("freshness_lag_minutes", "Freshness lag (minutes)", "min"),
                _metric_row("dedup_rate", "Duplication rate", ""),
            ]
    except Exception:
        dq_delta = {"metrics": []}

    # Config changes
    cfg_changes: List[Dict[str, Any]] = []
    if meta and meta.get("config_id"):
        cfg_changes = summarize_config_changes(db, meta["config_id"], prev_start)

    config_info = {
        "config_id": meta["config_id"] if meta else None,
        "version": meta["config_version"] if meta else None,
        "changes": cfg_changes,
    }

    # Human-readable config diff summary for inline display.
    config_diff: Dict[str, Any] = {
        "has_changes": bool(cfg_changes),
        "changes_count": len(cfg_changes),
        "lines": [],
    }
    if resolved_cfg:
        cfg_json = resolved_cfg.config_json or {}
        windows = cfg_json.get("windows", {})
        eligibility = cfg_json.get("eligible_touchpoints", {})
        conversions = cfg_json.get("conversions", {})
        lines: List[str] = []
        if "click_lookback_days" in windows:
            lines.append(f"Click lookback: {windows.get('click_lookback_days')}d (current)")
        if "impression_lookback_days" in windows:
            lines.append(f"Impression lookback: {windows.get('impression_lookback_days')}d (current)")
        if "conversion_latency_days" in windows:
            lines.append(f"Conversion latency: {windows.get('conversion_latency_days')}d (current)")
        if conversions.get("primary_key"):
            lines.append(f"Conversion key: {conversions.get('primary_key')}")
        if eligibility:
            lines.append("Eligibility rules updated (touchpoint filters applied).")
        if not lines and cfg_changes:
            lines.append("Config changed recently; see Settings → Measurement model configs for full details.")
        config_diff["lines"] = lines

    # Feature importance: share of attributed value in current period + direction of change
    feature_importance: List[FeatureImportanceItem] = []
    if scope in ("channel", "campaign") and curr_res.get("channels"):
        total_curr = float(curr_res.get("total_value", 0.0) or 0.0)
        delta_val_total = 0.0
        if drivers and drivers[0].metric == "attributed_value":
            delta_val_total = drivers[0].delta
        contrib_by_id = {c["id"]: c for c in contrib_deltas}
        for ch in curr_res.get("channels", []):
            cid = ch.get("channel")
            if not cid:
                continue
            share = float(ch.get("attributed_share", 0.0) or 0.0)
            contrib = contrib_by_id.get(cid, {})
            d = contrib.get("delta", 0.0)
            if abs(d) < 1e-9:
                direction = "neutral"
            else:
                direction = "up" if d > 0 else "down"
            feature_importance.append(
                FeatureImportanceItem(
                    id=cid,
                    name=cid,
                    importance=share,
                    share_pct=round(share * 100.0, 2),
                    delta=round(d, 2),
                    direction=direction,
                )
            )
        feature_importance.sort(key=lambda x: -x.importance)

    # Narrative tied to config and data changes
    narrative = _build_explainability_narrative(
        scope=scope,
        drivers=drivers,
        curr_j_count=len(curr_j),
        prev_j_count=len(prev_j),
        config_info=config_info,
        data_health=data_health,
        mechanics=mechanics,
    )

    # Serialize period datetimes for JSON
    def _dt_iso(d: datetime) -> str:
        return d.isoformat() + "Z" if d.tzinfo is None else d.isoformat()

    period_serialized = {
        "current": {"from": _dt_iso(start), "to": _dt_iso(end)},
        "previous": {"from": _dt_iso(prev_start), "to": _dt_iso(prev_end)},
    }

    # Change decomposition (volume vs mix vs rate/value)
    total_curr_val = float(curr_res.get("total_value", 0.0) or 0.0) if curr_res else 0.0
    total_prev_val = float(prev_res.get("total_value", 0.0) or 0.0) if prev_res else 0.0
    change_decomposition = _compute_change_decomposition(
        curr_val=total_curr_val,
        prev_val=total_prev_val,
        curr_j_count=len(curr_j),
        prev_j_count=len(prev_j),
    )

    # Comparability rating based on config changes, journey shifts, and data health.
    comparability_rating = "high"
    comp_warnings: List[ComparabilityWarning] = []

    # Journey volume shift
    if len(prev_j) == 0 or len(curr_j) == 0:
        comparability_rating = "low"
        comp_warnings.append(
            ComparabilityWarning(
                severity="critical",
                message="One of the periods has no journeys; results are not comparable.",
                action="Use a period with sufficient volume or adjust filters.",
            )
        )
    else:
        vol_ratio = len(curr_j) / float(len(prev_j))
        if vol_ratio > 2.0 or vol_ratio < 0.5:
            if comparability_rating != "low":
                comparability_rating = "medium"
            comp_warnings.append(
                ComparabilityWarning(
                    severity="warn",
                    message=f"Journey volume shifted significantly between periods (current {len(curr_j):,}, previous {len(prev_j):,}).",
                    action="Confirm no major tracking changes or seasonality explain this shift.",
                )
            )

    # Config changes
    if cfg_changes:
        if comparability_rating == "high":
            comparability_rating = "medium"
        comp_warnings.append(
            ComparabilityWarning(
                severity="warn",
                message=f"Model configuration changed {len(cfg_changes)} time(s) in the comparison window.",
                action="Pin analysis to a specific config version in Settings → Measurement model configs.",
            )
        )

    # Data quality issues
    if data_health.get("confidence"):
        conf_score = data_health["confidence"].get("score") or 0.0
        conf_label = data_health["confidence"].get("label", "unknown")
        if conf_label == "low" or conf_score < 60:
            comparability_rating = "low"
            comp_warnings.append(
                ComparabilityWarning(
                    severity="critical",
                    message=f"Data quality confidence is {conf_label} ({conf_score:.0f}/100).",
                    action="Investigate identifier completeness, join rate, and deduplication metrics in Data Quality.",
                )
            )
        elif conf_label == "medium" or conf_score < 80:
            if comparability_rating == "high":
                comparability_rating = "medium"
            comp_warnings.append(
                ComparabilityWarning(
                    severity="warn",
                    message=f"Data quality confidence is {conf_label} ({conf_score:.0f}/100).",
                    action="Review recent data quality alerts and fix ingestion issues before acting on fine-grained changes.",
                )
            )

    comparability = ComparabilitySummary(rating=comparability_rating, warnings=comp_warnings[:3])

    # Simple timeline: daily attributed value in current period (optionally used for "When did it change?")
    timeline: List[Dict[str, Any]] = []
    daily_values: Dict[str, float] = {}
    for j in curr_j:
        tps = j.get("touchpoints", [])
        if not tps:
            continue
        ts = tps[-1].get("timestamp")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            continue
        day_str = dt.date().isoformat()
        daily_values[day_str] = daily_values.get(day_str, 0.0) + float(j.get("conversion_value", 0.0) or 0.0)
    for day in sorted(daily_values.keys()):
        timeline.append({"date": day, "attributed_value": round(daily_values[day], 2)})

    return ExplainabilitySummary(
        period=period_serialized,
        drivers=drivers,
        data_health=data_health,
        config=config_info,
        mechanics=mechanics,
        feature_importance=feature_importance,
        narrative=narrative,
        change_decomposition=change_decomposition,
        comparability=comparability,
        config_diff=config_diff,
        data_quality_delta=dq_delta,
        timeline=timeline,
        channel_breakdowns=channel_breakdowns,
    )


# ==================== Incrementality Experiments API ====================


class ExperimentCreate(BaseModel):
    name: str
    channel: str
    start_at: datetime
    end_at: datetime
    conversion_key: Optional[str] = None
    notes: Optional[str] = None


class ExperimentSummary(BaseModel):
    id: int
    name: str
    channel: str
    start_at: datetime
    end_at: datetime
    status: str
    conversion_key: Optional[str] = None


@app.get("/api/experiments", response_model=List[ExperimentSummary])
def list_experiments(db=Depends(get_db)):
    rows = db.query(Experiment).order_by(Experiment.created_at.desc()).all()
    return [
        ExperimentSummary(
            id=r.id,
            name=r.name,
            channel=r.channel,
            start_at=r.start_at,
            end_at=r.end_at,
            status=r.status,
            conversion_key=r.conversion_key,
        )
        for r in rows
    ]


@app.post("/api/experiments", response_model=ExperimentSummary)
def create_experiment(body: ExperimentCreate, db=Depends(get_db)):
    exp = Experiment(
        name=body.name,
        channel=body.channel,
        start_at=body.start_at,
        end_at=body.end_at,
        status="draft",
        conversion_key=body.conversion_key,
        notes=body.notes,
        created_at=datetime.utcnow(),
    )
    db.add(exp)
    db.commit()
    db.refresh(exp)
    return ExperimentSummary(
        id=exp.id,
        name=exp.name,
        channel=exp.channel,
        start_at=exp.start_at,
        end_at=exp.end_at,
        status=exp.status,
        conversion_key=exp.conversion_key,
    )


@app.get("/api/experiments/{exp_id}")
def get_experiment(exp_id: int, db=Depends(get_db)):
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    return {
        "id": exp.id,
        "name": exp.name,
        "channel": exp.channel,
        "start_at": exp.start_at,
        "end_at": exp.end_at,
        "status": exp.status,
        "conversion_key": exp.conversion_key,
        "notes": exp.notes,
    }


class ExperimentStatusUpdate(BaseModel):
    status: str  # draft/running/completed


@app.post("/api/experiments/{exp_id}/status", response_model=ExperimentSummary)
def update_experiment_status(exp_id: int, body: ExperimentStatusUpdate, db=Depends(get_db)):
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    if body.status not in ("draft", "running", "completed"):
        raise HTTPException(status_code=400, detail="Invalid status")
    exp.status = body.status
    db.add(exp)
    db.commit()
    db.refresh(exp)
    return ExperimentSummary(
        id=exp.id,
        name=exp.name,
        channel=exp.channel,
        start_at=exp.start_at,
        end_at=exp.end_at,
        status=exp.status,
        conversion_key=exp.conversion_key,
    )


class AssignmentRequest(BaseModel):
    profile_ids: List[str]
    treatment_rate: float = 0.5


@app.post("/api/experiments/{exp_id}/assign")
def assign_experiment(exp_id: int, body: AssignmentRequest, db=Depends(get_db)):
    """
    Assign profiles to treatment/control using deterministic hashing.
    
    This ensures stable, reproducible assignments across calls.
    """
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    if not body.profile_ids:
        return {"assigned": 0, "treatment": 0, "control": 0}

    counts = assign_profiles_deterministic(
        db=db,
        experiment_id=exp_id,
        profile_ids=body.profile_ids,
        treatment_rate=body.treatment_rate,
    )

    return {
        "assigned": len(body.profile_ids),
        "treatment": counts["treatment"],
        "control": counts["control"],
    }


class OutcomePayload(BaseModel):
    profile_id: str
    conversion_ts: datetime
    value: float = 0.0


class OutcomesRequest(BaseModel):
    outcomes: List[OutcomePayload]


@app.post("/api/experiments/{exp_id}/outcomes")
def record_outcomes(exp_id: int, body: OutcomesRequest, db=Depends(get_db)):
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    if not body.outcomes:
        return {"inserted": 0}
    count = 0
    for o in body.outcomes:
        db.add(
            ExperimentOutcome(
                experiment_id=exp_id,
                profile_id=o.profile_id,
                conversion_ts=o.conversion_ts,
                value=o.value,
            )
        )
        count += 1
    db.commit()
    return {"inserted": count}


@app.get("/api/experiments/{exp_id}/results")
def get_experiment_results(exp_id: int, db=Depends(get_db)):
    import math

    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")

    assignments = db.query(ExperimentAssignment).filter(ExperimentAssignment.experiment_id == exp_id).all()
    if not assignments:
        return {"experiment_id": exp_id, "status": exp.status, "insufficient_data": True}

    outcomes = {
        o.profile_id: o
        for o in db.query(ExperimentOutcome).filter(ExperimentOutcome.experiment_id == exp_id).all()
    }

    treat_n = 0
    control_n = 0
    treat_conv = 0
    control_conv = 0
    treat_value = 0.0
    control_value = 0.0

    for a in assignments:
        out = outcomes.get(a.profile_id)
        if a.group == "treatment":
            treat_n += 1
            if out is not None:
                treat_conv += 1
                treat_value += float(out.value or 0.0)
        else:
            control_n += 1
            if out is not None:
                control_conv += 1
                control_value += float(out.value or 0.0)

    if treat_n == 0 or control_n == 0:
        return {"experiment_id": exp_id, "status": exp.status, "insufficient_data": True}

    p_t = treat_conv / treat_n if treat_n > 0 else 0.0
    p_c = control_conv / control_n if control_n > 0 else 0.0
    diff = p_t - p_c
    uplift_abs = diff
    uplift_rel = diff / p_c if p_c > 0 else None

    # Simple normal-approx CI for diff in proportions
    se = math.sqrt(p_t * (1 - p_t) / treat_n + p_c * (1 - p_c) / control_n)
    if se > 0:
        z = 1.96
        ci_low = diff - z * se
        ci_high = diff + z * se
        # Two-sided z-test p-value (approx)
        z_score = diff / se
        # approximate using complementary error function
        from math import erf

        p_value = 2 * (1 - 0.5 * (1 + erf(abs(z_score) / math.sqrt(2))))
    else:
        ci_low = None
        ci_high = None
        p_value = None

    # Upsert ExperimentResult
    res = db.query(ExperimentResult).filter(ExperimentResult.experiment_id == exp_id).first()
    if res is None:
        res = ExperimentResult(
            experiment_id=exp_id,
            computed_at=datetime.utcnow(),
            uplift_abs=uplift_abs,
            uplift_rel=uplift_rel,
            ci_low=ci_low,
            ci_high=ci_high,
            p_value=p_value,
            treatment_size=treat_n,
            control_size=control_n,
            meta_json={
                "treatment_conversions": treat_conv,
                "control_conversions": control_conv,
                "treatment_value": treat_value,
                "control_value": control_value,
            },
        )
        db.add(res)
    else:
        res.computed_at = datetime.utcnow()
        res.uplift_abs = uplift_abs
        res.uplift_rel = uplift_rel
        res.ci_low = ci_low
        res.ci_high = ci_high
        res.p_value = p_value
        res.treatment_size = treat_n
        res.control_size = control_n
        res.meta_json = {
            "treatment_conversions": treat_conv,
            "control_conversions": control_conv,
            "treatment_value": treat_value,
            "control_value": control_value,
        }
    db.commit()

    return {
        "experiment_id": exp_id,
        "status": exp.status,
        "treatment": {
            "n": treat_n,
            "conversions": treat_conv,
            "conversion_rate": p_t,
            "total_value": treat_value,
        },
        "control": {
            "n": control_n,
            "conversions": control_conv,
            "conversion_rate": p_c,
            "total_value": control_value,
        },
        "uplift_abs": uplift_abs,
        "uplift_rel": uplift_rel,
        "ci_low": ci_low,
        "ci_high": ci_high,
        "p_value": p_value,
        "insufficient_data": False,
    }


class ExposurePayload(BaseModel):
    profile_id: str
    exposure_ts: Optional[datetime] = None
    campaign_id: Optional[str] = None
    message_id: Optional[str] = None


class ExposuresRequest(BaseModel):
    exposures: List[ExposurePayload]


@app.post("/api/experiments/{exp_id}/exposures")
def record_experiment_exposures(exp_id: int, body: ExposuresRequest, db=Depends(get_db)):
    """
    Record exposures (e.g., message sent) for an experiment.
    
    Exposures track when treatment was actually delivered, separate from assignment.
    """
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    
    if not body.exposures:
        return {"recorded": 0}
    
    exposures = [
        {
            "profile_id": e.profile_id,
            "exposure_ts": e.exposure_ts,
            "campaign_id": e.campaign_id,
            "message_id": e.message_id,
        }
        for e in body.exposures
    ]
    
    count = record_exposures_batch(db, exp_id, exposures)
    return {"recorded": count}


@app.get("/api/experiments/{exp_id}/exposures")
def get_experiment_exposures(exp_id: int, limit: int = 100, db=Depends(get_db)):
    """Get recent exposures for an experiment."""
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    
    exposures = (
        db.query(ExperimentExposure)
        .filter(ExperimentExposure.experiment_id == exp_id)
        .order_by(ExperimentExposure.exposure_ts.desc())
        .limit(limit)
        .all()
    )
    
    return [
        {
            "profile_id": e.profile_id,
            "exposure_ts": e.exposure_ts,
            "campaign_id": e.campaign_id,
            "message_id": e.message_id,
        }
        for e in exposures
    ]


@app.get("/api/experiments/{exp_id}/time-series")
def get_experiment_time_series_endpoint(exp_id: int, freq: str = "D", db=Depends(get_db)):
    """
    Get daily/weekly time series of experiment metrics.
    
    Returns cumulative metrics over time for visualization.
    """
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    
    df = get_experiment_time_series(db, exp_id, freq=freq)
    
    if df.empty:
        return {"data": []}
    
    return {"data": df.to_dict(orient="records")}


class PowerAnalysisRequest(BaseModel):
    baseline_rate: float
    mde: float
    alpha: float = 0.05
    power: float = 0.8
    treatment_rate: float = 0.5


@app.post("/api/experiments/power-analysis")
def power_analysis(body: PowerAnalysisRequest):
    """
    Estimate required sample size for an experiment.
    
    Parameters:
    - baseline_rate: control group conversion rate (e.g., 0.05 = 5%)
    - mde: minimum detectable effect (absolute, e.g., 0.01 = 1pp)
    - alpha: significance level (default 0.05)
    - power: statistical power (default 0.8)
    - treatment_rate: fraction in treatment (default 0.5)
    
    Returns total sample size needed.
    """
    if not (0 < body.baseline_rate < 1):
        raise HTTPException(status_code=400, detail="baseline_rate must be between 0 and 1")
    if not (0 < body.mde < 1):
        raise HTTPException(status_code=400, detail="mde must be between 0 and 1")
    if not (0 < body.alpha < 1):
        raise HTTPException(status_code=400, detail="alpha must be between 0 and 1")
    if not (0 < body.power < 1):
        raise HTTPException(status_code=400, detail="power must be between 0 and 1")
    if not (0 < body.treatment_rate < 1):
        raise HTTPException(status_code=400, detail="treatment_rate must be between 0 and 1")
    
    total_n = estimate_sample_size(
        baseline_rate=body.baseline_rate,
        mde=body.mde,
        alpha=body.alpha,
        power=body.power,
        treatment_rate=body.treatment_rate,
    )
    
    n_treatment = int(total_n * body.treatment_rate)
    n_control = total_n - n_treatment
    
    return {
        "total_sample_size": total_n,
        "treatment_size": n_treatment,
        "control_size": n_control,
        "baseline_rate": body.baseline_rate,
        "mde": body.mde,
        "alpha": body.alpha,
        "power": body.power,
    }


@app.post("/api/experiments/nightly-report")
def run_nightly_report_endpoint(db=Depends(get_db)):
    """
    Run nightly report for all active experiments.
    
    Computes results and generates alerts. Intended for scheduled execution.
    """
    report = run_nightly_report(db)
    return report


class ExperimentHealth(BaseModel):
    experiment_id: int
    sample: Dict[str, int]
    exposures: Dict[str, int]
    outcomes: Dict[str, int]
    balance: Dict[str, Any]
    data_completeness: Dict[str, Dict[str, str]]
    overlap_risk: Dict[str, Any]
    ready_state: Dict[str, Any]


@app.get("/api/experiments/{exp_id}/health", response_model=ExperimentHealth)
def get_experiment_health(exp_id: int, db=Depends(get_db)):
    """
    Lightweight health summary for a single experiment.

    Designed as a trust layer for the Incrementality dashboard:
    - sample sizes and conversions by group
    - basic balance check vs an assumed 50/50 split (or close)
    - data completeness for assignments, outcomes, and exposures
    - minimal contamination / overlap heuristic
    - coarse readiness classification (not_ready / early / ready)
    """
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")

    payload = compute_experiment_health(db, exp_id)
    return ExperimentHealth(**payload)


class AutoAssignRequest(BaseModel):
    channel: str
    start_date: datetime
    end_date: datetime
    treatment_rate: float = 0.5


@app.post("/api/experiments/{exp_id}/auto-assign")
def auto_assign_experiment(exp_id: int, body: AutoAssignRequest, db=Depends(get_db)):
    """
    Automatically assign profiles based on conversion paths.
    
    Finds all profiles who had touchpoints in the specified channel during
    the experiment period and assigns them deterministically.
    
    Useful for post-hoc analysis of historical data.
    """
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    
    counts = auto_assign_from_conversion_paths(
        db=db,
        experiment_id=exp_id,
        channel=body.channel,
        start_date=body.start_date,
        end_date=body.end_date,
        treatment_rate=body.treatment_rate,
    )
    
    return {
        "treatment": counts["treatment"],
        "control": counts["control"],
        "total": counts["treatment"] + counts["control"],
    }


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
    return [
        {"dataset_id": k, "path": str(v.get("path", "")), "type": v.get("type", "sales"), "source": v.get("source", "upload"), "metadata": v.get("metadata")}
        for k, v in DATASETS.items()
    ]

@app.get("/api/datasets/{dataset_id}")
def get_dataset(dataset_id: str, preview_only: bool = True):
    dataset_info = DATASETS.get(dataset_id)
    if not dataset_info:
        raise HTTPException(status_code=404, detail="Dataset not found")
    path = dataset_info.get("path")
    if path is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    p = Path(path) if isinstance(path, str) else path
    if not p.exists():
        raise HTTPException(status_code=404, detail="Dataset not found")
    df = pd.read_csv(p).head(5) if preview_only else pd.read_csv(p)
    out = {"dataset_id": dataset_id, "columns": list(df.columns), "preview_rows": df.to_dict(orient="records"), "type": dataset_info.get("type", "sales")}
    if dataset_info.get("metadata"):
        out["metadata"] = dataset_info["metadata"]
    return out

@app.get("/api/datasets/{dataset_id}/validate")
def validate_dataset(dataset_id: str, kpi_target: Optional[str] = Query(None, description="sales | attribution for KPI suggestion bias")):
    dataset_info = DATASETS.get(dataset_id)
    if not dataset_info:
        raise HTTPException(status_code=404, detail="Dataset not found")
    path = dataset_info.get("path")
    if path is None:
        raise HTTPException(status_code=404, detail="Dataset file not found")
    p = Path(path) if isinstance(path, str) else path
    if not p.exists():
        raise HTTPException(status_code=404, detail="Dataset file not found")
    df = pd.read_csv(p)
    columns = list(df.columns)
    n_rows = len(df)
    col_info = [{"name": col, "dtype": str(df[col].dtype), "missing": int(df[col].isna().sum()), "unique": int(df[col].nunique()), "sample_values": df[col].dropna().head(3).tolist()} for col in columns]
    # Smart mapping suggestions
    suggestions = build_smart_suggestions(df, kpi_target=kpi_target)
    date_column = suggestions.get("date_column")
    date_range = None
    if date_column and date_column in df.columns:
        try:
            parsed = pd.to_datetime(df[date_column], errors="coerce")
            date_range = {"min": str(parsed.min().date()), "max": str(parsed.max().date()), "n_periods": int(parsed.nunique())}
        except Exception:
            pass
    # Legacy suggestion keys for backward compatibility
    suggestions["spend_channels"] = suggestions.get("spend_channels") or []
    suggestions["kpi_columns"] = suggestions.get("kpi_columns") or []
    is_tall = {"channel", "campaign", "spend"}.issubset(set(columns))
    warnings = []
    if n_rows < 20:
        warnings.append(f"Only {n_rows} rows.")
    if date_column and date_range and date_range["n_periods"] < 20:
        warnings.append(f"Only {date_range['n_periods']} unique dates.")
    for ci in col_info:
        if ci["missing"] > 0 and n_rows and ci["missing"] / n_rows > 0.1:
            warnings.append(f"Column '{ci['name']}' has {ci['missing']/n_rows*100:.0f}% missing values.")
    return {"dataset_id": dataset_id, "n_rows": n_rows, "n_columns": len(columns), "columns": col_info, "date_column": date_column, "date_range": date_range, "format": "tall" if is_tall else "wide", "suggestions": suggestions, "warnings": warnings}


@app.get("/api/mmm/platform-options")
def get_mmm_platform_options():
    """Return available spend channels and optional covariates for platform-built MMM datasets."""
    channels = set()
    for exp in EXPENSES.values():
        if getattr(exp, "status", "active") == "deleted":
            continue
        ch = getattr(exp, "channel", None)
        if ch:
            channels.add(ch)
    return {
        "spend_channels": sorted(channels),
        "covariates": [],  # Platform has no covariates yet; hide in UI when empty
    }


@app.post("/api/mmm/datasets/build-from-platform")
def build_mmm_dataset_from_platform_endpoint(body: BuildFromPlatformRequest, db=Depends(get_db)):
    """
    Build an MMM weekly dataset from platform data (journeys + expenses).
    Persists CSV and registers dataset_id. Returns preview, coverage, and metadata.
    """
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db, limit=50000)
    journeys = JOURNEYS
    expenses_list = list(EXPENSES.values())
    try:
        df, coverage = build_mmm_dataset_from_platform(
            journeys=journeys,
            expenses=expenses_list,
            date_start=body.date_start,
            date_end=body.date_end,
            kpi_target=body.kpi_target,
            spend_channels=body.spend_channels,
            covariates=body.covariates or [],
            currency=body.currency,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    kpi_col = "sales" if body.kpi_target == "sales" else "conversions"
    dataset_id = f"platform-mmm-{uuid.uuid4().hex[:12]}"
    dest = MMM_PLATFORM_DIR / f"{dataset_id}.csv"
    df.to_csv(dest, index=False)
    metadata = {
        "period_start": body.date_start,
        "period_end": body.date_end,
        "kpi_target": body.kpi_target,
        "kpi_column": kpi_col,
        "spend_channels": body.spend_channels,
        "covariates": body.covariates or [],
        "currency": body.currency,
        "source": "platform",
    }
    if body.kpi_target == "attribution":
        if body.attribution_model:
            metadata["attribution_model"] = body.attribution_model
        if body.attribution_config_id:
            metadata["attribution_config_id"] = body.attribution_config_id
    DATASETS[dataset_id] = {
        "path": dest,
        "type": "sales" if body.kpi_target == "sales" else "attribution",
        "source": "platform",
        "metadata": metadata,
    }
    columns = list(df.columns)
    preview = df.head(10).to_dict(orient="records")
    return {
        "dataset_id": dataset_id,
        "columns": columns,
        "preview_rows": preview,
        "coverage": coverage,
        "metadata": metadata,
        "path": str(dest),
        "type": DATASETS[dataset_id]["type"],
    }


@app.post("/api/mmm/datasets/{dataset_id}/validate-mapping")
def validate_mapping_endpoint(dataset_id: str, body: ValidateMappingRequest):
    """
    Validate an MMM column mapping before running the model.
    Returns blocking errors and warnings (e.g. weekly sanity, non-negative spend, history length, multicollinearity, missingness).
    """
    dataset_info = DATASETS.get(dataset_id)
    if not dataset_info:
        raise HTTPException(status_code=404, detail="Dataset not found")
    path = dataset_info.get("path")
    if path is None:
        raise HTTPException(status_code=404, detail="Dataset file not found")
    p = Path(path) if isinstance(path, str) else path
    if not p.exists():
        raise HTTPException(status_code=404, detail="Dataset file not found")
    df = pd.read_csv(p)
    errors, warnings, details = validate_mapping(
        df,
        date_column=body.date_column,
        kpi=body.kpi,
        spend_channels=body.spend_channels,
        covariates=body.covariates,
    )
    from app.services_mmm_mapping import get_missingness_top_offenders
    details["missingness_top"] = get_missingness_top_offenders(details, top_n=5)
    return {"errors": errors, "warnings": warnings, "details": details, "valid": len(errors) == 0}


# ==================== MMM Model Routes ====================

@app.post("/api/models")
def run_model(cfg: ModelConfig, tasks: BackgroundTasks):
    if cfg.dataset_id not in DATASETS:
        raise HTTPException(status_code=404, detail="dataset_id not found")
    if cfg.frequency == "W" and SETTINGS.mmm.frequency != "W":
        cfg.frequency = SETTINGS.mmm.frequency
    kpi_mode = getattr(cfg, "kpi_mode", "conversions") or "conversions"
    run_id = f"mmm_{uuid.uuid4().hex[:12]}"
    now = _now_iso()
    config_dict = json.loads(cfg.model_dump_json())
    dataset_meta = (DATASETS.get(cfg.dataset_id) or {}).get("metadata") or {}
    RUNS[run_id] = {
        "status": "queued",
        "config": config_dict,
        "kpi_mode": kpi_mode,
        "created_at": now,
        "updated_at": now,
        "dataset_id": cfg.dataset_id,
    }
    if dataset_meta.get("attribution_model"):
        RUNS[run_id]["attribution_model"] = dataset_meta["attribution_model"]
    if dataset_meta.get("attribution_config_id"):
        RUNS[run_id]["attribution_config_id"] = dataset_meta["attribution_config_id"]
    _save_runs()
    tasks.add_task(_fit_model, run_id, cfg)
    return {"run_id": run_id, "status": "queued"}


@app.get("/api/models")
def list_models():
    """List all MMM runs with summary for history. Most recent first."""
    items = []
    for run_id, r in RUNS.items():
        config = r.get("config") or {}
        items.append({
            "run_id": run_id,
            "status": r.get("status", "unknown"),
            "created_at": r.get("created_at"),
            "updated_at": r.get("updated_at"),
            "dataset_id": r.get("dataset_id") or config.get("dataset_id"),
            "kpi_mode": r.get("kpi_mode"),
            "kpi": config.get("kpi"),
            "n_channels": len(config.get("spend_channels") or []),
            "n_covariates": len(config.get("covariates") or []),
            "r2": r.get("r2"),
            "engine": r.get("engine"),
        })
    items.sort(key=lambda x: (x.get("created_at") or ""), reverse=True)
    return items

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
        expense_id = f"meta_ads_{since[:7]}"
        EXPENSES[expense_id] = _with_converted_amount(
            ExpenseEntry(
                channel="meta_ads",
                cost_type="Media Spend",
                amount=total_spend,
                currency="USD",
                reporting_currency=_default_reporting_currency(),
                period=since[:7],
                service_period_start=since,
                service_period_end=until,
                notes="Auto-imported from Meta Ads API",
                source_type="import",
                source_name="meta_ads",
                actor_type="import",
                created_at=_now_iso(),
            )
        )
    now_iso = _now_iso()
    IMPORT_SYNC_STATE["meta_ads"] = {
        "last_success_at": now_iso,
        "last_attempt_at": now_iso,
        "status": "Healthy",
        "records_imported": len(rows),
        "period_start": since,
        "period_end": until,
        "platform_total": total_spend,
        "last_error": None,
        "action_hint": None,
    }
    return {"rows": len(rows), "path": str(out_path)}

@app.post("/api/connectors/google")
def fetch_google(segments_date_from: str, segments_date_to: str):
    out_path = DATA_DIR / "google_ads.csv"
    if not out_path.exists():
        pd.DataFrame([], columns=["date", "channel", "campaign", "spend", "impressions", "clicks", "conversions", "revenue"]).to_csv(out_path, index=False)
    rows = int(pd.read_csv(out_path).shape[0])
    now_iso = _now_iso()
    IMPORT_SYNC_STATE["google_ads"] = {
        "last_success_at": now_iso,
        "last_attempt_at": now_iso,
        "status": "Healthy",
        "records_imported": rows,
        "period_start": segments_date_from,
        "period_end": segments_date_to,
        "platform_total": None,
        "last_error": None,
        "action_hint": None,
    }
    return {"rows": rows, "path": str(out_path)}

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
        expense_id = f"linkedin_ads_{since[:7]}"
        EXPENSES[expense_id] = _with_converted_amount(
            ExpenseEntry(
                channel="linkedin_ads",
                cost_type="Media Spend",
                amount=total_spend,
                currency="USD",
                reporting_currency=_default_reporting_currency(),
                period=since[:7],
                service_period_start=since,
                service_period_end=until,
                notes="Auto-imported from LinkedIn Ads API",
                source_type="import",
                source_name="linkedin_ads",
                actor_type="import",
                created_at=_now_iso(),
            )
        )
    now_iso = _now_iso()
    IMPORT_SYNC_STATE["linkedin_ads"] = {
        "last_success_at": now_iso,
        "last_attempt_at": now_iso,
        "status": "Healthy",
        "records_imported": len(rows),
        "period_start": since,
        "period_end": until,
        "platform_total": total_spend,
        "last_error": None,
        "action_hint": None,
    }
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
    now_ts = _now_iso()
    dataset_info = DATASETS.get(cfg.dataset_id)
    if not dataset_info:
        RUNS[run_id] = {**RUNS.get(run_id, {}), "status": "error", "detail": "Dataset not found", "updated_at": now_ts}
        _save_runs()
        return
    csv_path = dataset_info.get("path")
    p = Path(csv_path) if isinstance(csv_path, str) else csv_path
    df = pd.read_csv(p, parse_dates=["date"])
    if cfg.kpi not in df.columns:
        RUNS[run_id] = {**RUNS.get(run_id, {}), "status": "error", "detail": f"Column '{cfg.kpi}' missing", "updated_at": now_ts}
        _save_runs()
        return
    is_tall = {"channel", "campaign", "spend"}.issubset(set(df.columns))
    if not is_tall:
        for c in cfg.spend_channels:
            if c not in df.columns:
                RUNS[run_id] = {**RUNS.get(run_id, {}), "status": "error", "detail": f"Column '{c}' missing", "updated_at": now_ts}
                _save_runs()
                return
    priors = cfg.priors or {}
    adstock_cfg = {"l_max": 8, "alpha_mean": priors.get("adstock", {}).get("alpha_mean", 0.5), "alpha_sd": priors.get("adstock", {}).get("alpha_sd", 0.2)}
    saturation_cfg = {"lam_mean": priors.get("saturation", {}).get("lam_mean", 0.001), "lam_sd": priors.get("saturation", {}).get("lam_sd", 0.0005)}
    mcmc_cfg = cfg.mcmc or {"draws": 1000, "tune": 1000, "chains": 4, "target_accept": 0.9}
    use_adstock = getattr(cfg, "use_adstock", True)
    use_saturation = getattr(cfg, "use_saturation", True)
    force_engine = "ridge" if (not use_adstock and not use_saturation) else None
    random_seed = getattr(cfg, "random_seed", None)
    try:
        RUNS[run_id]["status"] = "running"
        RUNS[run_id]["updated_at"] = _now_iso()
        _save_runs()
        result = mmm_fit_model(
            df=df,
            target_column=cfg.kpi,
            channel_columns=cfg.spend_channels,
            control_columns=cfg.covariates or [],
            date_column="date",
            adstock_cfg=adstock_cfg,
            saturation_cfg=saturation_cfg,
            mcmc_cfg=mcmc_cfg,
            force_engine=force_engine,
            random_seed=random_seed,
        )
        RUNS[run_id] = {
            **RUNS[run_id],
            "status": "finished",
            "r2": result["r2"],
            "contrib": result["contrib"],
            "roi": result["roi"],
            "engine": result.get("engine", "unknown"),
            "updated_at": _now_iso(),
        }
        for key in ("campaigns", "channel_summary", "adstock_params", "saturation_params", "diagnostics"):
            if key in result:
                RUNS[run_id][key] = result[key]
        _save_runs()
    except Exception as exc:
        RUNS[run_id] = {
            **RUNS.get(run_id, {}),
            "status": "error",
            "detail": str(exc),
            "config": RUNS[run_id].get("config", {}),
            "kpi_mode": getattr(cfg, "kpi_mode", "conversions"),
            "updated_at": _now_iso(),
        }
        _save_runs()
