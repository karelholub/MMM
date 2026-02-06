from fastapi import FastAPI, BackgroundTasks, UploadFile, File, HTTPException, Query
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
import csv
import requests
from app.utils.token_store import save_token, get_token, delete_token, get_all_connected_platforms
from app.utils.encrypt import encrypt, decrypt

app = FastAPI(title="Meiro MMM API", version="0.1.0-prototype")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ModelConfig(BaseModel):
    dataset_id: str
    frequency: str = "W"
    kpi_mode: str = "conversions"  # 'conversions', 'aov', or 'profit'
    kpi: Optional[str] = None  # Deprecated, will be set based on kpi_mode
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
            data['mcmc'] = {
                "draws": 1000,
                "tune": 1000,
                "chains": 4,
                "target_accept": 0.9
            }
        # Auto-set kpi based on kpi_mode if not provided
        if 'kpi' not in data or data['kpi'] is None:
            if 'kpi_mode' in data:
                kpi_mapping = {
                    'conversions': 'conversions',
                    'aov': 'aov',
                    'profit': 'profit'
                }
                data['kpi'] = kpi_mapping.get(data['kpi_mode'], 'conversions')
        super().__init__(**data)

class OptimizeRequest(BaseModel):
    total_budget: float = 1.0
    min_spend: float = 0.5
    max_spend: float = 2.0

RUNS: Dict[str, Any] = {}
DATASETS: Dict[str, Dict[str, Any]] = {}

SAMPLE_DIR = Path(__file__).parent / "sample_data"
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Initialize sample datasets
DATASETS["sample-weekly-01"] = {
    "path": SAMPLE_DIR / "sample-weekly-01.csv",
    "type": "sales"
}
DATASETS["sample-weekly-realistic"] = {
    "path": SAMPLE_DIR / "sample-weekly-realistic.csv",
    "type": "sales"
}
DATASETS["sample-attribution-weekly"] = {
    "path": SAMPLE_DIR / "sample-attribution-weekly.csv",
    "type": "attribution"
}
DATASETS["sample-weekly-campaigns"] = {
    "path": SAMPLE_DIR / "sample-weekly-campaigns.csv",
    "type": "sales"
}

# Get base URL from environment
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173")

@app.get("/api/health")
def health():
    return {"status": "ok"}

# ==================== OAuth Routes ====================

@app.get("/api/auth/{platform}")
def start_oauth(platform: str):
    """Start OAuth flow for a platform by redirecting to authorization URL."""
    redirect_uri = f"{BASE_URL}/api/auth/callback/{platform}"
    
    if platform == "meta":
        client_id = os.getenv("META_APP_ID", "")
        if not client_id:
            raise HTTPException(status_code=500, detail="META_APP_ID not configured")
        auth_url = (
            f"https://www.facebook.com/v19.0/dialog/oauth?"
            f"client_id={client_id}&"
            f"redirect_uri={redirect_uri}&"
            f"scope=ads_read,ads_management,business_management"
        )
        return RedirectResponse(url=auth_url)
    
    elif platform == "google":
        client_id = os.getenv("GOOGLE_CLIENT_ID", "")
        if not client_id:
            raise HTTPException(status_code=500, detail="GOOGLE_CLIENT_ID not configured")
        auth_url = (
            f"https://accounts.google.com/o/oauth2/v2/auth?"
            f"client_id={client_id}&"
            f"redirect_uri={redirect_uri}&"
            f"response_type=code&"
            f"scope=https://www.googleapis.com/auth/adwords&"
            f"access_type=offline&"
            f"prompt=consent"
        )
        return RedirectResponse(url=auth_url)
    
    elif platform == "linkedin":
        client_id = os.getenv("LINKEDIN_CLIENT_ID", "")
        if not client_id:
            raise HTTPException(status_code=500, detail="LINKEDIN_CLIENT_ID not configured")
        auth_url = (
            f"https://www.linkedin.com/oauth/v2/authorization?"
            f"response_type=code&"
            f"client_id={client_id}&"
            f"redirect_uri={redirect_uri}&"
            f"scope=r_ads_reporting,r_ads,r_organization_social"
        )
        return RedirectResponse(url=auth_url)
    
    else:
        raise HTTPException(status_code=400, detail=f"Unknown platform: {platform}")

@app.get("/api/auth/callback/{platform}")
async def oauth_callback(platform: str, code: Optional[str] = Query(None), error: Optional[str] = Query(None)):
    """Handle OAuth callback and exchange code for access token."""
    if error:
        return RedirectResponse(url=f"{FRONTEND_URL}/datasources?error={platform}&message={error}")
    
    if not code:
        return RedirectResponse(url=f"{FRONTEND_URL}/datasources?error={platform}&message=no_code")
    
    redirect_uri = f"{BASE_URL}/api/auth/callback/{platform}"
    
    try:
        if platform == "meta":
            client_id = os.getenv("META_APP_ID", "")
            client_secret = os.getenv("META_APP_SECRET", "")
            if not client_id or not client_secret:
                raise HTTPException(status_code=500, detail="Meta OAuth not configured")
            
            token_url = "https://graph.facebook.com/v19.0/oauth/access_token"
            params = {
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": redirect_uri,
                "code": code,
            }
            response = requests.get(token_url, params=params, timeout=30)
            response.raise_for_status()
            token_data = response.json()
            
            # Store the token (includes access_token, expires_in, token_type)
            save_token("meta", token_data)
            return RedirectResponse(url=f"{FRONTEND_URL}/datasources?success=meta")
        
        elif platform == "google":
            client_id = os.getenv("GOOGLE_CLIENT_ID", "")
            client_secret = os.getenv("GOOGLE_CLIENT_SECRET", "")
            if not client_id or not client_secret:
                raise HTTPException(status_code=500, detail="Google OAuth not configured")
            
            token_url = "https://oauth2.googleapis.com/token"
            data = {
                "code": code,
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            }
            response = requests.post(token_url, data=data, timeout=30)
            response.raise_for_status()
            token_data = response.json()
            
            # Store the token (includes access_token, refresh_token, expires_in)
            save_token("google", token_data)
            return RedirectResponse(url=f"{FRONTEND_URL}/datasources?success=google")
        
        elif platform == "linkedin":
            client_id = os.getenv("LINKEDIN_CLIENT_ID", "")
            client_secret = os.getenv("LINKEDIN_CLIENT_SECRET", "")
            if not client_id or not client_secret:
                raise HTTPException(status_code=500, detail="LinkedIn OAuth not configured")
            
            token_url = "https://www.linkedin.com/oauth/v2/accessToken"
            data = {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": client_id,
                "client_secret": client_secret,
            }
            response = requests.post(token_url, data=data, timeout=30)
            response.raise_for_status()
            token_data = response.json()
            
            # Store the token
            save_token("linkedin", token_data)
            return RedirectResponse(url=f"{FRONTEND_URL}/datasources?success=linkedin")
        
        else:
            return RedirectResponse(url=f"{FRONTEND_URL}/datasources?error={platform}&message=unknown_platform")
    
    except requests.RequestException as e:
        print(f"OAuth error for {platform}: {e}")
        return RedirectResponse(url=f"{FRONTEND_URL}/datasources?error={platform}&message=token_exchange_failed")
    except Exception as e:
        print(f"Unexpected error for {platform}: {e}")
        return RedirectResponse(url=f"{FRONTEND_URL}/datasources?error={platform}&message=unexpected_error")

@app.get("/api/auth/status")
def auth_status():
    """Get connection status for all platforms."""
    connected = get_all_connected_platforms()
    return {"connected": connected}

@app.delete("/api/auth/{platform}")
def disconnect_platform(platform: str):
    """Disconnect a platform by deleting its tokens."""
    if delete_token(platform):
        return {"message": f"Disconnected {platform}"}
    else:
        raise HTTPException(status_code=404, detail=f"No connection found for {platform}")

@app.post("/api/datasets/upload")
async def upload_dataset(
    file: UploadFile = File(...), 
    dataset_id: Optional[str] = None,
    type: str = "sales"
):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")
    if type not in ["sales", "attribution"]:
        raise HTTPException(status_code=400, detail="Type must be 'sales' or 'attribution'")
    
    dataset_id = dataset_id or file.filename.replace(".csv", "")
    dest = SAMPLE_DIR / f"{dataset_id}.csv"
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    content = await file.read()
    dest.write_bytes(content)
    DATASETS[dataset_id] = {
        "path": dest,
        "type": type
    }
    
    # Return preview with columns
    df = pd.read_csv(dest).head(5)
    return {
        "dataset_id": dataset_id,
        "columns": list(df.columns),
        "preview_rows": df.to_dict(orient="records"),
        "path": str(dest),
        "type": type
    }

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
    
    # For dashboard, return full dataset; for preview, return first 5 rows
    if preview_only:
        df = pd.read_csv(path).head(5)
    else:
        df = pd.read_csv(path)
    
    return {
        "dataset_id": dataset_id,
        "columns": list(df.columns),
        "preview_rows": df.to_dict(orient="records"),
        "type": dataset_info.get("type", "sales")
    }

@app.post("/api/models")
def run_model(cfg: ModelConfig, tasks: BackgroundTasks):
    if cfg.dataset_id not in DATASETS:
        raise HTTPException(status_code=404, detail="dataset_id not found")
    # Create run_id with KPI mode prefix for clarity
    kpi_mode = cfg.kpi_mode if hasattr(cfg, 'kpi_mode') else 'conversions'
    run_id = f"{kpi_mode}_{len(RUNS)+1:04d}"
    RUNS[run_id] = {
        "status": "queued", 
        "config": json.loads(cfg.model_dump_json()),
        "kpi_mode": kpi_mode
    }
    tasks.add_task(_fit_model, run_id, cfg)
    return {"run_id": run_id, "status": "queued"}

@app.get("/api/models")
def list_models():
    return [{"run_id": k, **v} for k,v in RUNS.items()]

@app.get("/api/models/compare")
def compare_models():
    """
    Aggregates ROI and contribution across all model runs
    grouped by KPI mode (conversions, aov, profit).
    """
    if not RUNS:
        return []

    comparison = {}
    for run_id, run in RUNS.items():
        # Only include finished runs
        if run.get("status") != "finished":
            continue
            
        kpi_mode = run.get("kpi_mode", "conversions")
        roi_list = run.get("roi", [])
        contrib_list = run.get("contrib", [])

        for roi_entry in roi_list:
            ch = roi_entry["channel"]
            if ch not in comparison:
                comparison[ch] = {"channel": ch, "roi": {}, "contrib": {}}
            comparison[ch]["roi"][kpi_mode] = roi_entry["roi"]

        for c_entry in contrib_list:
            ch = c_entry["channel"]
            if ch not in comparison:
                comparison[ch] = {"channel": ch, "roi": {}, "contrib": {}}
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
    res = RUNS.get(run_id, {})
    return res.get("contrib", [])

@app.get("/api/models/{run_id}/roi")
def roi(run_id: str):
    res = RUNS.get(run_id, {})
    return res.get("roi", [])

@app.get("/api/connectors/status")
def connectors_status():
    sources = [
        (DATA_DIR / "meta_ads.csv", "Meta"),
        (DATA_DIR / "google_ads.csv", "Google"),
        (DATA_DIR / "linkedin_ads.csv", "LinkedIn"),
        (DATA_DIR / "unified_ads.csv", "Unified"),
    ]
    stats = {}
    for path, name in sources:
        if path.exists():
            try:
                df = pd.read_csv(path)
                spend = float(df.get("spend", pd.Series([], dtype=float)).fillna(0).sum())
                stats[name] = {
                    "path": str(path),
                    "rows": int(len(df)),
                    "total_spend": spend,
                    "last_modified": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(path.stat().st_mtime))
                }
            except Exception:
                stats[name] = {"path": str(path), "rows": 0}
        else:
            stats[name] = {"path": str(path), "rows": 0}
    return stats

@app.post("/api/connectors/meta")
def fetch_meta(ad_account_id: str, since: str, until: str, avg_aov: float = 0.0, access_token: Optional[str] = None):
    """Fetch Meta Ads campaign daily data and save to data/meta_ads.csv.
    
    If access_token is not provided, uses stored OAuth token.
    """
    # Use provided token or get from store
    if not access_token:
        token_data = get_token("meta")
        if not token_data or not token_data.get("access_token"):
            raise HTTPException(status_code=401, detail="No Meta access token available. Please connect your Meta account first.")
        access_token = token_data["access_token"]
    
    out_path = DATA_DIR / "meta_ads.csv"
    url = f"https://graph.facebook.com/v19.0/{ad_account_id}/ads"
    params = {
        "access_token": access_token,
        "fields": "campaign_name,spend,impressions,clicks,actions,updated_time",
        "date_preset": "",
        "time_range": json.dumps({"since": since, "until": until}),
        "limit": 500
    }
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
                revenue = conv * float(avg_aov)
                rows.append({
                    "date": since,  # Meta often returns aggregates; use since for prototype
                    "channel": "Meta",
                    "campaign": ad.get("campaign_name", "unknown"),
                    "spend": float(ad.get("spend", 0) or 0),
                    "impressions": int(ad.get("impressions", 0) or 0),
                    "clicks": int(ad.get("clicks", 0) or 0),
                    "conversions": float(conv),
                    "revenue": float(revenue)
                })
            paging = data.get("paging", {})
            next_url = paging.get("next")
            if not next_url:
                break
            url = next_url
            params = {}
    except Exception as e:
        # still write what we have
        pass
    pd.DataFrame(rows).to_csv(out_path, index=False)
    return {"rows": len(rows), "path": str(out_path)}

@app.post("/api/connectors/google")
def fetch_google(segments_date_from: str, segments_date_to: str):
    """Placeholder for Google Ads API fetch. Writes empty or existing file if creds not set."""
    # In production, use Google Ads client; here we expect pre-generated CSV or stub
    out_path = DATA_DIR / "google_ads.csv"
    if not out_path.exists():
        pd.DataFrame([], columns=["date","channel","campaign","spend","impressions","clicks","conversions","revenue"]).to_csv(out_path, index=False)
    return {"rows": int(pd.read_csv(out_path).shape[0]), "path": str(out_path)}

@app.post("/api/connectors/linkedin")
def fetch_linkedin(since: str, until: str, access_token: Optional[str] = None):
    """Fetch LinkedIn Ads analytics.
    
    If access_token is not provided, uses stored OAuth token.
    """
    # Use provided token or get from store
    if not access_token:
        token_data = get_token("linkedin")
        if not token_data or not token_data.get("access_token"):
            raise HTTPException(status_code=401, detail="No LinkedIn access token available. Please connect your LinkedIn account first.")
        access_token = token_data["access_token"]
    
    out_path = DATA_DIR / "linkedin_ads.csv"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {
        "q": "analytics",
        "pivot": "CAMPAIGN",
        "timeGranularity": "DAILY",
        # Additional filters would go here
    }
    rows = []
    try:
        r = requests.get("https://api.linkedin.com/v2/adAnalyticsV2", headers=headers, params=params, timeout=30)
        if r.ok:
            data = r.json().get("elements", [])
            for el in data:
                rows.append({
                    "date": since,
                    "channel": "LinkedIn",
                    "campaign": (el.get("campaign", {}) or {}).get("name", "unknown"),
                    "spend": float(el.get("costInLocalCurrency", 0) or 0),
                    "impressions": int(el.get("impressions", 0) or 0),
                    "clicks": int(el.get("clicks", 0) or 0),
                    "conversions": float(el.get("conversions", 0) or 0),
                    "revenue": float(el.get("revenueValue", 0) or 0),
                })
    except Exception:
        pass
    pd.DataFrame(rows).to_csv(out_path, index=False)
    return {"rows": len(rows), "path": str(out_path)}

@app.post("/api/connectors/merge")
def merge_ads():
    sources = [DATA_DIR / "meta_ads.csv", DATA_DIR / "google_ads.csv", DATA_DIR / "linkedin_ads.csv"]
    frames = []
    for p in sources:
        if p.exists():
            try:
                df = pd.read_csv(p)
                frames.append(df)
            except Exception:
                pass
    if not frames:
        unified = pd.DataFrame([], columns=["date","channel","campaign","spend","impressions","clicks","conversions","revenue"])
    else:
        unified = pd.concat(frames, ignore_index=True)
    # normalize and clean
    for col in ["spend","impressions","clicks","conversions","revenue"]:
        unified[col] = pd.to_numeric(unified.get(col, 0), errors='coerce').fillna(0)
    unified["date"] = pd.to_datetime(unified.get("date", pd.to_datetime([])), errors='coerce').dt.date.astype(str)
    unified["channel"] = unified.get("channel", "").astype(str)
    unified["campaign"] = unified.get("campaign", "").astype(str)
    unified.drop_duplicates(subset=["date","channel","campaign"], keep='last', inplace=True)
    out_path = DATA_DIR / "unified_ads.csv"
    unified.to_csv(out_path, index=False)
    return {"rows": int(len(unified)), "path": str(out_path)}

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
    # Build CSV
    out = io.StringIO()
    cols = ["channel","campaign","mean_spend","roi","mroas","elasticity"]
    out.write(",".join(["channel","campaign","spend","optimal_spend","roi","expected_conversions"]) + "\n")
    for row in campaigns:
        spend = float(row.get("mean_spend", 0.0))
        roi = float(row.get("roi", 0.0))
        optimal_spend = spend  # placeholder (no optimization applied yet)
        expected_conv = spend * roi
        out.write(f"{row.get('channel')},{row.get('campaign')},{spend:.4f},{optimal_spend:.4f},{roi:.6f},{expected_conv:.4f}\n")
    csv_data = out.getvalue()
    return csv_data, 200, {"Content-Type": "text/csv"}

@app.post("/api/models/{run_id}/optimize")
def optimize_budget(run_id: str, scenario: Dict[str, float]):
    """
    Receives multipliers per channel and returns predicted KPI uplift.
    Later will use full PyMC-Marketing model for posterior predictive simulation.
    """
    run = RUNS.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Model not found")
    
    roi = {r["channel"]: r["roi"] for r in run.get("roi", [])}
    contrib = {c["channel"]: c["mean_share"] for c in run.get("contrib", [])}
    
    # Calculate baseline (all multipliers = 1.0)
    baseline = sum(roi.get(ch, 0) * contrib.get(ch, 0) for ch in roi.keys())
    
    # Calculate new score with multipliers
    new_score = sum(roi.get(ch, 0) * contrib.get(ch, 0) * scenario.get(ch, 1.0) for ch in roi.keys())
    
    uplift = ((new_score - baseline) / baseline * 100) if baseline != 0 else 0
    
    return {
        "uplift": uplift,
        "predicted_kpi": new_score,
        "baseline": baseline
    }

@app.post("/api/models/{run_id}/optimize/auto")
def optimize_auto(run_id: str, request: OptimizeRequest = OptimizeRequest()):
    """
    Automatically find the optimal mix of channel budgets to maximize predicted KPI uplift.
    
    Args:
        run_id: Model run ID
        request: Optimization parameters (total_budget, min_spend, max_spend)
    
    Returns:
        Dictionary with optimal mix, predicted KPI, baseline, uplift, and message
    """
    from scipy.optimize import minimize
    import numpy as np
    import logging
    
    logger = logging.getLogger(__name__)
    
    total_budget = request.total_budget
    min_spend = request.min_spend
    max_spend = request.max_spend
    
    run = RUNS.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Model not found")

    roi = {r["channel"]: r["roi"] for r in run.get("roi", [])}
    contrib = {c["channel"]: c["mean_share"] for c in run.get("contrib", [])}

    if not roi or not contrib:
        raise HTTPException(status_code=400, detail="ROI or contribution data not available")

    channels = list(roi.keys())
    n = len(channels)
    
    # Clamp and normalize ROI values (handle negative or zero values)
    roi_values_raw = np.array([roi.get(ch, 0) for ch in channels])
    roi_values_clamped = np.maximum(roi_values_raw, 0.01)  # Clamp negative to small positive
    roi_values = roi_values_clamped
    
    # Normalize contributions
    contrib_values_raw = np.array([contrib.get(ch, 0) for ch in channels])
    contrib_sum = contrib_values_raw.sum()
    contrib_values = contrib_values_raw / contrib_sum if contrib_sum > 0 else contrib_values_raw
    
    logger.info(f"Channels: {channels}")
    logger.info(f"ROI values: {roi_values}")
    logger.info(f"Contrib values: {contrib_values}")

    # Calculate baseline score
    baseline_score = float(np.sum(roi_values * contrib_values))
    
    # Objective: maximize incremental gain = -(predicted - baseline) = baseline - predicted
    def objective(x):
        predicted = np.sum(roi_values * contrib_values * x)
        incremental_gain = predicted - baseline_score
        return -incremental_gain  # Minimize negative gain to maximize positive gain

    # Constraints: total budget must be maintained
    constraints = ({
        "type": "eq",
        "fun": lambda x: np.sum(x) - (total_budget * n)
    })

    # Bounds: each channel between min_spend and max_spend
    bounds = [(min_spend, max_spend)] * n

    # Initial guess: equal allocation (current spend ratios)
    x0 = np.ones(n) * total_budget

    try:
        result = minimize(
            objective, 
            x0, 
            method='SLSQP', 
            bounds=bounds, 
            constraints=constraints,
            options={'maxiter': 1000, 'disp': False}
        )

        if not result.success:
            logger.warning(f"Optimization warning: {result.message}")
            # Fallback: return current allocation
            optimal_mix = {ch: float(total_budget) for ch in channels}
            optimal_score = baseline_score
            message = f"Optimization couldn't improve allocation (at baseline)"
            uplift = 0.0
        else:
            optimal_mix = {ch: float(val) for ch, val in zip(channels, result.x)}
            predicted_score = float(-result.fun + baseline_score)
            optimal_score = max(predicted_score, baseline_score)
            
            # Calculate uplift
            uplift = ((optimal_score - baseline_score) / baseline_score * 100) if baseline_score > 0 else 0
            
            # Sanity check: clamp negative uplift to 0
            if uplift < 0:
                uplift = 0.0
                message = "Optimization suggests no significant improvement possible"
            elif uplift < 0.5:
                message = f"Optimization complete. Minimal uplift expected ({uplift:.1f}%)"
            else:
                message = f"Optimization successful! Expected uplift: {uplift:.1f}%"

        logger.info(f"Optimization result: uplift={uplift:.2f}%, message={message}")

        return {
            "optimal_mix": optimal_mix,
            "predicted_kpi": optimal_score,
            "baseline_kpi": baseline_score,
            "uplift": uplift,
            "message": message
        }
    except Exception as e:
        logger.error(f"Optimization error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Optimization error: {str(e)}")

def _fit_model(run_id: str, cfg: ModelConfig):
    # Load data
    dataset_info = DATASETS.get(cfg.dataset_id)
    if not dataset_info:
        RUNS[run_id] = {"status": "error", "detail": "Dataset not found"}
        return
    
    csv = dataset_info.get("path")
    df = pd.read_csv(csv, parse_dates=["date"])

    # Detect tall (campaign-level) vs wide (channel spend columns) format
    tall_cols = {"channel", "campaign", "spend"}
    is_tall = tall_cols.issubset(set(df.columns))

    import numpy as np
    from sklearn.linear_model import Ridge

    if is_tall:
        # Build feature name as channel|campaign and pivot to wide by date
        df["__feature"] = df["channel"].astype(str) + "|" + df["campaign"].astype(str)
        spend_wide = (
            df.pivot_table(index="date", columns="__feature", values="spend", aggfunc="sum", fill_value=0)
            .sort_index()
        )
        # KPI as total per date
        if cfg.kpi not in df.columns:
            RUNS[run_id] = {"status": "error", "detail": f"Column '{cfg.kpi}' missing"}
            return
        y_series = df.groupby("date")[cfg.kpi].sum().reindex(spend_wide.index).fillna(0.0)
        X = spend_wide.values
        y = y_series.values
        features = list(spend_wide.columns)
        model = Ridge(alpha=1.0).fit(X, y)
        coef = model.coef_.tolist()
        denom = float(sum(abs(c) for c in coef)) or 1.0
        kpi_mean = float(y_series.mean())

        # Means per feature
        feature_mean_spend = {f: float(spend_wide[f].mean()) for f in features}

        # Per-campaign metrics
        campaigns = []
        channel_summary_acc: Dict[str, Dict[str, float]] = {}
        for f, b in zip(features, coef):
            ch_name, camp_name = f.split("|", 1)
            mean_share = float(abs(b)) / denom
            roi_val = float(b * kpi_mean / (feature_mean_spend.get(f, 0.0) + 1e-6))
            mroas = roi_val  # placeholder for marginal ROAS
            # elasticity proxy: beta * mean_spend / kpi_mean
            elasticity = float(b) * (feature_mean_spend.get(f, 0.0)) / (kpi_mean + 1e-6)
            campaigns.append({
                "channel": ch_name,
                "campaign": camp_name,
                "feature": f,
                "beta": float(b),
                "mean_share": mean_share,
                "roi": roi_val,
                "mroas": mroas,
                "elasticity": elasticity,
                "mean_spend": feature_mean_spend.get(f, 0.0)
            })
            acc = channel_summary_acc.setdefault(ch_name, {"spend": 0.0, "roi": 0.0, "mroas": 0.0, "elasticity": 0.0})
            acc["spend"] += feature_mean_spend.get(f, 0.0)
            acc["roi"] += max(roi_val, 0.0)
            acc["mroas"] += max(mroas, 0.0)
            acc["elasticity"] += elasticity

        channel_summary = [
            {
                "channel": ch,
                "spend": vals["spend"],
                "roi": vals["roi"],
                "mroas": vals["mroas"],
                "elasticity": vals["elasticity"],
            }
            for ch, vals in channel_summary_acc.items()
        ]

        # Also compute legacy contrib/roi by channel (summing campaign metrics) for compatibility
        contrib = [
            {"channel": ch, "beta": 0.0, "mean_share": float(sum(c["mean_share"] for c in campaigns if c["channel"] == ch))}
            for ch in channel_summary_acc.keys()
        ]
        roi = [
            {"channel": ch, "roi": float(next((s["roi"] for s in channel_summary if s["channel"] == ch), 0.0))}
            for ch in channel_summary_acc.keys()
        ]

        RUNS[run_id] = {
            "status": "finished",
            "r2": float(model.score(X, y)),
            "contrib": contrib,
            "roi": roi,
            "config": RUNS[run_id]["config"],
            "kpi_mode": cfg.kpi_mode if hasattr(cfg, 'kpi_mode') else 'conversions',
            "campaigns": campaigns,
            "channel_summary": channel_summary,
        }
    else:
        # Wide format path (existing behavior)
        # Basic validation
        cols_needed = [cfg.kpi] + cfg.spend_channels
        for c in cols_needed:
            if c not in df.columns:
                RUNS[run_id] = {"status": "error", "detail": f"Column '{c}' missing"}
                return

        X = df[cfg.spend_channels].fillna(0.0).values
        y = df[cfg.kpi].fillna(0.0).values
        model = Ridge(alpha=1.0).fit(X, y)

        coef = model.coef_.tolist()
        denom = float(sum(abs(c) for c in coef)) or 1.0
        kpi_mean = float(df[cfg.kpi].mean())

        contrib = [
            {"channel": ch, "beta": float(b), "mean_share": float(abs(b))/denom}
            for ch, b in zip(cfg.spend_channels, coef)
        ]
        roi = [
            {"channel": ch, "roi": float(b * kpi_mean / (float(df[ch].mean()) + 1e-6))}
            for ch, b in zip(cfg.spend_channels, coef)
        ]

        RUNS[run_id] = {
            "status": "finished",
            "r2": float(model.score(X, y)),
            "contrib": contrib,
            "roi": roi,
            "config": RUNS[run_id]["config"],
            "kpi_mode": cfg.kpi_mode if hasattr(cfg, 'kpi_mode') else 'conversions',
        }