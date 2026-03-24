import json
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import pandas as pd
import requests
from fastapi import HTTPException


def connectors_status(*, data_dir: Path) -> Dict[str, Any]:
    sources = [
        (data_dir / "meta_ads.csv", "Meta"),
        (data_dir / "google_ads.csv", "Google"),
        (data_dir / "linkedin_ads.csv", "LinkedIn"),
        (data_dir / "meiro_cdp.csv", "Meiro CDP"),
        (data_dir / "unified_ads.csv", "Unified"),
    ]
    stats: Dict[str, Any] = {}
    for path, name in sources:
        if path.exists():
            try:
                df = pd.read_csv(path)
                stats[name] = {
                    "path": str(path),
                    "rows": int(len(df)),
                    "total_spend": float(df.get("spend", pd.Series([], dtype=float)).fillna(0).sum()),
                }
            except Exception:
                stats[name] = {"path": str(path), "rows": 0}
        else:
            stats[name] = {"path": str(path), "rows": 0}
    return stats


def fetch_meta(
    *,
    ad_account_id: str,
    since: str,
    until: str,
    avg_aov: float = 0.0,
    access_token: Optional[str] = None,
    get_token_fn: Callable[[str], Optional[Dict[str, Any]]],
    session_local_factory: Callable[[], Any],
    get_access_token_for_provider_fn: Callable[..., Optional[str]],
    data_dir: Path,
    expenses_obj: Dict[str, Any],
    expense_entry_cls: type,
    with_converted_amount_fn: Callable[[Any], Any],
    default_reporting_currency_fn: Callable[[], str],
    now_iso_fn: Callable[[], str],
    import_sync_state_obj: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    if not access_token:
        token_data = get_token_fn("meta")
        if not token_data or not token_data.get("access_token"):
            db = session_local_factory()
            try:
                token_from_conn = get_access_token_for_provider_fn(db, workspace_id="default", provider_key="meta_ads")
            finally:
                db.close()
            if not token_from_conn:
                raise HTTPException(status_code=401, detail="No Meta access token. Connect your Meta account first.")
            access_token = token_from_conn
        else:
            access_token = token_data["access_token"]
    out_path = data_dir / "meta_ads.csv"
    url = f"https://graph.facebook.com/v19.0/{ad_account_id}/ads"
    params = {
        "access_token": access_token,
        "fields": "campaign_name,spend,impressions,clicks,actions,updated_time",
        "time_range": json.dumps({"since": since, "until": until}),
        "limit": 500,
    }
    rows = []
    try:
        while True:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            for ad in data.get("data", []):
                actions = ad.get("actions", []) or []
                purchases = next((a for a in actions if a.get("action_type") == "purchase"), None)
                conv = float(purchases.get("value", 0)) if purchases else 0.0
                rows.append(
                    {
                        "date": since,
                        "channel": "meta_ads",
                        "campaign": ad.get("campaign_name", "unknown"),
                        "spend": float(ad.get("spend", 0) or 0),
                        "impressions": int(ad.get("impressions", 0) or 0),
                        "clicks": int(ad.get("clicks", 0) or 0),
                        "conversions": float(conv),
                        "revenue": float(conv * float(avg_aov)),
                    }
                )
            next_url = data.get("paging", {}).get("next")
            if not next_url:
                break
            url = next_url
            params = {}
    except Exception:
        pass
    pd.DataFrame(rows).to_csv(out_path, index=False)
    total_spend = sum(row.get("spend", 0) for row in rows)
    if total_spend > 0:
        expense_id = f"meta_ads_{since[:7]}"
        expenses_obj[expense_id] = with_converted_amount_fn(
            expense_entry_cls(
                channel="meta_ads",
                cost_type="Media Spend",
                amount=total_spend,
                currency="USD",
                reporting_currency=default_reporting_currency_fn(),
                period=since[:7],
                service_period_start=since,
                service_period_end=until,
                notes="Auto-imported from Meta Ads API",
                source_type="import",
                source_name="meta_ads",
                actor_type="import",
                created_at=now_iso_fn(),
            )
        )
    now_iso = now_iso_fn()
    import_sync_state_obj["meta_ads"] = {
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


def fetch_google(
    *,
    segments_date_from: str,
    segments_date_to: str,
    data_dir: Path,
    now_iso_fn: Callable[[], str],
    import_sync_state_obj: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    out_path = data_dir / "google_ads.csv"
    if not out_path.exists():
        pd.DataFrame([], columns=["date", "channel", "campaign", "spend", "impressions", "clicks", "conversions", "revenue"]).to_csv(out_path, index=False)
    rows = int(pd.read_csv(out_path).shape[0])
    now_iso = now_iso_fn()
    import_sync_state_obj["google_ads"] = {
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


def fetch_linkedin(
    *,
    since: str,
    until: str,
    access_token: Optional[str] = None,
    get_token_fn: Callable[[str], Optional[Dict[str, Any]]],
    session_local_factory: Callable[[], Any],
    get_access_token_for_provider_fn: Callable[..., Optional[str]],
    data_dir: Path,
    expenses_obj: Dict[str, Any],
    expense_entry_cls: type,
    with_converted_amount_fn: Callable[[Any], Any],
    default_reporting_currency_fn: Callable[[], str],
    now_iso_fn: Callable[[], str],
    import_sync_state_obj: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    if not access_token:
        token_data = get_token_fn("linkedin")
        if not token_data or not token_data.get("access_token"):
            db = session_local_factory()
            try:
                token_from_conn = get_access_token_for_provider_fn(db, workspace_id="default", provider_key="linkedin_ads")
            finally:
                db.close()
            if not token_from_conn:
                raise HTTPException(status_code=401, detail="No LinkedIn access token. Connect your LinkedIn account first.")
            access_token = token_from_conn
        else:
            access_token = token_data["access_token"]
    out_path = data_dir / "linkedin_ads.csv"
    headers = {"Authorization": f"Bearer {access_token}"}
    rows = []
    try:
        response = requests.get(
            "https://api.linkedin.com/v2/adAnalyticsV2",
            headers=headers,
            params={"q": "analytics", "pivot": "CAMPAIGN", "timeGranularity": "DAILY"},
            timeout=30,
        )
        if response.ok:
            for element in response.json().get("elements", []):
                rows.append(
                    {
                        "date": since,
                        "channel": "linkedin_ads",
                        "campaign": (element.get("campaign", {}) or {}).get("name", "unknown"),
                        "spend": float(element.get("costInLocalCurrency", 0) or 0),
                        "impressions": int(element.get("impressions", 0) or 0),
                        "clicks": int(element.get("clicks", 0) or 0),
                        "conversions": float(element.get("conversions", 0) or 0),
                        "revenue": float(element.get("revenueValue", 0) or 0),
                    }
                )
    except Exception:
        pass
    pd.DataFrame(rows).to_csv(out_path, index=False)
    total_spend = sum(row.get("spend", 0) for row in rows)
    if total_spend > 0:
        expense_id = f"linkedin_ads_{since[:7]}"
        expenses_obj[expense_id] = with_converted_amount_fn(
            expense_entry_cls(
                channel="linkedin_ads",
                cost_type="Media Spend",
                amount=total_spend,
                currency="USD",
                reporting_currency=default_reporting_currency_fn(),
                period=since[:7],
                service_period_start=since,
                service_period_end=until,
                notes="Auto-imported from LinkedIn Ads API",
                source_type="import",
                source_name="linkedin_ads",
                actor_type="import",
                created_at=now_iso_fn(),
            )
        )
    now_iso = now_iso_fn()
    import_sync_state_obj["linkedin_ads"] = {
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


def merge_ads(*, data_dir: Path) -> Dict[str, Any]:
    sources = [data_dir / "meta_ads.csv", data_dir / "google_ads.csv", data_dir / "linkedin_ads.csv", data_dir / "meiro_cdp.csv"]
    frames = [pd.read_csv(path) for path in sources if path.exists()]
    unified = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame([], columns=["date", "channel", "campaign", "spend", "impressions", "clicks", "conversions", "revenue"])
    for col in ["spend", "impressions", "clicks", "conversions", "revenue"]:
        unified[col] = pd.to_numeric(unified.get(col, 0), errors="coerce").fillna(0)
    unified["date"] = pd.to_datetime(unified.get("date", pd.to_datetime([])), errors="coerce").dt.date.astype(str)
    unified.drop_duplicates(subset=["date", "channel", "campaign"], keep="last", inplace=True)
    out_path = data_dir / "unified_ads.csv"
    unified.to_csv(out_path, index=False)
    return {"rows": int(len(unified)), "path": str(out_path)}
