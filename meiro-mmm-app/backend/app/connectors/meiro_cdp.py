"""
Meiro CDP connector.

Fetches customer, event, and attribution data from a Meiro CDP instance
and transforms it into an MMM-compatible weekly aggregated CSV.

Meiro CDP REST API reference:
  - GET  /api/v1/customers          – list / search customers
  - GET  /api/attributes         – list available attributes
  - GET  /api/v1/events             – list event types
  - POST /api/v1/exports            – create a data export
  - GET  /api/v1/exports/{id}       – poll export status & download

The connector stores the API base URL and API key in the encrypted
token store (same mechanism as OAuth tokens for Meta/Google/LinkedIn).
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from app.utils.token_store import save_token, get_token, delete_token
from app.utils.meiro_config import set_last_test_at, get_last_test_at

logger = logging.getLogger(__name__)

PLATFORM_KEY = "meiro_cdp"


# ---------------------------------------------------------------------------
# Configuration persistence
# ---------------------------------------------------------------------------

def save_config(api_base_url: str, api_key: str) -> None:
    """Store Meiro CDP credentials in the encrypted token store."""
    save_token(PLATFORM_KEY, {
        "access_token": api_key,
        "api_base_url": api_base_url,
    })


def get_config() -> Optional[Dict[str, str]]:
    """Retrieve stored Meiro CDP credentials."""
    data = get_token(PLATFORM_KEY)
    if not data or not data.get("access_token"):
        return None
    return {
        "api_key": data["access_token"],
        "api_base_url": data.get("api_base_url", ""),
    }


def disconnect() -> bool:
    """Remove stored Meiro CDP credentials."""
    return delete_token(PLATFORM_KEY)


def is_connected() -> bool:
    """Check if Meiro CDP credentials are stored."""
    cfg = get_config()
    return cfg is not None and bool(cfg.get("api_key")) and bool(cfg.get("api_base_url"))


def get_connection_metadata() -> Optional[Dict[str, Any]]:
    """
    Return safe connection metadata for UI. Never includes API key.
    """
    cfg = get_config()
    if not cfg:
        return None
    base = cfg.get("api_base_url", "")
    return {
        "api_base_url": base,
        "has_key": bool(cfg.get("api_key")),
        "last_test_at": get_last_test_at(),
    }


def update_last_test_at() -> None:
    """Record that connection test succeeded."""
    set_last_test_at(datetime.utcnow().isoformat() + "Z")


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _headers(api_key: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _get(url: str, api_key: str, params: Optional[Dict] = None, timeout: int = 30) -> Any:
    r = requests.get(url, headers=_headers(api_key), params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _post(url: str, api_key: str, body: Dict, timeout: int = 30) -> Any:
    r = requests.post(url, headers=_headers(api_key), json=body, timeout=timeout)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Public API functions
# ---------------------------------------------------------------------------

def test_connection(api_base_url: str, api_key: str) -> Dict[str, Any]:
    """
    Test connectivity to the Meiro CDP instance.
    Returns {"ok": True, "version": "..."} on success.
    """
    try:
        data = _get(f"{api_base_url.rstrip('/')}/api/v1/attributes", api_key, params={"limit": 1})
        return {"ok": True, "message": "Connection successful"}
    except requests.RequestException as exc:
        return {"ok": False, "message": str(exc)}


def list_attributes(api_base_url: Optional[str] = None, api_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """List customer attributes available in the CDP."""
    if not api_base_url or not api_key:
        cfg = get_config()
        if not cfg:
            raise ValueError("Meiro CDP not configured")
        api_base_url = cfg["api_base_url"]
        api_key = cfg["api_key"]

    data = _get(f"{api_base_url.rstrip('/')}/api/v1/attributes", api_key)
    return data.get("data", data) if isinstance(data, dict) else data


def list_events(api_base_url: Optional[str] = None, api_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """List event types tracked in the CDP."""
    if not api_base_url or not api_key:
        cfg = get_config()
        if not cfg:
            raise ValueError("Meiro CDP not configured")
        api_base_url = cfg["api_base_url"]
        api_key = cfg["api_key"]

    data = _get(f"{api_base_url.rstrip('/')}/api/v1/events", api_key)
    return data.get("data", data) if isinstance(data, dict) else data


def list_segments(api_base_url: Optional[str] = None, api_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """List customer segments defined in the CDP."""
    if not api_base_url or not api_key:
        cfg = get_config()
        if not cfg:
            raise ValueError("Meiro CDP not configured")
        api_base_url = cfg["api_base_url"]
        api_key = cfg["api_key"]

    data = _get(f"{api_base_url.rstrip('/')}/api/v1/segments", api_key)
    return data.get("data", data) if isinstance(data, dict) else data


def create_export(
    since: str,
    until: str,
    event_types: Optional[List[str]] = None,
    attributes: Optional[List[str]] = None,
    segment_id: Optional[str] = None,
    api_base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a data export in Meiro CDP and return the export metadata.

    Parameters
    ----------
    since, until : date strings (YYYY-MM-DD)
    event_types : list of event type names to include
    attributes : list of customer attribute names to include
    segment_id : optional segment filter
    """
    if not api_base_url or not api_key:
        cfg = get_config()
        if not cfg:
            raise ValueError("Meiro CDP not configured")
        api_base_url = cfg["api_base_url"]
        api_key = cfg["api_key"]

    body: Dict[str, Any] = {
        "date_from": since,
        "date_to": until,
        "format": "json",
    }
    if event_types:
        body["event_types"] = event_types
    if attributes:
        body["attributes"] = attributes
    if segment_id:
        body["segment_id"] = segment_id

    return _post(f"{api_base_url.rstrip('/')}/api/v1/exports", api_key, body)


def poll_export(
    export_id: str,
    api_base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    max_wait: int = 300,
    poll_interval: int = 5,
) -> Dict[str, Any]:
    """
    Poll an export until it completes or times out.

    Returns the export data payload on success.
    """
    if not api_base_url or not api_key:
        cfg = get_config()
        if not cfg:
            raise ValueError("Meiro CDP not configured")
        api_base_url = cfg["api_base_url"]
        api_key = cfg["api_key"]

    deadline = time.time() + max_wait
    while time.time() < deadline:
        data = _get(
            f"{api_base_url.rstrip('/')}/api/v1/exports/{export_id}",
            api_key,
        )
        status = data.get("status", "")
        if status == "completed":
            return data
        if status in ("failed", "error"):
            raise RuntimeError(f"Export failed: {data.get('message', status)}")
        time.sleep(poll_interval)

    raise TimeoutError(f"Export {export_id} did not complete within {max_wait}s")


def fetch_and_transform(
    since: str,
    until: str,
    event_types: Optional[List[str]] = None,
    attributes: Optional[List[str]] = None,
    segment_id: Optional[str] = None,
    api_base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    End-to-end: create export → poll → transform into MMM-ready DataFrame.

    The resulting DataFrame has weekly rows with columns:
    date, channel, campaign, spend, impressions, clicks, conversions, revenue
    """
    if not api_base_url or not api_key:
        cfg = get_config()
        if not cfg:
            raise ValueError("Meiro CDP not configured")
        api_base_url = cfg["api_base_url"]
        api_key = cfg["api_key"]

    export_meta = create_export(
        since=since,
        until=until,
        event_types=event_types,
        attributes=attributes,
        segment_id=segment_id,
        api_base_url=api_base_url,
        api_key=api_key,
    )
    export_id = export_meta.get("id") or export_meta.get("export_id", "")
    if not export_id:
        raise ValueError("No export ID returned from Meiro CDP")

    result = poll_export(export_id, api_base_url=api_base_url, api_key=api_key)

    # The export payload is a list of event / customer records.
    # Transform into MMM-compatible weekly aggregation.
    records = result.get("data", [])
    if not records:
        return pd.DataFrame(
            columns=["date", "channel", "campaign", "spend", "impressions", "clicks", "conversions", "revenue"]
        )

    df = pd.DataFrame(records)
    return _aggregate_to_weekly(df)


def fetch_raw_events(
    since: str,
    until: str,
    event_types: Optional[List[str]] = None,
    attributes: Optional[List[str]] = None,
    segment_id: Optional[str] = None,
    api_base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch raw event/customer records from Meiro export (for journey building)."""
    export_meta = create_export(
        since=since,
        until=until,
        event_types=event_types,
        attributes=attributes,
        segment_id=segment_id,
        api_base_url=api_base_url,
        api_key=api_key,
    )
    export_id = export_meta.get("id") or export_meta.get("export_id", "")
    if not export_id:
        raise ValueError("No export ID returned from Meiro CDP")
    result = poll_export(export_id, api_base_url=api_base_url, api_key=api_key)
    return result.get("data", [])


def build_journeys_from_events(
    records: List[Dict[str, Any]],
    *,
    id_attr: str = "customer_id",
    timestamp_attr: str = "timestamp",
    channel_attr: str = "channel",
    conversion_selector: str = "purchase",
    session_gap_minutes: int = 30,
    dedup_interval_minutes: int = 5,
    channel_mapping: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Build journey list from flat event records.
    Groups by customer, sessionizes, dedupes, identifies conversions.
    """
    channel_mapping = channel_mapping or {}
    ts_col = timestamp_attr
    ch_col = channel_attr
    id_col = id_attr

    # Infer columns
    for r in records[:10] if records else []:
        for k in ["date", "event_date", "created_at"]:
            if k in r and ts_col not in r:
                ts_col = k
                break
        for k in ["source", "utm_source", "traffic_source"]:
            if k in r and ch_col not in r:
                ch_col = k
                break

    journeys = []
    from collections import defaultdict
    by_customer = defaultdict(list)
    for r in records:
        cid = r.get(id_col, r.get("id", "unknown"))
        ts = r.get(ts_col, r.get("date", ""))
        ch = r.get(ch_col, r.get("source", "unknown"))
        ch = channel_mapping.get(str(ch).lower(), ch) if ch else "unknown"
        by_customer[cid].append({"ts": ts, "channel": ch, "raw": r})

    for cid, events in by_customer.items():
        events = sorted(events, key=lambda x: x["ts"])
        touchpoints = []
        last_ts = None
        gap_sec = session_gap_minutes * 60
        dedup_sec = dedup_interval_minutes * 60

        for e in events:
            ts = e["ts"]
            ch = e["channel"]
            try:
                t = pd.Timestamp(ts)
                ts_val = t.timestamp()
            except Exception:
                ts_val = 0
            if last_ts and (ts_val - last_ts) < dedup_sec and touchpoints and touchpoints[-1].get("channel") == ch:
                continue
            last_ts = ts_val
            touchpoints.append({"channel": ch, "timestamp": ts})

        is_conversion = any(
            str(e.get("event_type", "")).lower() == conversion_selector or
            str(e.get("event_name", "")).lower() == conversion_selector
            for e in [x["raw"] for x in events]
        )
        conv_val = 0.0
        for e in [x["raw"] for x in events]:
            v = e.get("conversion_value", e.get("value", 0))
            try:
                conv_val = max(conv_val, float(v or 0))
            except (TypeError, ValueError):
                pass

        journeys.append({
            "customer_id": cid,
            "touchpoints": touchpoints,
            "conversion_value": conv_val if is_conversion else 0,
            "converted": is_conversion,
        })
    return journeys


def _aggregate_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate raw CDP event/attribution records into weekly granularity.

    Expects at minimum columns like: timestamp/date, source/channel,
    campaign, and metric columns. Adapts to whatever columns are present.
    """
    # Normalise date column
    date_col = None
    for candidate in ["date", "timestamp", "event_date", "created_at"]:
        if candidate in df.columns:
            date_col = candidate
            break
    if not date_col:
        # Use first column that parses as datetime
        for col in df.columns:
            try:
                pd.to_datetime(df[col].head())
                date_col = col
                break
            except Exception:
                continue

    if date_col:
        df["date"] = pd.to_datetime(df[date_col], errors="coerce")
        df["week"] = df["date"].dt.to_period("W").dt.start_time
    else:
        df["week"] = pd.Timestamp.today().normalize()

    # Normalise channel column
    channel_col = None
    for candidate in ["channel", "source", "utm_source", "traffic_source"]:
        if candidate in df.columns:
            channel_col = candidate
            break
    if channel_col and channel_col != "channel":
        df["channel"] = df[channel_col]
    elif "channel" not in df.columns:
        df["channel"] = "Meiro CDP"

    # Normalise campaign column
    campaign_col = None
    for candidate in ["campaign", "utm_campaign", "campaign_name"]:
        if candidate in df.columns:
            campaign_col = candidate
            break
    if campaign_col and campaign_col != "campaign":
        df["campaign"] = df[campaign_col]
    elif "campaign" not in df.columns:
        df["campaign"] = "unknown"

    # Ensure numeric metrics exist
    for metric in ["spend", "impressions", "clicks", "conversions", "revenue"]:
        if metric not in df.columns:
            df[metric] = 0
        df[metric] = pd.to_numeric(df[metric], errors="coerce").fillna(0)

    # Aggregate
    agg = (
        df.groupby(["week", "channel", "campaign"], as_index=False)
        .agg({
            "spend": "sum",
            "impressions": "sum",
            "clicks": "sum",
            "conversions": "sum",
            "revenue": "sum",
        })
    )
    agg.rename(columns={"week": "date"}, inplace=True)
    agg["date"] = agg["date"].dt.strftime("%Y-%m-%d")
    return agg
