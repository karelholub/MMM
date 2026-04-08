"""Meiro integration config: metadata, mapping, webhook stats."""
import json
import secrets
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# Store in app/data/ alongside other meiro files
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
CONFIG_PATH = DATA_DIR / "meiro_config.json"
WEBHOOK_ARCHIVE_PATH = DATA_DIR / "meiro_webhook_archive.jsonl"
EVENT_ARCHIVE_PATH = DATA_DIR / "meiro_event_archive.jsonl"
MEIRO_CDP_PLATFORM = "meiro_cdp"


def _load() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    try:
        return json.loads(CONFIG_PATH.read_text())
    except Exception:
        return {}


def _save(data: Dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(data, indent=2))


def get_last_test_at() -> Optional[str]:
    return _load().get("last_test_at")


def set_last_test_at(iso: str) -> None:
    d = _load()
    d["last_test_at"] = iso
    _save(d)


def get_webhook_secret() -> Optional[str]:
    return _load().get("webhook_secret")


def rotate_webhook_secret() -> str:
    secret = secrets.token_urlsafe(32)
    d = _load()
    d["webhook_secret"] = secret
    _save(d)
    return secret


def get_webhook_last_received_at() -> Optional[str]:
    return _load().get("webhook_last_received_at")


def get_webhook_received_count() -> int:
    return _load().get("webhook_received_count", 0)


def set_webhook_received(count_delta: int = 1, last_received_at: Optional[str] = None) -> None:
    import datetime
    d = _load()
    d["webhook_received_count"] = d.get("webhook_received_count", 0) + count_delta
    if last_received_at:
        d["webhook_last_received_at"] = last_received_at
    else:
        d["webhook_last_received_at"] = datetime.datetime.utcnow().isoformat() + "Z"
    _save(d)


def append_webhook_event(entry: Dict[str, Any], max_items: int = 100) -> None:
    d = _load()
    events = d.get("webhook_events")
    if not isinstance(events, list):
        events = []
    events.append(entry)
    keep = max(1, min(1000, int(max_items)))
    d["webhook_events"] = events[-keep:]
    _save(d)


def get_webhook_events(limit: int = 100) -> list[Dict[str, Any]]:
    d = _load()
    events = d.get("webhook_events")
    if not isinstance(events, list):
        return []
    keep = max(1, min(1000, int(limit)))
    return list(reversed(events[-keep:]))


def append_webhook_archive_entry(entry: Dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with WEBHOOK_ARCHIVE_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def append_event_archive_entry(entry: Dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with EVENT_ARCHIVE_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def get_webhook_archive_entries(limit: int = 100) -> list[Dict[str, Any]]:
    return query_webhook_archive_entries(limit=limit)


def query_webhook_archive_entries(
    *,
    limit: Optional[int] = 100,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> list[Dict[str, Any]]:
    if not WEBHOOK_ARCHIVE_PATH.exists():
        return []
    rows: list[Dict[str, Any]] = []
    try:
        with WEBHOOK_ARCHIVE_PATH.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except Exception:
                    continue
                if isinstance(parsed, dict):
                    rows.append(parsed)
    except Exception:
        return []
    if since:
        rows = [row for row in rows if str(row.get("received_at") or "") >= since]
    if until:
        rows = [row for row in rows if str(row.get("received_at") or "") <= until]
    if limit is not None:
        keep = max(1, min(50000, int(limit)))
        rows = rows[-keep:]
    return list(reversed(rows))


def get_event_archive_entries(limit: int = 100) -> list[Dict[str, Any]]:
    return query_event_archive_entries(limit=limit)


def query_event_archive_entries(
    *,
    limit: Optional[int] = 100,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> list[Dict[str, Any]]:
    if not EVENT_ARCHIVE_PATH.exists():
        return []
    if limit is not None and since is None and until is None:
        keep = max(1, min(50000, int(limit)))
        try:
            tail = deque(maxlen=keep)
            with EVENT_ARCHIVE_PATH.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if line:
                        tail.append(line)
            rows: list[Dict[str, Any]] = []
            for line in reversed(tail):
                try:
                    parsed = json.loads(line)
                except Exception:
                    continue
                if isinstance(parsed, dict):
                    rows.append(parsed)
            return rows
        except Exception:
            return []
    rows: list[Dict[str, Any]] = []
    try:
        with EVENT_ARCHIVE_PATH.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except Exception:
                    continue
                if isinstance(parsed, dict):
                    rows.append(parsed)
    except Exception:
        return []
    if since:
        rows = [row for row in rows if str(row.get("received_at") or "") >= since]
    if until:
        rows = [row for row in rows if str(row.get("received_at") or "") <= until]
    if limit is not None:
        keep = max(1, min(50000, int(limit)))
        rows = rows[-keep:]
    return list(reversed(rows))


def get_webhook_archive_status() -> Dict[str, Any]:
    if not WEBHOOK_ARCHIVE_PATH.exists():
        return {"available": False, "entries": 0, "profiles_received": 0, "last_received_at": None, "parser_versions": []}
    entries = 0
    profiles_received = 0
    last_received_at: Optional[str] = None
    parser_versions: set[str] = set()
    try:
        with WEBHOOK_ARCHIVE_PATH.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except Exception:
                    continue
                if not isinstance(parsed, dict):
                    continue
                entries += 1
                try:
                    profiles_received += int(parsed.get("received_count") or len(parsed.get("profiles") or []))
                except Exception:
                    pass
                received_at = parsed.get("received_at")
                if isinstance(received_at, str) and received_at:
                    last_received_at = received_at
                parser_version = parsed.get("parser_version")
                if isinstance(parser_version, str) and parser_version:
                    parser_versions.add(parser_version)
    except Exception:
        return {"available": False, "entries": 0, "profiles_received": 0, "last_received_at": None, "parser_versions": []}
    return {
        "available": entries > 0,
        "entries": entries,
        "profiles_received": profiles_received,
        "last_received_at": last_received_at,
        "parser_versions": sorted(parser_versions),
    }


def get_event_archive_status() -> Dict[str, Any]:
    if not EVENT_ARCHIVE_PATH.exists():
        return {"available": False, "entries": 0, "events_received": 0, "last_received_at": None, "parser_versions": []}
    entries = 0
    events_received = 0
    last_received_at: Optional[str] = None
    parser_versions: set[str] = set()
    try:
        with EVENT_ARCHIVE_PATH.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except Exception:
                    continue
                if not isinstance(parsed, dict):
                    continue
                entries += 1
                try:
                    events_received += int(parsed.get("received_count") or len(parsed.get("events") or []))
                except Exception:
                    pass
                received_at = parsed.get("received_at")
                if isinstance(received_at, str) and received_at:
                    last_received_at = received_at
                parser_version = parsed.get("parser_version")
                if isinstance(parser_version, str) and parser_version:
                    parser_versions.add(parser_version)
    except Exception:
        return {"available": False, "entries": 0, "events_received": 0, "last_received_at": None, "parser_versions": []}
    return {
        "available": entries > 0,
        "entries": entries,
        "events_received": events_received,
        "last_received_at": last_received_at,
        "parser_versions": sorted(parser_versions),
    }


def rebuild_profiles_from_webhook_archive(
    limit: Optional[int] = None,
    *,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> list[Any]:
    rows = list(reversed(query_webhook_archive_entries(limit=limit, since=since, until=until)))
    rebuilt: list[Any] = []
    for row in rows:
        profiles = row.get("profiles")
        if not isinstance(profiles, list):
            continue
        replace = bool(row.get("replace", True))
        if replace:
            rebuilt = list(profiles)
        else:
            rebuilt.extend(list(profiles))
    return rebuilt


def get_mapping() -> Dict[str, Any]:
    raw = _load().get("mapping", {})
    if isinstance(raw, dict) and isinstance(raw.get("config"), dict):
        return raw.get("config", {})
    return raw if isinstance(raw, dict) else {}


def save_mapping(mapping: Dict[str, Any]) -> None:
    d = _load()
    existing = d.get("mapping", {})
    history = []
    approval = {"status": "approved", "note": None, "updated_at": None}
    version = 0
    if isinstance(existing, dict) and isinstance(existing.get("config"), dict):
        history = list(existing.get("history") or [])
        approval_raw = existing.get("approval")
        if isinstance(approval_raw, dict):
            approval.update({
                "status": approval_raw.get("status") or approval["status"],
                "note": approval_raw.get("note"),
                "updated_at": approval_raw.get("updated_at"),
            })
        try:
            version = int(existing.get("version") or 0)
        except Exception:
            version = 0
        previous = existing.get("config")
    else:
        previous = existing if isinstance(existing, dict) else {}
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if previous != mapping:
        history.append(
            {
                "at": now_iso,
                "action": "mapping_saved",
                "mapping": mapping,
            }
        )
        history = history[-50:]
        version += 1
    approval["status"] = "approved"
    approval["updated_at"] = now_iso
    d["mapping"] = {
        "config": mapping,
        "approval": approval,
        "history": history,
        "version": max(version, 1),
    }
    _save(d)


def get_mapping_state() -> Dict[str, Any]:
    raw = _load().get("mapping", {})
    if isinstance(raw, dict) and isinstance(raw.get("config"), dict):
        return {
            "mapping": raw.get("config", {}),
            "approval": raw.get("approval") or {"status": "approved", "note": None, "updated_at": None},
            "history": raw.get("history") or [],
            "version": raw.get("version") or 1,
        }
    if isinstance(raw, dict):
        return {
            "mapping": raw,
            "approval": {"status": "approved", "note": None, "updated_at": None},
            "history": [],
            "version": 1 if raw else 0,
        }
    return {
        "mapping": {},
        "approval": {"status": "unreviewed", "note": None, "updated_at": None},
        "history": [],
        "version": 0,
    }


def update_mapping_approval(status: str, note: Optional[str] = None) -> Dict[str, Any]:
    d = _load()
    state = get_mapping_state()
    normalized_status = status.strip().lower() if isinstance(status, str) else ""
    if normalized_status not in {"approved", "rejected", "unreviewed"}:
        normalized_status = "unreviewed"
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    history = list(state.get("history") or [])
    history.append(
        {
            "at": now_iso,
            "action": "approval_updated",
            "status": normalized_status,
            "note": note,
        }
    )
    d["mapping"] = {
        "config": state.get("mapping", {}),
        "approval": {
            "status": normalized_status,
            "note": note,
            "updated_at": now_iso,
        },
        "history": history[-50:],
        "version": state.get("version") or 0,
    }
    _save(d)
    return get_mapping_state()


def get_auto_replay_state() -> Dict[str, Any]:
    raw = _load().get("auto_replay_state", {})
    if not isinstance(raw, dict):
        raw = {}
    last_event_batch_db_id_seen_raw = raw.get("last_event_batch_db_id_seen")
    try:
        last_event_batch_db_id_seen = int(last_event_batch_db_id_seen_raw) if last_event_batch_db_id_seen_raw is not None else None
    except Exception:
        last_event_batch_db_id_seen = None
    return {
        "last_attempted_at": raw.get("last_attempted_at"),
        "last_completed_at": raw.get("last_completed_at"),
        "last_status": raw.get("last_status"),
        "last_reason": raw.get("last_reason"),
        "last_trigger": raw.get("last_trigger"),
        "last_archive_entries_seen": int(raw.get("last_archive_entries_seen") or 0),
        "last_archive_received_at": raw.get("last_archive_received_at"),
        "last_event_batch_db_id_seen": last_event_batch_db_id_seen,
        "last_result_summary": raw.get("last_result_summary") if isinstance(raw.get("last_result_summary"), dict) else {},
    }


def update_auto_replay_state(patch: Dict[str, Any]) -> Dict[str, Any]:
    current = get_auto_replay_state()
    merged = {**current, **(patch if isinstance(patch, dict) else {})}
    d = _load()
    d["auto_replay_state"] = merged
    _save(d)
    return get_auto_replay_state()


def append_auto_replay_history(entry: Dict[str, Any], max_items: int = 100) -> list[Dict[str, Any]]:
    d = _load()
    history = d.get("auto_replay_history")
    if not isinstance(history, list):
        history = []
    history.append(entry)
    keep = max(1, min(1000, int(max_items)))
    d["auto_replay_history"] = history[-keep:]
    _save(d)
    return list(reversed(d["auto_replay_history"]))


def get_auto_replay_history(limit: int = 25) -> list[Dict[str, Any]]:
    d = _load()
    history = d.get("auto_replay_history")
    if not isinstance(history, list):
        return []
    keep = max(1, min(1000, int(limit)))
    return list(reversed(history[-keep:]))


def _normalize_pull_config(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raw = {}
    allowed_dedup_modes = {"strict", "balanced", "aggressive"}
    allowed_dedup_keys = {"auto", "conversion_id", "order_id", "event_id"}
    allowed_value_fallback = {"default", "zero", "quarantine"}
    allowed_currency_fallback = {"default", "quarantine"}
    allowed_timestamp_fallback = {"profile", "conversion", "quarantine"}
    allowed_replay_modes = {"all", "last_n", "date_range"}
    allowed_replay_sources = {"auto", "profiles", "events"}
    allowed_primary_sources = {"profiles", "events"}
    allowed_auto_replay_modes = {"disabled", "interval", "after_batch"}

    def _as_int(value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(value)
        except Exception:
            parsed = default
        return max(minimum, min(maximum, parsed))

    def _as_bool(value: Any, default: bool) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "yes", "on"}:
                return True
            if lowered in {"0", "false", "no", "off"}:
                return False
        return default

    dedup_mode = str(raw.get("dedup_mode") or "balanced").strip().lower()
    if dedup_mode not in allowed_dedup_modes:
        dedup_mode = "balanced"

    primary_dedup_key = str(raw.get("primary_dedup_key") or "auto").strip().lower()
    if primary_dedup_key not in allowed_dedup_keys:
        primary_dedup_key = "auto"

    fallback_raw = raw.get("fallback_dedup_keys")
    fallback_keys: list[str] = []
    if isinstance(fallback_raw, list):
        source_values = fallback_raw
    elif isinstance(fallback_raw, str):
        source_values = [part.strip() for part in fallback_raw.split(",")]
    else:
        source_values = []
    for item in source_values:
        key = str(item or "").strip().lower()
        if key in {"conversion_id", "order_id", "event_id"} and key != primary_dedup_key and key not in fallback_keys:
            fallback_keys.append(key)
    if not fallback_keys:
        fallback_keys = ["conversion_id", "order_id", "event_id"]

    event_aliases_raw = raw.get("conversion_event_aliases")
    event_aliases: dict[str, str] = {}
    if isinstance(event_aliases_raw, dict):
        for key, value in event_aliases_raw.items():
            src = str(key or "").strip().lower()
            dst = str(value or "").strip().lower()
            if src and dst:
                event_aliases[src] = dst

    interaction_aliases_raw = raw.get("touchpoint_interaction_aliases")
    interaction_aliases: dict[str, str] = {}
    if isinstance(interaction_aliases_raw, dict):
        for key, value in interaction_aliases_raw.items():
            src = str(key or "").strip().lower()
            dst = str(value or "").strip().lower()
            if src and dst:
                interaction_aliases[src] = dst

    adjustment_aliases_raw = raw.get("adjustment_event_aliases")
    adjustment_aliases: dict[str, str] = {}
    if isinstance(adjustment_aliases_raw, dict):
        for key, value in adjustment_aliases_raw.items():
            src = str(key or "").strip().lower()
            dst = str(value or "").strip().lower()
            if src and dst:
                adjustment_aliases[src] = dst

    linkage_raw = raw.get("adjustment_linkage_keys")
    linkage_keys: list[str] = []
    if isinstance(linkage_raw, list):
        source_linkage = linkage_raw
    elif isinstance(linkage_raw, str):
        source_linkage = [part.strip() for part in linkage_raw.split(",")]
    else:
        source_linkage = []
    for item in source_linkage:
        key = str(item or "").strip().lower()
        if key in {"conversion_id", "order_id", "lead_id", "event_id"} and key not in linkage_keys:
            linkage_keys.append(key)
    if not linkage_keys:
        linkage_keys = ["conversion_id", "order_id", "lead_id", "event_id"]

    value_fallback_policy = str(raw.get("value_fallback_policy") or "default").strip().lower()
    if value_fallback_policy not in allowed_value_fallback:
        value_fallback_policy = "default"

    currency_fallback_policy = str(raw.get("currency_fallback_policy") or "default").strip().lower()
    if currency_fallback_policy not in allowed_currency_fallback:
        currency_fallback_policy = "default"

    timestamp_fallback_policy = str(raw.get("timestamp_fallback_policy") or "profile").strip().lower()
    if timestamp_fallback_policy not in allowed_timestamp_fallback:
        timestamp_fallback_policy = "profile"

    replay_mode = str(raw.get("replay_mode") or "last_n").strip().lower()
    if replay_mode not in allowed_replay_modes:
        replay_mode = "last_n"
    primary_ingest_source = str(raw.get("primary_ingest_source") or "profiles").strip().lower()
    if primary_ingest_source not in allowed_primary_sources:
        primary_ingest_source = "profiles"
    replay_archive_source = str(raw.get("replay_archive_source") or "auto").strip().lower()
    if replay_archive_source not in allowed_replay_sources:
        replay_archive_source = "auto"
    auto_replay_mode = str(raw.get("auto_replay_mode") or "disabled").strip().lower()
    if auto_replay_mode not in allowed_auto_replay_modes:
        auto_replay_mode = "disabled"

    return {
        "lookback_days": _as_int(raw.get("lookback_days"), 30, 1, 365),
        "session_gap_minutes": _as_int(raw.get("session_gap_minutes"), 30, 1, 720),
        "conversion_selector": str(raw.get("conversion_selector") or "purchase").strip() or "purchase",
        "output_mode": "single",  # single | per_conversion
        "dedup_interval_minutes": _as_int(raw.get("dedup_interval_minutes"), 5, 0, 1440),
        "dedup_mode": dedup_mode,
        "primary_dedup_key": primary_dedup_key,
        "fallback_dedup_keys": fallback_keys,
        "strict_ingest": _as_bool(raw.get("strict_ingest"), True),
        "quarantine_unknown_channels": _as_bool(raw.get("quarantine_unknown_channels"), True),
        "quarantine_missing_utm": _as_bool(raw.get("quarantine_missing_utm"), False),
        "quarantine_duplicate_profiles": _as_bool(raw.get("quarantine_duplicate_profiles"), True),
        "timestamp_fallback_policy": timestamp_fallback_policy,
        "value_fallback_policy": value_fallback_policy,
        "currency_fallback_policy": currency_fallback_policy,
        "replay_mode": replay_mode,
        "primary_ingest_source": primary_ingest_source,
        "replay_archive_source": replay_archive_source,
        "replay_archive_limit": _as_int(raw.get("replay_archive_limit"), 5000, 1, 50000),
        "replay_date_from": str(raw.get("replay_date_from") or "").strip() or None,
        "replay_date_to": str(raw.get("replay_date_to") or "").strip() or None,
        "auto_replay_mode": auto_replay_mode,
        "auto_replay_interval_minutes": _as_int(raw.get("auto_replay_interval_minutes"), 15, 1, 1440),
        "auto_replay_require_mapping_approval": _as_bool(raw.get("auto_replay_require_mapping_approval"), True),
        "auto_replay_quarantine_spike_threshold_pct": _as_int(raw.get("auto_replay_quarantine_spike_threshold_pct"), 40, 0, 100),
        "conversion_event_aliases": event_aliases,
        "touchpoint_interaction_aliases": interaction_aliases,
        "adjustment_event_aliases": adjustment_aliases,
        "adjustment_linkage_keys": linkage_keys,
    }


def get_pull_config() -> Dict[str, Any]:
    return _normalize_pull_config(_load().get("pull_config", {}))


def save_pull_config(pull_config: Dict[str, Any]) -> None:
    d = _load()
    current = get_pull_config()
    merged = {**current, **(pull_config if isinstance(pull_config, dict) else {})}
    d["pull_config"] = _normalize_pull_config(merged)
    _save(d)
