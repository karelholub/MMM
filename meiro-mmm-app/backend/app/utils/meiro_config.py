"""Meiro integration config: metadata, mapping, webhook stats."""
import json
import secrets
from pathlib import Path
from typing import Any, Dict, Optional

# Store in app/data/ alongside other meiro files
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
CONFIG_PATH = DATA_DIR / "meiro_config.json"
WEBHOOK_ARCHIVE_PATH = DATA_DIR / "meiro_webhook_archive.jsonl"
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


def get_webhook_archive_entries(limit: int = 100) -> list[Dict[str, Any]]:
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
    keep = max(1, min(5000, int(limit)))
    return list(reversed(rows[-keep:]))


def get_webhook_archive_status() -> Dict[str, Any]:
    if not WEBHOOK_ARCHIVE_PATH.exists():
        return {"available": False, "entries": 0, "last_received_at": None, "parser_versions": []}
    entries = 0
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
                received_at = parsed.get("received_at")
                if isinstance(received_at, str) and received_at:
                    last_received_at = received_at
                parser_version = parsed.get("parser_version")
                if isinstance(parser_version, str) and parser_version:
                    parser_versions.add(parser_version)
    except Exception:
        return {"available": False, "entries": 0, "last_received_at": None, "parser_versions": []}
    return {
        "available": entries > 0,
        "entries": entries,
        "last_received_at": last_received_at,
        "parser_versions": sorted(parser_versions),
    }


def rebuild_profiles_from_webhook_archive(limit: Optional[int] = None) -> list[Any]:
    if not WEBHOOK_ARCHIVE_PATH.exists():
        return []
    rows: list[Dict[str, Any]] = []
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
    if limit is not None:
        keep = max(1, int(limit))
        rows = rows[-keep:]

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
    return _load().get("mapping", {})


def save_mapping(mapping: Dict[str, Any]) -> None:
    d = _load()
    d["mapping"] = mapping
    _save(d)


def get_pull_config() -> Dict[str, Any]:
    return _load().get("pull_config", {
        "lookback_days": 30,
        "session_gap_minutes": 30,
        "conversion_selector": "purchase",
        "output_mode": "single",  # single | per_conversion
        "dedup_interval_minutes": 5,
    })


def save_pull_config(pull_config: Dict[str, Any]) -> None:
    d = _load()
    d["pull_config"] = {**get_pull_config(), **pull_config}
    _save(d)
