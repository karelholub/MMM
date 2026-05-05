"""Meiro integration config: metadata, mapping, webhook stats."""
import json
import os
import secrets
import threading
from collections import Counter
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlparse

# Store in app/data/ alongside other meiro files
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
CONFIG_PATH = DATA_DIR / "meiro_config.json"
WEBHOOK_ARCHIVE_PATH = DATA_DIR / "meiro_webhook_archive.jsonl"
EVENT_ARCHIVE_PATH = DATA_DIR / "meiro_event_archive.jsonl"
MEIRO_CDP_PLATFORM = "meiro_cdp"
DEFAULT_TARGET_INSTANCE_URL = "https://meiro-internal.eu.pipes.meiro.io"
DEFAULT_TARGET_SITE_DOMAINS = ("meiro.io", "meir.store")
_CONFIG_LOCK = threading.RLock()
_OUT_OF_SCOPE_CAMPAIGN_CACHE: tuple[float, set[str]] | None = None


def _normalized_url(value: Any) -> str:
    raw = str(value or "").strip().rstrip("/")
    if raw and "://" not in raw:
        raw = f"https://{raw}"
    return raw


def _url_host(value: Any) -> str:
    normalized = _normalized_url(value)
    if not normalized:
        return ""
    try:
        return (urlparse(normalized).hostname or "").lower()
    except Exception:
        return ""


def _domain_token(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    if "://" in raw:
        try:
            raw = urlparse(raw).hostname or raw
        except Exception:
            pass
    raw = raw.split("/", 1)[0].split(":", 1)[0].strip(".")
    if raw.startswith("www."):
        raw = raw[4:]
    return raw


def get_target_site_domains() -> list[str]:
    raw = os.getenv("MEIRO_TARGET_SITE_DOMAINS")
    values = raw.split(",") if raw else list(DEFAULT_TARGET_SITE_DOMAINS)
    domains: list[str] = []
    seen: set[str] = set()
    for value in values:
        domain = _domain_token(value)
        if not domain or domain in seen:
            continue
        seen.add(domain)
        domains.append(domain)
    return domains or list(DEFAULT_TARGET_SITE_DOMAINS)


def site_scope_is_strict() -> bool:
    return str(os.getenv("MEIRO_STRICT_SITE_SCOPE", "1")).strip().lower() not in {"0", "false", "no", "off"}


def _event_dict(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    nested = value.get("event_payload")
    if not isinstance(nested, dict):
        return value
    merged = dict(nested)
    for key, item in value.items():
        if key != "event_payload" and key not in merged:
            merged[key] = item
    return merged


def _first_present(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", []):
            return value
    return None


def event_site_host(value: Any) -> str:
    event = _event_dict(value)
    if not event:
        return ""
    context = event.get("context") if isinstance(event.get("context"), dict) else {}
    explicit = _domain_token(_first_present(event.get("site"), event.get("hostname"), event.get("domain")))
    if explicit:
        return explicit
    for key in ("page_url", "page_location", "url", "location", "href"):
        host = _domain_token(event.get(key))
        if host:
            return host
    host = _domain_token(context.get("url"))
    return host


def site_domain_matches(host: Any, domains: Optional[list[str]] = None) -> bool:
    normalized_host = _domain_token(host)
    if not normalized_host:
        return False
    target_domains = domains if domains is not None else get_target_site_domains()
    return any(normalized_host == domain or normalized_host.endswith(f".{domain}") for domain in target_domains)


def event_site_scope(value: Any) -> Dict[str, Any]:
    host = event_site_host(value)
    target_domains = get_target_site_domains()
    if not host:
        status = "unknown"
        reason = "No page/site host is present on the event."
    elif site_domain_matches(host, target_domains):
        status = "target_site"
        reason = "Event page host matches the active Meiro Measurement site scope."
    else:
        status = "out_of_scope"
        reason = f"Event page host '{host}' is outside the active site scope."
    return {
        "status": status,
        "host": host or None,
        "target_sites": target_domains,
        "reason": reason,
    }


def event_matches_target_site_scope(value: Any, *, allow_unknown: bool = True) -> bool:
    status = event_site_scope(value).get("status")
    return status == "target_site" or (allow_unknown and status == "unknown")


def _campaign_label(value: Any) -> str:
    if isinstance(value, dict):
        value = _first_present(value.get("name"), value.get("id"))
    return str(value or "").strip()


def _event_campaign_labels(event: Any) -> list[str]:
    payload = _event_dict(event)
    labels = [
        _campaign_label(payload.get("campaign")),
        _campaign_label(payload.get("campaign_name")),
        _campaign_label(payload.get("utm_campaign")),
    ]
    return [label for label in labels if label]


def _profile_campaign_labels(profile: Any) -> list[str]:
    labels: list[str] = []
    if not isinstance(profile, dict):
        return labels
    for touchpoint in profile.get("touchpoints") or []:
        if not isinstance(touchpoint, dict):
            continue
        labels.extend(_event_campaign_labels(touchpoint))
        utm = touchpoint.get("utm") if isinstance(touchpoint.get("utm"), dict) else {}
        labels.append(_campaign_label(utm.get("campaign")))
    return [label for label in labels if label]


def _normalized_campaign_label(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def normalize_campaign_label(value: Any) -> str:
    raw = str(value or "").strip()
    if ":" in raw:
        raw = raw.split(":", 1)[1]
    return _normalized_campaign_label(raw)


def get_out_of_scope_campaign_labels() -> set[str]:
    global _OUT_OF_SCOPE_CAMPAIGN_CACHE
    now = datetime.now(timezone.utc).timestamp()
    if _OUT_OF_SCOPE_CAMPAIGN_CACHE and now - _OUT_OF_SCOPE_CAMPAIGN_CACHE[0] < 60:
        return set(_OUT_OF_SCOPE_CAMPAIGN_CACHE[1])
    labels: set[str] = set()
    for entry in query_event_archive_entries(limit=50000):
        for event in entry.get("events") or []:
            if event_site_scope(event).get("status") == "out_of_scope":
                labels.update(_normalized_campaign_label(label) for label in _event_campaign_labels(event))
    for entry in query_webhook_archive_entries(limit=50000):
        source_host = str(entry.get("source_instance_host") or "").strip().lower()
        if source_host and source_host == get_target_instance_host():
            continue
        for profile in entry.get("profiles") or []:
            labels.update(_normalized_campaign_label(label) for label in _profile_campaign_labels(profile))
    labels.discard("")
    _OUT_OF_SCOPE_CAMPAIGN_CACHE = (now, labels)
    return set(labels)


def campaign_label_matches_target_site_scope(value: Any, *, allow_unknown: bool = True) -> bool:
    normalized = normalize_campaign_label(value)
    if not normalized:
        return bool(allow_unknown)
    if not site_scope_is_strict():
        return True
    return normalized not in get_out_of_scope_campaign_labels()


def touchpoint_matches_target_site_scope(value: Any, *, allow_unknown: bool = True) -> bool:
    scope = event_site_scope(value)
    status = scope.get("status")
    if status == "out_of_scope":
        return False
    if status == "target_site":
        return True
    for label in _event_campaign_labels(value):
        if not campaign_label_matches_target_site_scope(label, allow_unknown=allow_unknown):
            return False
    return bool(allow_unknown)


def journey_matches_target_site_scope(value: Any, *, allow_unknown: bool = True) -> bool:
    if not site_scope_is_strict():
        return True
    if not isinstance(value, dict):
        return bool(allow_unknown)
    touchpoints = value.get("touchpoints") or []
    if touchpoints:
        return all(touchpoint_matches_target_site_scope(tp, allow_unknown=allow_unknown) for tp in touchpoints if isinstance(tp, dict))
    return campaign_label_matches_target_site_scope(
        _first_present(value.get("campaign"), value.get("campaign_name"), value.get("campaign_id")),
        allow_unknown=allow_unknown,
    )


def filter_journeys_to_target_site_scope(journeys: list[dict[str, Any]], *, allow_unknown: bool = True) -> list[dict[str, Any]]:
    if not site_scope_is_strict():
        return list(journeys or [])
    return [journey for journey in (journeys or []) if journey_matches_target_site_scope(journey, allow_unknown=allow_unknown)]


def expense_site_scope(value: Any) -> Dict[str, Any]:
    campaign = _campaign_label(getattr(value, "campaign", None) if not isinstance(value, dict) else value.get("campaign"))
    normalized_campaign = _normalized_campaign_label(campaign)
    labels = get_out_of_scope_campaign_labels()
    if normalized_campaign and normalized_campaign in labels:
        return {
            "status": "out_of_scope",
            "host": None,
            "campaign": campaign,
            "target_sites": get_target_site_domains(),
            "reason": "Expense campaign label was observed in out-of-scope Meiro archive traffic.",
        }
    return {
        "status": "target_site" if normalized_campaign else "unknown",
        "host": None,
        "campaign": campaign or None,
        "target_sites": get_target_site_domains(),
        "reason": (
            "Expense campaign label is not tied to known out-of-scope archive traffic."
            if normalized_campaign
            else "Expense has no campaign label to compare with site-scoped archive diagnostics."
        ),
    }


def expense_matches_target_site_scope(value: Any, *, allow_unknown: bool = True) -> bool:
    status = expense_site_scope(value).get("status")
    return status == "target_site" or (allow_unknown and status == "unknown")


def get_target_instance_url() -> str:
    return _normalized_url(
        os.getenv("MEIRO_TARGET_INSTANCE_URL")
        or os.getenv("MEIRO_PRISM_BASE_URL")
        or os.getenv("MEIRO_PIPES_BASE_URL")
        or DEFAULT_TARGET_INSTANCE_URL
    )


def get_target_instance_host() -> str:
    return _url_host(get_target_instance_url())


def instance_scope(url: Any) -> Dict[str, Any]:
    configured_url = _normalized_url(url)
    configured_host = _url_host(configured_url)
    target_url = get_target_instance_url()
    target_host = get_target_instance_host()
    if not configured_url:
        status = "not_configured"
        reason = "No Meiro connector instance is configured."
    elif configured_host == target_host:
        status = "in_scope"
        reason = "Configured instance matches the active Meiro Measurement target."
    else:
        status = "out_of_scope"
        reason = f"Configured instance host '{configured_host or configured_url}' does not match target host '{target_host}'."
    return {
        "status": status,
        "configured_url": configured_url or None,
        "configured_host": configured_host or None,
        "target_url": target_url,
        "target_host": target_host,
        "reason": reason,
    }


def instance_scope_is_strict() -> bool:
    return str(os.getenv("MEIRO_STRICT_INSTANCE_SCOPE", "1")).strip().lower() not in {"0", "false", "no", "off"}


def require_target_instance(url: Any) -> None:
    scope = instance_scope(url)
    if instance_scope_is_strict() and scope.get("status") == "out_of_scope":
        raise ValueError(
            "This workspace is scoped to "
            f"{scope.get('target_url')}. The configured Meiro instance is "
            f"{scope.get('configured_url') or 'not set'}."
        )


def archive_source_metadata() -> Dict[str, Any]:
    return {
        "source_instance_url": get_target_instance_url(),
        "source_instance_host": get_target_instance_host(),
        "source_scope_status": "target_instance",
    }


def summarize_archive_source_scope(*, entries: int, verified_entries: int, out_of_scope_entries: int = 0) -> Dict[str, Any]:
    legacy_unverified_entries = max(0, int(entries or 0) - int(verified_entries or 0) - int(out_of_scope_entries or 0))
    status = (
        "empty"
        if int(entries or 0) <= 0
        else "out_of_scope"
        if out_of_scope_entries > 0
        else "legacy_unverified"
        if legacy_unverified_entries > 0
        else "target_verified"
    )
    return {
        "target_url": get_target_instance_url(),
        "target_host": get_target_instance_host(),
        "verified_entries": int(verified_entries or 0),
        "legacy_unverified_entries": legacy_unverified_entries,
        "out_of_scope_entries": int(out_of_scope_entries or 0),
        "status": status,
    }


def _backup_path() -> Path:
    return CONFIG_PATH.with_name(f"{CONFIG_PATH.stem}.bak{CONFIG_PATH.suffix}")


def _read_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text())
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _write_json_file(path: Path, data: Dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    payload = json.dumps(data, indent=2)
    tmp_path.write_text(payload)
    tmp_path.replace(path)


def _mutate(mutator: Callable[[Dict[str, Any]], None]) -> Dict[str, Any]:
    with _CONFIG_LOCK:
        current = _read_json_file(CONFIG_PATH)
        next_data = dict(current)
        mutator(next_data)
        _write_json_file(CONFIG_PATH, next_data)
        _write_json_file(_backup_path(), next_data)
        return next_data


def _load() -> Dict[str, Any]:
    with _CONFIG_LOCK:
        current = _read_json_file(CONFIG_PATH)
        if current:
            return current
        backup = _read_json_file(_backup_path())
        if backup and not CONFIG_PATH.exists():
            _write_json_file(CONFIG_PATH, backup)
        return backup


def _save(data: Dict[str, Any]) -> None:
    with _CONFIG_LOCK:
        _write_json_file(CONFIG_PATH, data)
        _write_json_file(_backup_path(), data)


def get_last_test_at() -> Optional[str]:
    return _load().get("last_test_at")


def set_last_test_at(iso: str) -> None:
    _mutate(lambda d: d.__setitem__("last_test_at", iso))


def get_webhook_secret() -> Optional[str]:
    return _load().get("webhook_secret")


def rotate_webhook_secret() -> str:
    secret = secrets.token_urlsafe(32)
    _mutate(lambda d: d.__setitem__("webhook_secret", secret))
    return secret


def get_webhook_last_received_at() -> Optional[str]:
    return _load().get("webhook_last_received_at")


def get_webhook_received_count() -> int:
    return _load().get("webhook_received_count", 0)


def set_webhook_received(count_delta: int = 1, last_received_at: Optional[str] = None) -> None:
    import datetime
    def apply(d: Dict[str, Any]) -> None:
        d["webhook_received_count"] = d.get("webhook_received_count", 0) + count_delta
        if last_received_at:
            d["webhook_last_received_at"] = last_received_at
        else:
            d["webhook_last_received_at"] = datetime.datetime.utcnow().isoformat() + "Z"
    _mutate(apply)


def append_webhook_event(entry: Dict[str, Any], max_items: int = 100) -> None:
    def apply(d: Dict[str, Any]) -> None:
        events = d.get("webhook_events")
        if not isinstance(events, list):
            events = []
        events.append(entry)
        keep = max(1, min(1000, int(max_items)))
        d["webhook_events"] = events[-keep:]
    _mutate(apply)


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
        return {
            "available": False,
            "entries": 0,
            "profiles_received": 0,
            "last_received_at": None,
            "parser_versions": [],
            "source_scope": summarize_archive_source_scope(entries=0, verified_entries=0),
        }
    entries = 0
    profiles_received = 0
    last_received_at: Optional[str] = None
    parser_versions: set[str] = set()
    verified_entries = 0
    out_of_scope_entries = 0
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
                source_host = str(parsed.get("source_instance_host") or "").strip().lower()
                source_status = str(parsed.get("source_scope_status") or "").strip().lower()
                if source_host and source_host == get_target_instance_host():
                    verified_entries += 1
                elif source_status == "out_of_scope":
                    out_of_scope_entries += 1
    except Exception:
        return {
            "available": False,
            "entries": 0,
            "profiles_received": 0,
            "last_received_at": None,
            "parser_versions": [],
            "source_scope": summarize_archive_source_scope(entries=0, verified_entries=0),
        }
    return {
        "available": entries > 0,
        "entries": entries,
        "profiles_received": profiles_received,
        "last_received_at": last_received_at,
        "parser_versions": sorted(parser_versions),
        "source_scope": summarize_archive_source_scope(
            entries=entries,
            verified_entries=verified_entries,
            out_of_scope_entries=out_of_scope_entries,
        ),
    }


def get_event_archive_status() -> Dict[str, Any]:
    empty_site_scope = {
        "strict": site_scope_is_strict(),
        "target_sites": get_target_site_domains(),
        "target_site_events": 0,
        "out_of_scope_site_events": 0,
        "unknown_site_events": 0,
        "top_hosts": [],
    }
    if not EVENT_ARCHIVE_PATH.exists():
        return {
            "available": False,
            "entries": 0,
            "events_received": 0,
            "last_received_at": None,
            "parser_versions": [],
            "source_scope": summarize_archive_source_scope(entries=0, verified_entries=0),
            "site_scope": empty_site_scope,
        }
    entries = 0
    events_received = 0
    last_received_at: Optional[str] = None
    parser_versions: set[str] = set()
    verified_entries = 0
    out_of_scope_entries = 0
    target_site_events = 0
    out_of_scope_site_events = 0
    unknown_site_events = 0
    site_host_counts: Counter[str] = Counter()
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
                source_host = str(parsed.get("source_instance_host") or "").strip().lower()
                source_status = str(parsed.get("source_scope_status") or "").strip().lower()
                if source_host and source_host == get_target_instance_host():
                    verified_entries += 1
                elif source_status == "out_of_scope":
                    out_of_scope_entries += 1
                for event in parsed.get("events") or []:
                    scope = event_site_scope(event)
                    status = str(scope.get("status") or "")
                    host = str(scope.get("host") or "unknown")
                    site_host_counts[host] += 1
                    if status == "target_site":
                        target_site_events += 1
                    elif status == "out_of_scope":
                        out_of_scope_site_events += 1
                    else:
                        unknown_site_events += 1
    except Exception:
        return {
            "available": False,
            "entries": 0,
            "events_received": 0,
            "last_received_at": None,
            "parser_versions": [],
            "source_scope": summarize_archive_source_scope(entries=0, verified_entries=0),
            "site_scope": empty_site_scope,
        }
    return {
        "available": entries > 0,
        "entries": entries,
        "events_received": events_received,
        "last_received_at": last_received_at,
        "parser_versions": sorted(parser_versions),
        "source_scope": summarize_archive_source_scope(
            entries=entries,
            verified_entries=verified_entries,
            out_of_scope_entries=out_of_scope_entries,
        ),
        "site_scope": {
            "strict": site_scope_is_strict(),
            "target_sites": get_target_site_domains(),
            "target_site_events": target_site_events,
            "out_of_scope_site_events": out_of_scope_site_events,
            "unknown_site_events": unknown_site_events,
            "top_hosts": [
                {"host": host, "count": count}
                for host, count in site_host_counts.most_common(12)
            ],
        },
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
    def apply(d: Dict[str, Any]) -> None:
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
    _mutate(apply)


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
    def apply(d: Dict[str, Any]) -> None:
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
    _mutate(apply)
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
    _mutate(lambda d: d.__setitem__("auto_replay_state", merged))
    return get_auto_replay_state()


def append_auto_replay_history(entry: Dict[str, Any], max_items: int = 100) -> list[Dict[str, Any]]:
    final_history: list[Dict[str, Any]] = []
    def apply(d: Dict[str, Any]) -> None:
        nonlocal final_history
        history = d.get("auto_replay_history")
        if not isinstance(history, list):
            history = []
        history.append(entry)
        keep = max(1, min(1000, int(max_items)))
        d["auto_replay_history"] = history[-keep:]
        final_history = list(d["auto_replay_history"])
    _mutate(apply)
    return list(reversed(final_history))


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
    def apply(d: Dict[str, Any]) -> None:
        current = _normalize_pull_config(d.get("pull_config", {}))
        merged = {**current, **(pull_config if isinstance(pull_config, dict) else {})}
        d["pull_config"] = _normalize_pull_config(merged)
    _mutate(apply)
