"""Official Meiro CDP API client helpers.

This module is intentionally environment-variable driven. It never stores or
returns the Meiro password or access token.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from app.utils.meiro_config import get_target_instance_url, instance_scope, instance_scope_is_strict, require_target_instance


TOKEN_TTL_SECONDS = 55 * 60
_TOKEN_CACHE: Dict[str, Any] = {"token": None, "expires_at": 0.0, "last_login_at": None}
_TOKEN_LOCK = threading.Lock()

CAMPAIGN_ENDPOINTS = {
    "email": "/api/emails",
    "push": "/api/push_notifications",
    "whatsapp": "/api/whatsapp_campaigns",
}
CAMPAIGN_TRASH_ENDPOINTS = {
    "email": "/api/emails/trash",
    "push": "/api/push_notifications/trash",
    "whatsapp": "/api/whatsapp_campaigns/trash",
}
REPORTS_API_PREFIX = "/reports/api"
LARGE_RAW_FIELD_NAMES = {
    "html",
    "body",
    "content",
    "template",
    "template_html",
    "source_html",
    "rendered_html",
    "amp_html",
    "plain_text",
    "text_body",
}


@dataclass(frozen=True)
class MeiroApiConfig:
    domain: str
    username: str
    password: str
    timeout_ms: int


class MeiroApiError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int = 502,
        code: str = "upstream_error",
        upstream_status: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.code = code
        self.upstream_status = upstream_status

    def to_response(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"ok": False, "error": self.code, "message": self.message}
        if self.upstream_status is not None:
            payload["upstream_status"] = self.upstream_status
        return payload


def _env(name: str) -> str:
    return str(os.getenv(name) or "").strip()


def get_config() -> MeiroApiConfig:
    domain = _env("MEIRO_DOMAIN").rstrip("/")
    username = _env("MEIRO_USERNAME")
    password = _env("MEIRO_PASSWORD")
    timeout_raw = _env("MEIRO_TIMEOUT_MS") or "15000"
    try:
        timeout_ms = max(1000, min(120000, int(timeout_raw)))
    except ValueError:
        timeout_ms = 15000
    missing = [
        name
        for name, value in (
            ("MEIRO_DOMAIN", domain),
            ("MEIRO_USERNAME", username),
            ("MEIRO_PASSWORD", password),
        )
        if not value
    ]
    if missing:
        raise MeiroApiError(
            f"Meiro API is not configured. Missing environment variables: {', '.join(missing)}.",
            status_code=503,
            code="configuration_error",
        )
    try:
        require_target_instance(domain)
    except ValueError as exc:
        raise MeiroApiError(str(exc), status_code=409, code="out_of_scope_instance") from exc
    return MeiroApiConfig(domain=domain, username=username, password=password, timeout_ms=timeout_ms)


def get_safe_status() -> Dict[str, Any]:
    domain = _env("MEIRO_DOMAIN").rstrip("/") or None
    username = _env("MEIRO_USERNAME") or None
    has_password = bool(_env("MEIRO_PASSWORD"))
    timeout_raw = _env("MEIRO_TIMEOUT_MS") or "15000"
    try:
        timeout_ms = int(timeout_raw)
    except ValueError:
        timeout_ms = 15000
    scope = instance_scope(domain)
    in_scope = scope.get("status") in {"not_configured", "in_scope"} or not instance_scope_is_strict()
    return {
        "configured": bool(domain and username and has_password and in_scope),
        "domain": domain,
        "username": username,
        "has_password": has_password,
        "target_instance_url": get_target_instance_url(),
        "instance_scope": scope,
        "strict_instance_scope": instance_scope_is_strict(),
        "timeout_ms": timeout_ms,
        "token_cached": bool(_TOKEN_CACHE.get("token") and float(_TOKEN_CACHE.get("expires_at") or 0) > time.time()),
        "last_login_at": _TOKEN_CACHE.get("last_login_at"),
    }


def _redact(value: str, config: Optional[MeiroApiConfig] = None, token: Optional[str] = None) -> str:
    redacted = value
    candidates: List[str] = []
    if config:
        candidates.extend([config.username, config.password])
    if token:
        candidates.append(token)
    cached = _TOKEN_CACHE.get("token")
    if cached:
        candidates.append(str(cached))
    for candidate in candidates:
        if candidate:
            redacted = redacted.replace(str(candidate), "[redacted]")
    return redacted


def _extract_token(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    for key in ("token", "access_token", "accessToken", "jwt"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    for nested_key in ("data", "user", "result"):
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            token = _extract_token(nested)
            if token:
                return token
    return None


def clear_token_cache() -> None:
    with _TOKEN_LOCK:
        _TOKEN_CACHE.update({"token": None, "expires_at": 0.0, "last_login_at": None})


def login(*, force: bool = False) -> str:
    config = get_config()
    now = time.time()
    with _TOKEN_LOCK:
        token = _TOKEN_CACHE.get("token")
        if not force and token and float(_TOKEN_CACHE.get("expires_at") or 0) > now:
            return str(token)

    url = f"{config.domain}/api/users/login"
    try:
        response = requests.post(
            url,
            json={"email": config.username, "password": config.password},
            timeout=config.timeout_ms / 1000,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
    except requests.Timeout as exc:
        raise MeiroApiError("Meiro login timed out.", code="upstream_timeout") from exc
    except requests.RequestException as exc:
        raise MeiroApiError(_redact(f"Meiro login failed: {exc}", config), code="upstream_error") from exc
    if response.status_code in {401, 403}:
        raise MeiroApiError("Meiro login failed: invalid credentials.", status_code=401, code="authentication_error")
    if response.status_code >= 400:
        raise MeiroApiError(
            f"Meiro login failed with HTTP {response.status_code}.",
            status_code=502,
            code="upstream_error",
            upstream_status=response.status_code,
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise MeiroApiError("Meiro login returned a non-JSON response.", code="upstream_error") from exc
    token = _extract_token(payload)
    if not token:
        raise MeiroApiError("Meiro login response did not include an access token.", code="upstream_error")
    with _TOKEN_LOCK:
        _TOKEN_CACHE.update(
            {
                "token": token,
                "expires_at": time.time() + TOKEN_TTL_SECONDS,
                "last_login_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
        )
    return token


def check_login() -> Dict[str, Any]:
    token = login(force=True)
    status = get_safe_status()
    status.update({"ok": True, "token_cached": bool(token)})
    return status


def _headers(token: str) -> Dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Access-Token": token,
        "Authorization": f"Bearer {token}",
    }


def _request(method: str, path: str, *, params: Optional[Dict[str, Any]] = None) -> Any:
    config = get_config()
    token = login()
    url = f"{config.domain}{path}"
    try:
        response = requests.request(
            method,
            url,
            headers=_headers(token),
            params={k: v for k, v in (params or {}).items() if v not in (None, "")},
            timeout=config.timeout_ms / 1000,
        )
    except requests.Timeout as exc:
        raise MeiroApiError("Meiro API request timed out.", code="upstream_timeout") from exc
    except requests.RequestException as exc:
        raise MeiroApiError(_redact(f"Meiro API request failed: {exc}", config, token), code="upstream_error") from exc
    if response.status_code in {401, 403}:
        clear_token_cache()
        raise MeiroApiError("Meiro API authentication failed.", status_code=401, code="authentication_error")
    if response.status_code == 400:
        raise MeiroApiError(
            "Meiro rejected the request. For WBS lookup, confirm the attribute is allowed by this Meiro instance.",
            status_code=400,
            code="validation_error",
            upstream_status=400,
        )
    if response.status_code >= 400:
        raise MeiroApiError(
            f"Meiro API returned HTTP {response.status_code}.",
            status_code=502,
            code="upstream_error",
            upstream_status=response.status_code,
        )
    if response.status_code == 204:
        return None
    try:
        return response.json()
    except ValueError:
        return response.text


def _reports_session() -> Tuple[MeiroApiConfig, requests.Session]:
    config = get_config()
    token = login()
    session = requests.Session()
    try:
        response = session.post(
            f"{config.domain}/api/users/metabase_login",
            headers=_headers(token),
            timeout=config.timeout_ms / 1000,
        )
    except requests.Timeout as exc:
        raise MeiroApiError("Meiro reporting login timed out.", code="upstream_timeout") from exc
    except requests.RequestException as exc:
        raise MeiroApiError(
            _redact(f"Meiro reporting login failed: {exc}", config, token),
            code="upstream_error",
        ) from exc
    if response.status_code in {401, 403}:
        clear_token_cache()
        raise MeiroApiError("Meiro reporting login failed.", status_code=401, code="authentication_error")
    if response.status_code >= 400:
        raise MeiroApiError(
            f"Meiro reporting login returned HTTP {response.status_code}.",
            status_code=502,
            code="upstream_error",
            upstream_status=response.status_code,
        )
    return config, session


def _reports_request(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
) -> Any:
    config, session = _reports_session()
    url = f"{config.domain}{REPORTS_API_PREFIX}{path}"
    try:
        response = session.request(
            method,
            url,
            params={k: v for k, v in (params or {}).items() if v not in (None, "")},
            json=json_body,
            timeout=config.timeout_ms / 1000,
        )
    except requests.Timeout as exc:
        raise MeiroApiError("Meiro reporting request timed out.", code="upstream_timeout") from exc
    except requests.RequestException as exc:
        raise MeiroApiError(
            _redact(f"Meiro reporting request failed: {exc}", config),
            code="upstream_error",
        ) from exc
    if response.status_code in {401, 403}:
        clear_token_cache()
        raise MeiroApiError("Meiro reporting authentication failed.", status_code=401, code="authentication_error")
    redirected_to = str(getattr(response, "url", "") or "")
    headers = getattr(response, "headers", {}) or {}
    content_type = str(headers.get("content-type") or "").lower()
    if redirected_to.endswith("/not-authorized") or (
        "html" in content_type and str(getattr(response, "text", "") or "").lstrip().startswith("<!DOCTYPE html>")
    ):
        raise MeiroApiError(
            "Meiro reporting access is not enabled for the configured user.",
            status_code=403,
            code="authorization_error",
            upstream_status=302 if getattr(response, "history", None) else response.status_code,
        )
    if response.status_code == 404:
        raise MeiroApiError(
            "Requested Meiro reporting asset was not found.",
            status_code=404,
            code="not_found",
            upstream_status=404,
        )
    if response.status_code >= 400:
        raise MeiroApiError(
            f"Meiro reporting API returned HTTP {response.status_code}.",
            status_code=502,
            code="upstream_error",
            upstream_status=response.status_code,
        )
    if response.status_code == 204:
        return None
    try:
        return response.json()
    except ValueError:
        return response.text


def _first(raw: Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = raw.get(key)
        if value not in (None, "", []):
            return value
    return None


def _extract_items(payload: Any) -> List[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("data", "items", "results", "campaigns", "records"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested_items = _extract_items(value)
            if nested_items:
                return nested_items
    return []


def _reporting_collection_name(raw: Dict[str, Any]) -> Optional[str]:
    collection = raw.get("collection")
    if isinstance(collection, dict):
        name = collection.get("name")
        return str(name) if name not in (None, "") else None
    return None


def _normalize_reporting_item(raw: Any) -> Dict[str, Any]:
    item = raw if isinstance(raw, dict) else {"value": raw}
    return {
        "id": _first(item, ("id", "entity_id")),
        "model": item.get("model"),
        "name": item.get("name"),
        "collection": _reporting_collection_name(item),
        "archived": bool(item.get("archived")),
        "updatedAt": _first(item, ("updated_at", "last_edited_at", "created_at")),
        "raw": _compact_raw(item),
    }


def _compact_raw(value: Any, *, depth: int = 0) -> Any:
    if depth > 5:
        return "[omitted: nested]"
    if isinstance(value, dict):
        compact: Dict[str, Any] = {}
        for key, item in value.items():
            key_str = str(key)
            lowered = key_str.lower()
            if lowered in LARGE_RAW_FIELD_NAMES or any(marker in lowered for marker in ("html", "content", "template")):
                compact[key_str] = "[omitted: large content field]"
                continue
            compact[key_str] = _compact_raw(item, depth=depth + 1)
        return compact
    if isinstance(value, list):
        return [_compact_raw(item, depth=depth + 1) for item in value[:100]]
    if isinstance(value, str) and len(value) > 1000:
        return f"[omitted: {len(value)} characters]"
    return value


def _segment_ids_from(value: Any) -> List[str]:
    ids: List[str] = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                candidate = _first(item, ("id", "segment_id", "uuid", "name"))
            else:
                candidate = item
            if candidate not in (None, ""):
                ids.append(str(candidate))
    elif isinstance(value, dict):
        candidate = _first(value, ("id", "segment_id", "uuid", "name"))
        if candidate not in (None, ""):
            ids.append(str(candidate))
    elif value not in (None, ""):
        ids.append(str(value))
    return list(dict.fromkeys(ids))


def _extract_segment_ids(raw: Dict[str, Any]) -> List[str]:
    ids: List[str] = []
    for key in ("segment_ids", "segmentIds", "segments", "audiences", "audience_ids", "audienceIds"):
        ids.extend(_segment_ids_from(raw.get(key)))
    for schedule in _extract_schedules(raw):
        if not isinstance(schedule, dict):
            continue
        for key in ("segment_ids", "segmentIds", "segments", "audiences", "audience_ids", "audienceIds"):
            ids.extend(_segment_ids_from(schedule.get(key)))
    return list(dict.fromkeys(ids))


def _extract_schedules(raw: Dict[str, Any]) -> List[Any]:
    for key in ("schedules", "schedule", "schedule_windows", "scheduleWindows", "activations"):
        value = raw.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            return [value]
    return []


def normalize_campaign(raw: Any, *, channel: str, deleted: bool = False, compact_raw: bool = True) -> Dict[str, Any]:
    item = raw if isinstance(raw, dict) else {"value": raw}
    is_deleted = bool(deleted or item.get("deleted") or item.get("is_deleted") or item.get("archived"))
    normalized = {
        "id": str(_first(item, ("id", "uuid", "_id", "campaign_id", "campaignId")) or ""),
        "channel": channel,
        "name": str(_first(item, ("name", "title", "subject", "label")) or "Untitled campaign"),
        "deleted": is_deleted,
        "modifiedAt": _first(item, ("modifiedAt", "modified_at", "modified", "updated_at", "updated", "last_modified_at")),
        "lastActivationAt": _first(item, ("lastActivationAt", "last_activation_at", "last_activated_at", "last_activation")),
        "schedules": _extract_schedules(item),
        "segmentIds": _extract_segment_ids(item),
        "frequencyCap": _first(item, ("frequency_cap", "frequencyCap", "frequency_capping", "frequencyCapping")),
        "raw": _compact_raw(item) if compact_raw else item,
    }
    return normalized


def _filter_campaigns(
    items: List[Dict[str, Any]],
    *,
    q: Optional[str],
    limit: Optional[int],
    offset: int,
) -> Tuple[List[Dict[str, Any]], int]:
    filtered = items
    if q:
        needle = q.lower()
        filtered = [
            item
            for item in filtered
            if needle in str(item.get("name") or "").lower() or needle in str(item.get("id") or "").lower()
        ]
    total = len(filtered)
    start = max(0, int(offset or 0))
    end = None if limit is None else start + max(0, int(limit))
    return filtered[start:end], total


def list_native_campaigns(
    *,
    channel: Optional[str] = None,
    limit: Optional[int] = 100,
    offset: int = 0,
    q: Optional[str] = None,
    include_deleted: bool = False,
) -> Dict[str, Any]:
    channels = [channel] if channel else list(CAMPAIGN_ENDPOINTS.keys())
    invalid = [item for item in channels if item not in CAMPAIGN_ENDPOINTS]
    if invalid:
        raise MeiroApiError(f"Unsupported Meiro campaign channel: {invalid[0]}.", status_code=400, code="validation_error")
    campaigns: List[Dict[str, Any]] = []
    for current_channel in channels:
        active_payload = _request("GET", CAMPAIGN_ENDPOINTS[current_channel])
        campaigns.extend(
            normalize_campaign(item, channel=current_channel, deleted=False, compact_raw=True)
            for item in _extract_items(active_payload)
        )
        if include_deleted:
            trash_payload = _request("GET", CAMPAIGN_TRASH_ENDPOINTS[current_channel])
            campaigns.extend(
                normalize_campaign(item, channel=current_channel, deleted=True, compact_raw=True)
                for item in _extract_items(trash_payload)
            )
    sliced, total = _filter_campaigns(campaigns, q=q, limit=limit, offset=offset)
    return {"items": sliced, "total": total, "limit": limit, "offset": offset}


def get_native_campaign(channel: str, campaign_id: str) -> Dict[str, Any]:
    if channel not in CAMPAIGN_ENDPOINTS:
        raise MeiroApiError(f"Unsupported Meiro campaign channel: {channel}.", status_code=400, code="validation_error")
    payload = _request("GET", f"{CAMPAIGN_ENDPOINTS[channel]}/{campaign_id}")
    return normalize_campaign(payload, channel=channel, deleted=False, compact_raw=False)


def lookup_wbs_profile(*, attribute: str, value: str, category_id: Optional[str] = None) -> Dict[str, Any]:
    attribute = str(attribute or "").strip()
    value = str(value or "").strip()
    if not attribute or not value:
        raise MeiroApiError("Both attribute and value are required for WBS profile lookup.", status_code=400, code="validation_error")
    payload = _request("GET", "/wbs", params={"attribute": attribute, "value": value, "category_id": category_id})
    raw = payload if isinstance(payload, dict) else {"data": payload}
    data = raw.get("data") if isinstance(raw.get("data"), dict) else raw
    returned = (
        raw.get("returnedAttributes")
        or raw.get("returned_attributes")
        or raw.get("attributes")
        or (data.get("attributes") if isinstance(data, dict) else None)
        or {}
    )
    customer_id = (
        raw.get("customerEntityId")
        or raw.get("customer_entity_id")
        or raw.get("customer_id")
        or (data.get("customerEntityId") if isinstance(data, dict) else None)
        or (data.get("customer_entity_id") if isinstance(data, dict) else None)
        or (data.get("id") if isinstance(data, dict) else None)
    )
    return {
        "status": raw.get("status") or "ok",
        "customerEntityId": customer_id,
        "returnedAttributes": returned,
        "data": data,
        "raw": raw,
    }


def lookup_wbs_segments(*, attribute: str, value: str) -> Dict[str, Any]:
    attribute = str(attribute or "").strip()
    value = str(value or "").strip()
    if not attribute or not value:
        raise MeiroApiError("Both attribute and value are required for WBS segment lookup.", status_code=400, code="validation_error")
    payload = _request("GET", "/wbs/segments", params={"attribute": attribute, "value": value})
    raw = payload if isinstance(payload, dict) else {"data": payload}
    segment_ids = _segment_ids_from(raw.get("segmentIds") or raw.get("segment_ids") or raw.get("segments") or raw.get("data"))
    return {
        "status": raw.get("status") or "ok",
        "segmentIds": segment_ids,
        "raw": raw,
    }


def search_reporting_assets(*, q: str, model: Optional[str] = None, limit: int = 20) -> Dict[str, Any]:
    query = str(q or "").strip()
    if not query:
        raise MeiroApiError("Search query is required for Meiro reporting search.", status_code=400, code="validation_error")
    payload = _reports_request("GET", "/search", params={"q": query})
    items = [_normalize_reporting_item(item) for item in _extract_items(payload)]
    if model:
        items = [item for item in items if str(item.get("model") or "").lower() == str(model).lower()]
    capped_limit = max(1, min(int(limit or 20), 100))
    return {"items": items[:capped_limit], "total": len(items), "limit": capped_limit, "query": query, "model": model}


def get_reporting_dashboard(dashboard_id: int) -> Dict[str, Any]:
    payload = _reports_request("GET", f"/dashboard/{int(dashboard_id)}")
    raw = payload if isinstance(payload, dict) else {"value": payload}
    dashcards = raw.get("dashcards") if isinstance(raw.get("dashcards"), list) else []
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "collection": _reporting_collection_name(raw),
        "parameters": [
            {"name": item.get("name"), "slug": item.get("slug"), "type": item.get("type")}
            for item in (raw.get("parameters") or [])
            if isinstance(item, dict)
        ],
        "cards": [
            {"cardId": (item.get("card") or {}).get("id"), "name": (item.get("card") or {}).get("name")}
            for item in dashcards
            if isinstance(item, dict)
        ],
        "raw": _compact_raw(raw),
    }


def get_reporting_card(card_id: int) -> Dict[str, Any]:
    payload = _reports_request("GET", f"/card/{int(card_id)}")
    raw = payload if isinstance(payload, dict) else {"value": payload}
    dataset_query = raw.get("dataset_query") if isinstance(raw.get("dataset_query"), dict) else {}
    native = dataset_query.get("native") if isinstance(dataset_query.get("native"), dict) else {}
    template_tags = native.get("template-tags") if isinstance(native.get("template-tags"), dict) else {}
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "collection": _reporting_collection_name(raw),
        "display": raw.get("display"),
        "databaseId": dataset_query.get("database"),
        "datasetType": dataset_query.get("type"),
        "templateTags": list(template_tags.keys()),
        "resultColumns": [item.get("name") for item in (raw.get("result_metadata") or []) if isinstance(item, dict)],
        "raw": _compact_raw(raw),
    }


def query_reporting_card_json(card_id: int, *, parameters: Optional[List[Dict[str, Any]]] = None, limit: int = 100) -> Dict[str, Any]:
    payload = _reports_request(
        "POST",
        f"/card/{int(card_id)}/query/json",
        json_body={"parameters": parameters or []},
    )
    rows = payload if isinstance(payload, list) else _extract_items(payload)
    capped_limit = max(1, min(int(limit or 100), 1000))
    return {
        "cardId": int(card_id),
        "rows": rows[:capped_limit],
        "rowCount": len(rows),
        "limit": capped_limit,
    }
