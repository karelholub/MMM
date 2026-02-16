from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
import json
import math


_SETTINGS_PATH = Path(__file__).resolve().parent / "data" / "settings.json"

_DEFAULT_CONFIG: Dict[str, Any] = {
    "conversion_names": ["purchase"],
    "value_field_path": "value",
    "currency_field_path": "currency",
    "dedup_key": "conversion_id",
    "base_currency": "EUR",
    "fx_enabled": False,
    "fx_mode": "none",
    "fx_rates_json": {},
    "source_type": "conversion_event",
}

_ALLOWED_DEDUP = {"conversion_id", "order_id", "event_id"}
_ALLOWED_FX_MODE = {"none", "static_rates"}


def default_revenue_config() -> Dict[str, Any]:
    return dict(_DEFAULT_CONFIG)


def _safe_number(value: Any) -> float:
    try:
        out = float(value)
        if not math.isfinite(out):
            return 0.0
        return out
    except Exception:
        return 0.0


def _norm_path(path: str, fallback: str) -> str:
    raw = (path or fallback).strip()
    if raw.startswith("conversion."):
        return raw[len("conversion.") :]
    return raw or fallback


def _extract_path(record: Dict[str, Any], path: str, fallback: Any = None) -> Any:
    if not path:
        return fallback
    current: Any = record
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            current = None
        if current is None:
            return fallback
    return current


def normalize_revenue_config(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = default_revenue_config()
    if not isinstance(raw, dict):
        return out

    names = raw.get("conversion_names")
    if isinstance(names, list):
        cleaned = sorted(
            {
                str(name).strip()
                for name in names
                if str(name).strip()
            }
        )
        out["conversion_names"] = cleaned or ["purchase"]

    out["value_field_path"] = _norm_path(str(raw.get("value_field_path") or "value"), "value")
    out["currency_field_path"] = _norm_path(str(raw.get("currency_field_path") or "currency"), "currency")

    dedup_key = str(raw.get("dedup_key") or "conversion_id").strip()
    out["dedup_key"] = dedup_key if dedup_key in _ALLOWED_DEDUP else "conversion_id"

    base_currency = str(raw.get("base_currency") or "EUR").strip().upper()
    out["base_currency"] = base_currency or "EUR"

    out["fx_enabled"] = bool(raw.get("fx_enabled", False))
    fx_mode = str(raw.get("fx_mode") or "none").strip()
    out["fx_mode"] = fx_mode if fx_mode in _ALLOWED_FX_MODE else "none"

    rates = raw.get("fx_rates_json")
    if isinstance(rates, dict):
        cleaned_rates: Dict[str, float] = {}
        for currency, rate in rates.items():
            code = str(currency or "").strip().upper()
            if not code:
                continue
            value = _safe_number(rate)
            if value > 0:
                cleaned_rates[code] = value
        out["fx_rates_json"] = cleaned_rates

    source_type = str(raw.get("source_type") or "conversion_event").strip()
    out["source_type"] = source_type or "conversion_event"
    return out


def get_revenue_config_from_settings(settings_obj: Any) -> Dict[str, Any]:
    if isinstance(settings_obj, dict):
        return normalize_revenue_config(settings_obj.get("revenue_config"))
    raw = getattr(settings_obj, "revenue_config", None)
    if raw is not None and hasattr(raw, "model_dump"):
        raw = raw.model_dump()
    return normalize_revenue_config(raw)


def get_revenue_config() -> Dict[str, Any]:
    try:
        raw = json.loads(_SETTINGS_PATH.read_text(encoding="utf-8"))
    except Exception:
        raw = {}
    return get_revenue_config_from_settings(raw)


def _conversion_name(conversion: Dict[str, Any], payload: Dict[str, Any]) -> str:
    return str(
        conversion.get("name")
        or conversion.get("event_name")
        or payload.get("kpi_type")
        or payload.get("conversion_key")
        or "purchase"
    ).strip()


def _conversion_id(conversion: Dict[str, Any], payload: Dict[str, Any], fallback: Optional[str]) -> str:
    return str(
        conversion.get("id")
        or conversion.get("conversion_id")
        or payload.get("conversion_id")
        or payload.get("journey_id")
        or fallback
        or ""
    ).strip()


def _dedup_key_value(
    *,
    conversion: Dict[str, Any],
    payload: Dict[str, Any],
    fallback_conversion_id: Optional[str],
    dedup_key: str,
) -> str:
    conversion_id = _conversion_id(conversion, payload, fallback_conversion_id)
    if dedup_key == "order_id":
        return str(
            conversion.get("order_id")
            or conversion.get("orderId")
            or payload.get("order_id")
            or conversion_id
        ).strip()
    if dedup_key == "event_id":
        return str(
            conversion.get("event_id")
            or conversion.get("eventId")
            or payload.get("event_id")
            or conversion_id
        ).strip()
    return conversion_id


def _value_and_currency(
    conversion: Dict[str, Any],
    payload: Dict[str, Any],
    *,
    value_field_path: str,
    currency_field_path: str,
    base_currency: str,
) -> Tuple[float, str]:
    value = _extract_path(conversion, value_field_path)
    if value is None:
        value = _extract_path(payload, value_field_path)
    currency = _extract_path(conversion, currency_field_path)
    if currency is None:
        currency = _extract_path(payload, currency_field_path)
    return _safe_number(value), str(currency or base_currency).strip().upper() or base_currency


def _to_base(value: float, currency: str, config: Dict[str, Any]) -> float:
    base = str(config.get("base_currency") or "EUR").upper()
    if not config.get("fx_enabled"):
        return value
    if str(config.get("fx_mode") or "none") != "static_rates":
        return value
    if currency == base:
        return _safe_number(value)
    rates = config.get("fx_rates_json") or {}
    rate = _safe_number(rates.get(currency))
    if rate > 0:
        return _safe_number(value) * rate
    return _safe_number(value)


def iter_payload_revenue_conversions(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    convs = payload.get("conversions")
    if isinstance(convs, list) and convs:
        for item in convs:
            if isinstance(item, dict):
                yield item
        return
    yield payload


def compute_payload_revenue_value(
    payload: Optional[Dict[str, Any]],
    config: Optional[Dict[str, Any]] = None,
    *,
    dedupe_seen: Optional[Set[str]] = None,
    fallback_conversion_id: Optional[str] = None,
) -> float:
    if not isinstance(payload, dict):
        return 0.0
    cfg = normalize_revenue_config(config)
    selected = {str(name).strip().lower() for name in cfg.get("conversion_names") or []}
    total = 0.0
    for conversion in iter_payload_revenue_conversions(payload):
        name = _conversion_name(conversion, payload).lower()
        if selected and name not in selected:
            continue
        dedup_value = _dedup_key_value(
            conversion=conversion,
            payload=payload,
            fallback_conversion_id=fallback_conversion_id,
            dedup_key=str(cfg.get("dedup_key") or "conversion_id"),
        )
        if dedupe_seen is not None and dedup_value:
            if dedup_value in dedupe_seen:
                continue
            dedupe_seen.add(dedup_value)
        raw_value, currency = _value_and_currency(
            conversion,
            payload,
            value_field_path=str(cfg.get("value_field_path") or "value"),
            currency_field_path=str(cfg.get("currency_field_path") or "currency"),
            base_currency=str(cfg.get("base_currency") or "EUR"),
        )
        total += _to_base(raw_value, currency, cfg)
    return round(total, 6)
