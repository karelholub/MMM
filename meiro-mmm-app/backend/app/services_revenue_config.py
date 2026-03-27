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
    "default_value": 0.0,
    "default_value_mode": "missing_only",
    "per_conversion_overrides": [],
    "base_currency": "EUR",
    "fx_enabled": False,
    "fx_mode": "none",
    "fx_rates_json": {},
    "source_type": "conversion_event",
}

_ALLOWED_DEDUP = {"conversion_id", "order_id", "event_id"}
_ALLOWED_FX_MODE = {"none", "static_rates"}
_ALLOWED_DEFAULT_VALUE_MODE = {"disabled", "missing_only", "missing_or_zero"}


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


def _parse_number(value: Any) -> Tuple[float, bool]:
    try:
        out = float(value)
        if not math.isfinite(out):
            return 0.0, False
        return out, True
    except Exception:
        return 0.0, False


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
    out["default_value"] = max(0.0, _safe_number(raw.get("default_value")))
    default_value_mode = str(raw.get("default_value_mode") or "missing_only").strip()
    out["default_value_mode"] = (
        default_value_mode
        if default_value_mode in _ALLOWED_DEFAULT_VALUE_MODE
        else "missing_only"
    )

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

    overrides_out: List[Dict[str, Any]] = []
    overrides_raw = raw.get("per_conversion_overrides")
    if isinstance(overrides_raw, list):
        for item in overrides_raw:
            if not isinstance(item, dict):
                continue
            conversion_name = str(item.get("conversion_name") or "").strip()
            if not conversion_name:
                continue
            override_dedup_key = str(item.get("dedup_key") or out["dedup_key"]).strip()
            override_mode = str(item.get("default_value_mode") or out["default_value_mode"]).strip()
            overrides_out.append(
                {
                    "conversion_name": conversion_name,
                    "value_field_path": _norm_path(
                        str(item.get("value_field_path") or out["value_field_path"]),
                        out["value_field_path"],
                    ),
                    "currency_field_path": _norm_path(
                        str(item.get("currency_field_path") or out["currency_field_path"]),
                        out["currency_field_path"],
                    ),
                    "dedup_key": (
                        override_dedup_key if override_dedup_key in _ALLOWED_DEDUP else out["dedup_key"]
                    ),
                    "default_value": max(0.0, _safe_number(item.get("default_value", out["default_value"]))),
                    "default_value_mode": (
                        override_mode
                        if override_mode in _ALLOWED_DEFAULT_VALUE_MODE
                        else out["default_value_mode"]
                    ),
                }
            )
    if overrides_out:
        out["per_conversion_overrides"] = sorted(
            overrides_out,
            key=lambda item: str(item.get("conversion_name") or "").lower(),
        )
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


def _safe_status(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"pending", "valid", "refunded", "partially_refunded", "cancelled", "invalid", "disqualified"}:
        return raw
    return "valid"


def _safe_adjustment_type(value: Any) -> str:
    raw = str(value or "").strip().lower()
    mapping = {
        "refund": "refund",
        "partial_refund": "partial_refund",
        "partially_refunded": "partial_refund",
        "cancellation": "cancellation",
        "cancelled": "cancellation",
        "returned_order": "cancellation",
        "return": "cancellation",
        "invalid_lead": "invalid_lead",
        "invalid": "invalid_lead",
        "disqualified_lead": "disqualified_lead",
        "disqualified": "disqualified_lead",
        "duplicate_lead": "invalid_lead",
        "spam_lead": "invalid_lead",
        "test_lead": "invalid_lead",
    }
    return mapping.get(raw, raw or "adjustment")


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
) -> Tuple[float, str, bool, bool, bool]:
    value = _extract_path(conversion, value_field_path)
    if value in (None, "", []):
        value = _extract_path(payload, value_field_path)
    raw_present = value not in (None, "", [])
    parsed_value, parsed_ok = _parse_number(value) if raw_present else (0.0, False)
    currency = _extract_path(conversion, currency_field_path)
    if currency is None:
        currency = _extract_path(payload, currency_field_path)
    return (
        _safe_number(parsed_value),
        str(currency or base_currency).strip().upper() or base_currency,
        raw_present,
        parsed_ok,
        raw_present and parsed_ok and _safe_number(parsed_value) == 0.0,
    )


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
    entries = extract_revenue_entries(
        payload,
        cfg,
        fallback_conversion_id=fallback_conversion_id,
    )
    total = 0.0
    for entry in entries:
        dedup_value = str(entry.get("dedup_key") or "")
        if dedupe_seen is not None and dedup_value:
            if dedup_value in dedupe_seen:
                continue
            dedupe_seen.add(dedup_value)
        total += _safe_number(entry.get("value_in_base"))
    return round(total, 6)


def extract_revenue_entries(
    payload: Optional[Dict[str, Any]],
    config: Optional[Dict[str, Any]] = None,
    *,
    fallback_conversion_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    cfg = normalize_revenue_config(config)
    overrides_by_name = {
        str(item.get("conversion_name") or "").strip().lower(): item
        for item in cfg.get("per_conversion_overrides") or []
        if isinstance(item, dict) and str(item.get("conversion_name") or "").strip()
    }
    selected = {
        str(name).strip().lower()
        for name in cfg.get("conversion_names") or []
        if str(name).strip()
    }
    selected.update(overrides_by_name.keys())
    out: List[Dict[str, Any]] = []
    for conversion in iter_payload_revenue_conversions(payload):
        name = _conversion_name(conversion, payload).lower()
        if selected and name not in selected:
            continue
        effective = dict(cfg)
        override = overrides_by_name.get(name)
        if override:
            effective.update(override)
        dedup_value = _dedup_key_value(
            conversion=conversion,
            payload=payload,
            fallback_conversion_id=fallback_conversion_id,
            dedup_key=str(effective.get("dedup_key") or "conversion_id"),
        )
        raw_value, currency, raw_present, parsed_ok, raw_zero = _value_and_currency(
            conversion,
            payload,
            value_field_path=str(effective.get("value_field_path") or "value"),
            currency_field_path=str(effective.get("currency_field_path") or "currency"),
            base_currency=str(effective.get("base_currency") or "EUR"),
        )
        default_value = max(0.0, _safe_number(effective.get("default_value")))
        default_mode = str(effective.get("default_value_mode") or "missing_only")
        default_applied = False
        default_reason = "none"
        value_for_base = raw_value
        if default_mode != "disabled":
            if (not raw_present) or (raw_present and not parsed_ok):
                value_for_base = default_value
                default_applied = True
                default_reason = "missing"
            elif raw_zero and default_mode == "missing_or_zero":
                value_for_base = default_value
                default_applied = True
                default_reason = "zero"
        gross_value_in_base = _safe_number(_to_base(value_for_base, currency, effective))
        status = _safe_status(conversion.get("status"))
        refunded_value = 0.0
        cancelled_value = 0.0
        invalidated = status in {"invalid", "disqualified"}
        adjustments = conversion.get("adjustments")
        if isinstance(adjustments, list):
            for item in adjustments:
                if not isinstance(item, dict):
                    continue
                adjustment_type = _safe_adjustment_type(item.get("type"))
                adjustment_currency = str(item.get("currency") or currency or effective.get("base_currency") or "EUR").strip().upper()
                adjustment_value = _safe_number(item.get("value"))
                adjustment_in_base = _safe_number(_to_base(adjustment_value, adjustment_currency, effective))
                if adjustment_type in {"refund", "partial_refund"}:
                    refunded_value += adjustment_in_base
                elif adjustment_type == "cancellation":
                    cancelled_value += gross_value_in_base
                    status = "cancelled"
                elif adjustment_type in {"invalid_lead", "disqualified_lead"}:
                    invalidated = True
                    status = "disqualified" if adjustment_type == "disqualified_lead" else "invalid"
        net_value_in_base = max(0.0, gross_value_in_base - refunded_value)
        if status == "cancelled":
            net_value_in_base = 0.0
        if status in {"refunded"}:
            net_value_in_base = 0.0
        if refunded_value >= gross_value_in_base > 0 and status not in {"cancelled", "invalid", "disqualified"}:
            status = "refunded"
            net_value_in_base = 0.0
        elif refunded_value > 0 and status not in {"cancelled", "invalid", "disqualified", "refunded"}:
            status = "partially_refunded"
        if invalidated:
            net_value_in_base = 0.0
        out.append(
            {
                "name": name,
                "dedup_key": dedup_value,
                "conversion_id": _conversion_id(conversion, payload, fallback_conversion_id),
                "status": status,
                "currency": currency,
                "value_raw": _safe_number(raw_value),
                "value_effective": _safe_number(value_for_base),
                "value_in_base": gross_value_in_base,
                "gross_value_in_base": gross_value_in_base,
                "net_value_in_base": net_value_in_base,
                "refunded_value": refunded_value,
                "cancelled_value": cancelled_value,
                "gross_value": _safe_number(value_for_base),
                "net_value": max(0.0, _safe_number(value_for_base) - _safe_number(refunded_value)),
                "gross_conversions": 1.0,
                "net_conversions": 0.0 if status in {"refunded", "cancelled", "invalid", "disqualified"} else 1.0,
                "invalid_leads": 1.0 if status in {"invalid", "disqualified"} else 0.0,
                "valid_leads": 0.0 if status in {"invalid", "disqualified"} else 1.0,
                "default_applied": default_applied,
                "default_reason": default_reason,
                "value_source": "defaulted" if default_applied else "raw",
                "raw_value_present": raw_present,
                "raw_value_zero": raw_zero,
            }
        )
    return out
