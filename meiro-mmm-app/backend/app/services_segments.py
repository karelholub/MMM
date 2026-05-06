from __future__ import annotations

import json
import math
import statistics
import urllib.parse
import urllib.request
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.connectors import meiro_cdp
from app.models_config_dq import (
    ConversionScopeDiagnosticFact,
    JourneyDefinitionInstanceFact,
    JourneyRoleFact,
    LocalAnalyticalSegment,
    MeiroEventProfileState,
    MeiroProfileFact,
)


LEGACY_LOCAL_SEGMENT_KEYS = {"channel_group", "campaign_id", "device", "country"}
DIMENSION_FILTER_KEYS = {"channel_group", "campaign_id", "device", "country"}
NUMERIC_RULE_FIELDS = {
    "path_length",
    "lag_days",
    "net_revenue_total",
    "gross_revenue_total",
    "net_conversions_total",
    "gross_conversions_total",
}
SEGMENT_FIELD_SPECS: Dict[str, Dict[str, Any]] = {
    "channel_group": {"label": "Channel group", "kind": "dimension", "operators": {"eq"}},
    "campaign_id": {"label": "Campaign", "kind": "dimension", "operators": {"eq"}},
    "device": {"label": "Device", "kind": "dimension", "operators": {"eq"}},
    "country": {"label": "Country", "kind": "dimension", "operators": {"eq"}},
    "conversion_key": {"label": "Conversion KPI", "kind": "dimension", "operators": {"eq"}},
    "last_touch_channel": {"label": "Last-touch channel", "kind": "dimension", "operators": {"eq"}},
    "interaction_path_type": {"label": "Path type", "kind": "dimension", "operators": {"eq"}},
    "path_length": {"label": "Path length", "kind": "numeric", "operators": {"gte", "lte"}},
    "lag_days": {"label": "Lag (days)", "kind": "numeric", "operators": {"gte", "lte"}},
    "net_revenue_total": {"label": "Net revenue", "kind": "numeric", "operators": {"gte", "lte"}},
    "gross_revenue_total": {"label": "Gross revenue", "kind": "numeric", "operators": {"gte", "lte"}},
    "net_conversions_total": {"label": "Net conversions", "kind": "numeric", "operators": {"gte", "lte"}},
    "gross_conversions_total": {"label": "Gross conversions", "kind": "numeric", "operators": {"gte", "lte"}},
}
DATA_DIR = Path(__file__).resolve().parent / "data"
DECIENGINE_EVENTS_CONFIG_PATH = DATA_DIR / "deciengine_events_config.json"
DECIENGINE_PIPES_REGISTRY_CACHE_PATH = DATA_DIR / "deciengine_pipes_field_registry_cache.json"


def _new_id() -> str:
    return str(uuid.uuid4())


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _coerce_numeric(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _normalize_rule(raw_rule: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw_rule, dict):
        return None
    field = _clean_text(raw_rule.get("field"))
    spec = SEGMENT_FIELD_SPECS.get(field)
    if not spec:
        return None
    operator = _clean_text(raw_rule.get("op") or raw_rule.get("operator")).lower()
    if not operator:
        operator = "eq" if spec["kind"] == "dimension" else "gte"
    if operator not in spec["operators"]:
        return None
    if spec["kind"] == "dimension":
        value = _clean_text(raw_rule.get("value"))
        if not value:
            return None
        return {"field": field, "op": operator, "value": value}
    numeric_value = _coerce_numeric(raw_rule.get("value"))
    if numeric_value is None:
        return None
    return {"field": field, "op": operator, "value": numeric_value}


def normalize_local_segment_definition(definition: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    raw = definition or {}
    rules: List[Dict[str, Any]] = []
    if isinstance(raw.get("rules"), list):
        for candidate in raw.get("rules") or []:
            normalized = _normalize_rule(candidate)
            if normalized is not None:
                rules.append(normalized)
    else:
        for key in LEGACY_LOCAL_SEGMENT_KEYS:
            value = raw.get(key)
            normalized_value = _clean_text(value)
            if normalized_value:
                rules.append({"field": key, "op": "eq", "value": normalized_value})
    match = _clean_text(raw.get("match")).lower()
    if match not in {"all", "any"}:
        match = "all"
    return {
        "version": "v2",
        "match": match,
        "rules": rules,
    }


def _infer_segment_family(definition: Dict[str, Any]) -> str:
    fields = {str(rule.get("field")) for rule in definition.get("rules") or []}
    if not fields:
        return "empty"
    if fields.issubset(DIMENSION_FILTER_KEYS):
        return "dimension_slice"
    if fields & {"lag_days"}:
        return "lag_timing"
    if fields & {"path_length", "interaction_path_type"}:
        return "journey_behavior"
    if fields & {"net_revenue_total", "gross_revenue_total", "net_conversions_total", "gross_conversions_total"}:
        return "value_conversion"
    return "mixed"


def _format_rule_label(rule: Dict[str, Any]) -> str:
    spec = SEGMENT_FIELD_SPECS.get(str(rule.get("field")))
    if not spec:
        return "Unknown rule"
    field_label = spec["label"]
    operator = str(rule.get("op") or "")
    value = rule.get("value")
    if str(rule.get("field")) == "lag_days":
        suffix = "d"
    else:
        suffix = ""
    operator_label = {"eq": "=", "gte": ">=", "lte": "<="}.get(operator, operator)
    return f"{field_label} {operator_label} {value}{suffix}"


def _build_criteria_label(definition: Dict[str, Any]) -> str:
    rules = definition.get("rules") or []
    if not rules:
        return "No criteria"
    joiner = " · ANY · " if definition.get("match") == "any" else " · "
    return joiner.join(_format_rule_label(rule) for rule in rules)


def _extract_auto_filter_definition(definition: Dict[str, Any]) -> Dict[str, str]:
    extracted: Dict[str, str] = {}
    for rule in definition.get("rules") or []:
        field = str(rule.get("field") or "")
        if field in DIMENSION_FILTER_KEYS and str(rule.get("op") or "") == "eq":
            value = _clean_text(rule.get("value"))
            if value:
                extracted[field] = value
    return extracted


def _build_segment_compatibility(definition: Dict[str, Any]) -> Dict[str, Any]:
    rules = definition.get("rules") or []
    filter_keys = sorted(_extract_auto_filter_definition(definition).keys())
    auto_filter_compatible = bool(rules) and all(
        str(rule.get("field") or "") in DIMENSION_FILTER_KEYS and str(rule.get("op") or "") == "eq"
        for rule in rules
    )
    return {
        "filter_keys": filter_keys,
        "auto_filter_compatible": auto_filter_compatible,
        "advanced": any(
            str(rule.get("field") or "") not in DIMENSION_FILTER_KEYS or str(rule.get("op") or "") != "eq"
            for rule in rules
        ),
    }


def _segment_rule_expression(rule: Dict[str, Any]) -> Any:
    field = str(rule.get("field") or "")
    operator = str(rule.get("op") or "")
    value = rule.get("value")
    if field == "channel_group":
        column = JourneyDefinitionInstanceFact.channel_group
    elif field == "campaign_id":
        column = JourneyDefinitionInstanceFact.campaign_id
    elif field == "device":
        column = JourneyDefinitionInstanceFact.device
    elif field == "country":
        column = JourneyDefinitionInstanceFact.country
    elif field == "conversion_key":
        column = JourneyDefinitionInstanceFact.conversion_key
    elif field == "last_touch_channel":
        column = JourneyDefinitionInstanceFact.last_touch_channel
    elif field == "interaction_path_type":
        column = JourneyDefinitionInstanceFact.interaction_path_type
    elif field == "path_length":
        column = JourneyDefinitionInstanceFact.path_length
    elif field == "lag_days":
        column = JourneyDefinitionInstanceFact.time_to_convert_sec
        numeric_value = float(value or 0) * 86400.0
        return column >= numeric_value if operator == "gte" else column <= numeric_value
    elif field == "net_revenue_total":
        column = JourneyDefinitionInstanceFact.net_revenue_total
    elif field == "gross_revenue_total":
        column = JourneyDefinitionInstanceFact.gross_revenue_total
    elif field == "net_conversions_total":
        column = JourneyDefinitionInstanceFact.net_conversions_total
    elif field == "gross_conversions_total":
        column = JourneyDefinitionInstanceFact.gross_conversions_total
    else:
        return None
    if operator == "eq":
        return column == value
    if operator == "gte":
        return column >= value
    if operator == "lte":
        return column <= value
    return None


def _list_segment_matches(db: Session, definition: Dict[str, Any]) -> List[Tuple[Any, ...]]:
    rules = definition.get("rules") or []
    query = db.query(
        JourneyDefinitionInstanceFact.profile_id,
        JourneyDefinitionInstanceFact.path_length,
        JourneyDefinitionInstanceFact.time_to_convert_sec,
        JourneyDefinitionInstanceFact.net_conversions_total,
        JourneyDefinitionInstanceFact.net_revenue_total,
    )
    expressions = [expr for expr in (_segment_rule_expression(rule) for rule in rules) if expr is not None]
    if expressions:
        if definition.get("match") == "any":
            from sqlalchemy import or_

            query = query.filter(or_(*expressions))
        else:
            query = query.filter(*expressions)
    return list(query.all())


def _build_segment_preview(db: Session, definition: Dict[str, Any]) -> Dict[str, Any]:
    rows = _list_segment_matches(db, definition)
    total_rows = int(db.query(func.count(JourneyDefinitionInstanceFact.id)).scalar() or 0)
    profiles = {str(profile_id).strip() for profile_id, *_ in rows if str(profile_id or "").strip()}
    lag_days = [float(value) / 86400.0 for _, _, value, _, _ in rows if value not in (None, "")]
    path_lengths = [int(value) for _, value, _, _, _ in rows if value not in (None, "")]
    conversions = sum(float(value or 0) for _, _, _, value, _ in rows)
    revenue = sum(float(value or 0) for _, _, _, _, value in rows)
    return {
        "journey_rows": len(rows),
        "share_of_rows": (len(rows) / total_rows) if total_rows else 0.0,
        "profiles": len(profiles),
        "conversions": round(conversions, 2),
        "revenue": round(revenue, 2),
        "median_lag_days": round(statistics.median(lag_days), 2) if lag_days else None,
        "avg_path_length": round(sum(path_lengths) / len(path_lengths), 2) if path_lengths else None,
    }


def get_local_segment_row(
    db: Session,
    *,
    segment_id: str,
    workspace_id: str,
) -> Optional[LocalAnalyticalSegment]:
    return (
        db.query(LocalAnalyticalSegment)
        .filter(
            LocalAnalyticalSegment.id == segment_id,
            LocalAnalyticalSegment.workspace_id == workspace_id,
        )
        .first()
    )


def _build_scoped_rows_query(
    db: Session,
    *,
    definition: Dict[str, Any],
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    journey_definition_id: Optional[str] = None,
):
    query = db.query(
        JourneyDefinitionInstanceFact.conversion_id,
        JourneyDefinitionInstanceFact.profile_id,
        JourneyDefinitionInstanceFact.channel_group,
        JourneyDefinitionInstanceFact.campaign_id,
        JourneyDefinitionInstanceFact.device,
        JourneyDefinitionInstanceFact.country,
        JourneyDefinitionInstanceFact.conversion_key,
        JourneyDefinitionInstanceFact.last_touch_channel,
        JourneyDefinitionInstanceFact.interaction_path_type,
        JourneyDefinitionInstanceFact.path_length,
        JourneyDefinitionInstanceFact.time_to_convert_sec,
        JourneyDefinitionInstanceFact.net_conversions_total,
        JourneyDefinitionInstanceFact.net_revenue_total,
    )
    if date_from is not None:
        query = query.filter(JourneyDefinitionInstanceFact.date >= date_from)
    if date_to is not None:
        query = query.filter(JourneyDefinitionInstanceFact.date <= date_to)
    if journey_definition_id:
        query = query.filter(JourneyDefinitionInstanceFact.journey_definition_id == journey_definition_id)
    expressions = [expr for expr in (_segment_rule_expression(rule) for rule in definition.get("rules") or []) if expr is not None]
    if expressions:
        if definition.get("match") == "any":
            from sqlalchemy import or_

            query = query.filter(or_(*expressions))
        else:
            query = query.filter(*expressions)
    return query


def _matched_instance_ids(
    db: Session,
    *,
    definition: Dict[str, Any],
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    journey_definition_id: Optional[str] = None,
) -> List[int]:
    query = db.query(JourneyDefinitionInstanceFact.id)
    if date_from is not None:
        query = query.filter(JourneyDefinitionInstanceFact.date >= date_from)
    if date_to is not None:
        query = query.filter(JourneyDefinitionInstanceFact.date <= date_to)
    if journey_definition_id:
        query = query.filter(JourneyDefinitionInstanceFact.journey_definition_id == journey_definition_id)
    expressions = [expr for expr in (_segment_rule_expression(rule) for rule in definition.get("rules") or []) if expr is not None]
    if expressions:
        if definition.get("match") == "any":
            from sqlalchemy import or_

            query = query.filter(or_(*expressions))
        else:
            query = query.filter(*expressions)
    return [int(row_id) for (row_id,) in query.all() if row_id is not None]


def _summarize_scoped_rows(rows: List[Tuple[Any, ...]], baseline_count: int) -> Dict[str, Any]:
    profiles = {str(profile_id).strip() for _, profile_id, *_ in rows if str(profile_id or "").strip()}
    lag_days = [float(time_to_convert_sec) / 86400.0 for *_, time_to_convert_sec, _, _ in rows if time_to_convert_sec not in (None, "")]
    path_lengths = [int(path_length) for *_, path_length, _, _, _ in rows if path_length not in (None, "")]
    conversions = sum(float(net_conversions_total or 0) for *_, net_conversions_total, _ in rows)
    revenue = sum(float(net_revenue_total or 0) for *_, net_revenue_total in rows)
    return {
        "journey_rows": len(rows),
        "share_of_rows": (len(rows) / baseline_count) if baseline_count else 0.0,
        "profiles": len(profiles),
        "conversions": round(conversions, 2),
        "revenue": round(revenue, 2),
        "median_lag_days": round(statistics.median(lag_days), 2) if lag_days else None,
        "avg_path_length": round(sum(path_lengths) / len(path_lengths), 2) if path_lengths else None,
    }


def _top_values_from_rows(values: List[Any], *, limit: int = 8) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = {}
    total = 0
    for value in values:
        normalized = str(value or "").strip()
        if not normalized:
            continue
        total += 1
        counts[normalized] = counts.get(normalized, 0) + 1
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]
    return [
        {"value": value, "count": count, "share": round(count / total, 4) if total else 0.0}
        for value, count in ordered
    ]


def _build_role_entity_summary(
    db: Session,
    *,
    conversion_ids: List[str],
    scope_type: str,
    limit: int = 24,
) -> List[Dict[str, Any]]:
    if not conversion_ids:
        return []
    q = (
        db.query(JourneyRoleFact)
        .filter(JourneyRoleFact.conversion_id.in_(conversion_ids))
        .filter(JourneyRoleFact.role_key.in_(["first", "assist", "last"]))
    )
    buckets: Dict[str, Dict[str, Any]] = {}
    for row in q.all():
        if scope_type == "channels":
            key = str(row.channel_group or row.channel or "unknown")
            label = key
            secondary_label = "Channel"
        else:
            key = str(row.campaign or "").strip() or str(row.channel_group or row.channel or "unknown")
            label = key
            secondary_label = str(row.channel_group or row.channel or "unknown")
        bucket = buckets.setdefault(
            key,
            {
                "id": key,
                "label": label,
                "secondaryLabel": secondary_label,
                "firstConversions": 0.0,
                "assistConversions": 0.0,
                "lastConversions": 0.0,
                "firstRevenue": 0.0,
                "assistRevenue": 0.0,
                "lastRevenue": 0.0,
            },
        )
        if row.role_key == "first":
            bucket["firstConversions"] += float(row.net_conversions_total or 0)
            bucket["firstRevenue"] += float(row.net_revenue_total or 0)
        elif row.role_key == "assist":
            bucket["assistConversions"] += float(row.net_conversions_total or 0)
            bucket["assistRevenue"] += float(row.net_revenue_total or 0)
        else:
            bucket["lastConversions"] += float(row.net_conversions_total or 0)
            bucket["lastRevenue"] += float(row.net_revenue_total or 0)
    items = sorted(
        buckets.values(),
        key=lambda item: -(
            float(item["firstConversions"])
            + float(item["assistConversions"])
            + float(item["lastConversions"])
        ),
    )
    return items[:limit]


def _build_role_mix(db: Session, *, conversion_ids: List[str]) -> Dict[str, Any]:
    if not conversion_ids:
        return {
            "first_touch_conversions": 0.0,
            "assist_conversions": 0.0,
            "last_touch_conversions": 0.0,
            "first_touch_revenue": 0.0,
            "assist_revenue": 0.0,
            "last_touch_revenue": 0.0,
        }
    q = (
        db.query(ConversionScopeDiagnosticFact)
        .filter(ConversionScopeDiagnosticFact.scope_type == "channel")
        .filter(ConversionScopeDiagnosticFact.conversion_id.in_(conversion_ids))
    )
    totals = {
        "first_touch_conversions": 0.0,
        "assist_conversions": 0.0,
        "last_touch_conversions": 0.0,
        "first_touch_revenue": 0.0,
        "assist_revenue": 0.0,
        "last_touch_revenue": 0.0,
    }
    for row in q.all():
        totals["first_touch_conversions"] += float(row.first_touch_conversions or 0)
        totals["assist_conversions"] += float(row.assist_conversions or 0)
        totals["last_touch_conversions"] += float(row.last_touch_conversions or 0)
        totals["first_touch_revenue"] += float(row.first_touch_revenue or 0)
        totals["assist_revenue"] += float(row.assist_revenue or 0)
        totals["last_touch_revenue"] += float(row.last_touch_revenue or 0)
    for key, value in list(totals.items()):
        totals[key] = round(value, 2)
    return totals


def build_local_segment_analysis(
    db: Session,
    *,
    segment_id: str,
    workspace_id: str,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    journey_definition_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    row = get_local_segment_row(db, segment_id=segment_id, workspace_id=workspace_id)
    if not row:
        return None
    item = _serialize_local_segment_with_preview(db, row)
    definition = dict(item.get("definition") or {})
    baseline_query = db.query(func.count(JourneyDefinitionInstanceFact.id))
    if date_from is not None:
        baseline_query = baseline_query.filter(JourneyDefinitionInstanceFact.date >= date_from)
    if date_to is not None:
        baseline_query = baseline_query.filter(JourneyDefinitionInstanceFact.date <= date_to)
    if journey_definition_id:
        baseline_query = baseline_query.filter(JourneyDefinitionInstanceFact.journey_definition_id == journey_definition_id)
    baseline_count = int(baseline_query.scalar() or 0)
    rows = _build_scoped_rows_query(
        db,
        definition=definition,
        date_from=date_from,
        date_to=date_to,
        journey_definition_id=journey_definition_id,
    ).all()
    conversion_ids = [str(conversion_id or "").strip() for conversion_id, *_ in rows if str(conversion_id or "").strip()]
    return {
        "segment": item,
        "scope": {
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "journey_definition_id": journey_definition_id,
        },
        "summary": _summarize_scoped_rows(rows, baseline_count),
        "baseline_summary": {"journey_rows": baseline_count},
        "distributions": {
            "channels": _top_values_from_rows([row[2] for row in rows]),
            "campaigns": _top_values_from_rows([row[3] for row in rows]),
            "devices": _top_values_from_rows([row[4] for row in rows]),
            "countries": _top_values_from_rows([row[5] for row in rows]),
            "conversion_keys": _top_values_from_rows([row[6] for row in rows]),
            "last_touch_channels": _top_values_from_rows([row[7] for row in rows]),
            "path_types": _top_values_from_rows([row[8] for row in rows]),
        },
        "role_mix": _build_role_mix(db, conversion_ids=conversion_ids),
        "role_entities": {
            "channels": _build_role_entity_summary(db, conversion_ids=conversion_ids, scope_type="channels"),
            "campaigns": _build_role_entity_summary(db, conversion_ids=conversion_ids, scope_type="campaigns"),
        },
    }


def _classify_segment_overlap(
    *,
    overlap_share_of_primary: float,
    overlap_share_of_other: float,
    jaccard: float,
) -> str:
    if overlap_share_of_primary >= 0.8 and overlap_share_of_other >= 0.8:
        return "near_duplicate"
    if overlap_share_of_primary >= 0.8:
        return "mostly_contained_in_other"
    if overlap_share_of_other >= 0.8:
        return "mostly_contains_other"
    if jaccard >= 0.4:
        return "substantial_overlap"
    if jaccard > 0:
        return "partial_overlap"
    return "distinct"


def build_local_segment_overlap(
    db: Session,
    *,
    segment_id: str,
    workspace_id: str,
) -> Optional[Dict[str, Any]]:
    row = get_local_segment_row(db, segment_id=segment_id, workspace_id=workspace_id)
    if not row:
        return None
    primary_item = _serialize_local_segment_with_preview(db, row)
    primary_definition = dict(primary_item.get("definition") or {})
    primary_ids = set(_matched_instance_ids(db, definition=primary_definition))
    candidate_rows = (
        db.query(LocalAnalyticalSegment)
        .filter(
            LocalAnalyticalSegment.workspace_id == workspace_id,
            LocalAnalyticalSegment.id != segment_id,
            LocalAnalyticalSegment.status != "archived",
        )
        .order_by(LocalAnalyticalSegment.created_at.desc())
        .all()
    )
    items: List[Dict[str, Any]] = []
    for candidate in candidate_rows:
        candidate_item = _serialize_local_segment_with_preview(db, candidate)
        candidate_definition = dict(candidate_item.get("definition") or {})
        candidate_ids = set(_matched_instance_ids(db, definition=candidate_definition))
        overlap_count = len(primary_ids & candidate_ids)
        union_count = len(primary_ids | candidate_ids)
        primary_count = len(primary_ids)
        candidate_count = len(candidate_ids)
        overlap_share_of_primary = (overlap_count / primary_count) if primary_count else 0.0
        overlap_share_of_other = (overlap_count / candidate_count) if candidate_count else 0.0
        jaccard = (overlap_count / union_count) if union_count else 0.0
        items.append(
            {
                "segment": candidate_item,
                "overlap_rows": overlap_count,
                "primary_rows": primary_count,
                "other_rows": candidate_count,
                "overlap_share_of_primary": round(overlap_share_of_primary, 4),
                "overlap_share_of_other": round(overlap_share_of_other, 4),
                "jaccard": round(jaccard, 4),
                "relationship": _classify_segment_overlap(
                    overlap_share_of_primary=overlap_share_of_primary,
                    overlap_share_of_other=overlap_share_of_other,
                    jaccard=jaccard,
                ),
            }
        )
    items.sort(key=lambda item: (-float(item["jaccard"]), -int(item["overlap_rows"]), str(item["segment"].get("name") or "")))
    return {
        "segment": primary_item,
        "summary": {
            "journey_rows": len(primary_ids),
            "compared_segments": len(items),
            "near_duplicates": sum(1 for item in items if item["relationship"] == "near_duplicate"),
            "substantial_overlaps": sum(
                1 for item in items if item["relationship"] in {"near_duplicate", "mostly_contained_in_other", "mostly_contains_other", "substantial_overlap"}
            ),
        },
        "items": items[:8],
    }


def build_local_segment_comparison(
    db: Session,
    *,
    segment_id: str,
    other_segment_id: str,
    workspace_id: str,
) -> Optional[Dict[str, Any]]:
    primary_row = get_local_segment_row(db, segment_id=segment_id, workspace_id=workspace_id)
    other_row = get_local_segment_row(db, segment_id=other_segment_id, workspace_id=workspace_id)
    if not primary_row or not other_row:
        return None
    primary_item = _serialize_local_segment_with_preview(db, primary_row)
    other_item = _serialize_local_segment_with_preview(db, other_row)
    primary_definition = dict(primary_item.get("definition") or {})
    other_definition = dict(other_item.get("definition") or {})

    baseline_count = int(db.query(func.count(JourneyDefinitionInstanceFact.id)).scalar() or 0)
    primary_rows = _build_scoped_rows_query(db, definition=primary_definition).all()
    other_rows = _build_scoped_rows_query(db, definition=other_definition).all()
    primary_summary = _summarize_scoped_rows(primary_rows, baseline_count)
    other_summary = _summarize_scoped_rows(other_rows, baseline_count)

    primary_ids = {int(row_id) for row_id in _matched_instance_ids(db, definition=primary_definition)}
    other_ids = {int(row_id) for row_id in _matched_instance_ids(db, definition=other_definition)}
    overlap_count = len(primary_ids & other_ids)
    union_count = len(primary_ids | other_ids)
    overlap_share_of_primary = (overlap_count / len(primary_ids)) if primary_ids else 0.0
    overlap_share_of_other = (overlap_count / len(other_ids)) if other_ids else 0.0
    jaccard = (overlap_count / union_count) if union_count else 0.0

    def _delta(key: str) -> Optional[float]:
        left = primary_summary.get(key)
        right = other_summary.get(key)
        if left is None or right is None:
            return None
        return round(float(left) - float(right), 2)

    return {
        "primary_segment": primary_item,
        "other_segment": other_item,
        "baseline_summary": {"journey_rows": baseline_count},
        "primary_summary": primary_summary,
        "other_summary": other_summary,
        "overlap": {
            "overlap_rows": overlap_count,
            "overlap_share_of_primary": round(overlap_share_of_primary, 4),
            "overlap_share_of_other": round(overlap_share_of_other, 4),
            "jaccard": round(jaccard, 4),
            "relationship": _classify_segment_overlap(
                overlap_share_of_primary=overlap_share_of_primary,
                overlap_share_of_other=overlap_share_of_other,
                jaccard=jaccard,
            ),
        },
        "distributions": {
            "primary_channels": _top_values_from_rows([row[2] for row in primary_rows], limit=5),
            "other_channels": _top_values_from_rows([row[2] for row in other_rows], limit=5),
            "primary_path_types": _top_values_from_rows([row[8] for row in primary_rows], limit=5),
            "other_path_types": _top_values_from_rows([row[8] for row in other_rows], limit=5),
        },
        "deltas": {
            "journey_rows": _delta("journey_rows"),
            "share_of_rows": _delta("share_of_rows"),
            "profiles": _delta("profiles"),
            "conversions": _delta("conversions"),
            "revenue": _delta("revenue"),
            "median_lag_days": _delta("median_lag_days"),
            "avg_path_length": _delta("avg_path_length"),
        },
    }


def serialize_local_segment(row: LocalAnalyticalSegment) -> Dict[str, Any]:
    definition = normalize_local_segment_definition(dict(row.definition_json or {}))
    compatibility = _build_segment_compatibility(definition)
    return {
        "id": row.id,
        "workspace_id": row.workspace_id,
        "owner_user_id": row.owner_user_id,
        "name": row.name,
        "description": row.description,
        "status": row.status,
        "source": "local_analytical",
        "source_label": "Local analytical",
        "kind": "analytical",
        "supports_analysis": True,
        "supports_activation": False,
        "supports_hypotheses": True,
        "supports_experiments": True,
        "definition": definition,
        "definition_version": definition.get("version"),
        "segment_family": _infer_segment_family(definition),
        "criteria_label": _build_criteria_label(definition),
        "compatibility": compatibility,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "archived_at": row.archived_at.isoformat() if row.archived_at else None,
    }


def _serialize_local_segment_with_preview(db: Session, row: LocalAnalyticalSegment) -> Dict[str, Any]:
    item = serialize_local_segment(row)
    item["preview"] = _build_segment_preview(db, dict(item.get("definition") or {}))
    return item


def list_local_segments(
    db: Session,
    *,
    workspace_id: str,
    include_archived: bool = False,
) -> List[Dict[str, Any]]:
    query = db.query(LocalAnalyticalSegment).filter(LocalAnalyticalSegment.workspace_id == workspace_id)
    if not include_archived:
        query = query.filter(LocalAnalyticalSegment.status != "archived")
    rows = query.order_by(LocalAnalyticalSegment.created_at.desc()).all()
    items = [serialize_local_segment(row) for row in rows]
    for item in items:
        item["preview"] = _build_segment_preview(db, dict(item.get("definition") or {}))
    return items


def create_local_segment(
    db: Session,
    *,
    workspace_id: str,
    owner_user_id: str,
    name: str,
    description: Optional[str],
    definition: Dict[str, Any],
) -> Dict[str, Any]:
    normalized_definition = normalize_local_segment_definition(definition)
    if not normalized_definition.get("rules"):
        raise ValueError("definition must include at least one valid rule")
    row = LocalAnalyticalSegment(
        id=_new_id(),
        workspace_id=workspace_id,
        owner_user_id=owner_user_id,
        name=name.strip(),
        description=(description or "").strip() or None,
        definition_json=normalized_definition,
        status="active",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return serialize_local_segment(row)


def update_local_segment(
    db: Session,
    *,
    segment_id: str,
    workspace_id: str,
    name: str,
    description: Optional[str],
    definition: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    row = (
        db.query(LocalAnalyticalSegment)
        .filter(
            LocalAnalyticalSegment.id == segment_id,
            LocalAnalyticalSegment.workspace_id == workspace_id,
        )
        .first()
    )
    if not row:
        return None
    normalized_definition = normalize_local_segment_definition(definition)
    if not normalized_definition.get("rules"):
        raise ValueError("definition must include at least one valid rule")
    row.name = name.strip()
    row.description = (description or "").strip() or None
    row.definition_json = normalized_definition
    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()
    db.refresh(row)
    return serialize_local_segment(row)


def set_local_segment_status(
    db: Session,
    *,
    segment_id: str,
    workspace_id: str,
    status: str,
) -> Optional[Dict[str, Any]]:
    row = (
        db.query(LocalAnalyticalSegment)
        .filter(
            LocalAnalyticalSegment.id == segment_id,
            LocalAnalyticalSegment.workspace_id == workspace_id,
        )
        .first()
    )
    if not row:
        return None
    row.status = status
    row.archived_at = datetime.utcnow() if status == "archived" else None
    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()
    db.refresh(row)
    return serialize_local_segment(row)


def _normalize_meiro_segment(raw: Dict[str, Any], *, workspace_id: str) -> Dict[str, Any]:
    segment_id = (
        raw.get("id")
        or raw.get("segment_id")
        or raw.get("uuid")
        or raw.get("key")
        or raw.get("slug")
        or ""
    )
    name = raw.get("name") or raw.get("title") or raw.get("label") or str(segment_id or "Unnamed segment")
    count = (
        raw.get("profiles_count")
        or raw.get("profile_count")
        or raw.get("profileCount")
        or raw.get("count")
        or raw.get("size")
        or raw.get("estimated_count")
    )
    size_value = int(count) if isinstance(count, (int, float)) else None
    return {
        "id": f"meiro:{segment_id}" if segment_id else f"meiro:{name}",
        "external_segment_id": str(segment_id or name),
        "workspace_id": workspace_id,
        "name": str(name),
        "description": raw.get("description"),
        "status": "active",
        "source": "meiro_pipes",
        "source_label": "Meiro Pipes",
        "kind": "operational",
        "supports_analysis": False,
        "supports_activation": True,
        "supports_hypotheses": True,
        "supports_experiments": True,
        "definition": {"external_segment_id": str(segment_id or name)},
        "criteria_label": "Operational audience from Meiro Pipes",
        "definition_version": "external",
        "segment_family": "operational_external",
        "compatibility": {"filter_keys": [], "auto_filter_compatible": False, "advanced": True},
        "size": size_value,
        "audience_capability": {
            "level": "estimated_reach" if size_value is not None and size_value > 0 else "definition_only",
            "label": "Estimated reach" if size_value is not None and size_value > 0 else "Definition only",
            "membership_backed": False,
            "activation_ready": True,
            "measurement_ready": False,
            "profile_count": size_value,
            "reason": (
                "External audience provides an estimated profile count, but profile membership is not present in measurement journeys."
                if size_value is not None and size_value > 0
                else "Audience definition is available, but profile membership has not been observed in the MMM workspace."
            ),
        },
        "raw": raw,
    }


def _normalize_segment_membership(raw: Any) -> Optional[Dict[str, str]]:
    if raw in (None, "", []):
        return None
    if isinstance(raw, dict):
        segment_id = (
            raw.get("id")
            or raw.get("segment_id")
            or raw.get("key")
            or raw.get("slug")
            or raw.get("uuid")
        )
        name = raw.get("name") or raw.get("segment_name") or raw.get("title") or raw.get("label")
        if segment_id in (None, "", []) and name in (None, "", []):
            return None
        resolved_id = str(segment_id or name).strip()
        resolved_name = str(name or segment_id).strip()
        if not resolved_id or not resolved_name:
            return None
        return {"id": resolved_id, "name": resolved_name}
    if isinstance(raw, (str, int, float)):
        value = str(raw).strip()
        if not value:
            return None
        return {"id": value, "name": value}
    return None


def _extract_segment_memberships(record: Any) -> List[Dict[str, str]]:
    memberships: List[Dict[str, str]] = []

    def _capture(raw: Any) -> None:
        if isinstance(raw, list):
            for item in raw:
                _capture(item)
            return
        normalized = _normalize_segment_membership(raw)
        if normalized is None:
            return
        if any(item["id"] == normalized["id"] for item in memberships):
            return
        memberships.append(normalized)

    def _inspect(container: Any) -> None:
        if not isinstance(container, dict):
            return
        if container.get("segments") not in (None, "", []):
            _capture(container.get("segments"))
        if container.get("segment") not in (None, "", []):
            _capture(container.get("segment"))
        segment_id = container.get("segment_id")
        segment_name = container.get("segment_name") or container.get("segment_label")
        if segment_id not in (None, "", []) or segment_name not in (None, "", []):
            _capture({"id": segment_id or segment_name, "name": segment_name or segment_id})

    if not isinstance(record, dict):
        return memberships
    _inspect(record)
    for key in ("attributes", "profile_attributes", "traits", "properties"):
        _inspect(record.get(key))
    for key in ("customer", "profile", "user", "person"):
        nested = record.get(key)
        _inspect(nested)
        if isinstance(nested, dict):
            for nested_key in ("attributes", "profile_attributes", "traits", "properties"):
                _inspect(nested.get(nested_key))
    return memberships


def _list_webhook_derived_meiro_segments(
    db: Session,
    *,
    workspace_id: str,
) -> List[Dict[str, Any]]:
    memberships: Dict[str, Dict[str, Any]] = {}

    def _record(profile_id: Any, profile_json: Any, source_label: str) -> None:
        normalized_profile_id = str(profile_id or "").strip()
        if not normalized_profile_id or not isinstance(profile_json, dict):
            return
        for membership in _extract_segment_memberships(profile_json):
            external_segment_id = membership["id"]
            bucket = memberships.setdefault(
                external_segment_id,
                {
                    "id": external_segment_id,
                    "name": membership["name"],
                    "profile_ids": set(),
                    "sources": set(),
                },
            )
            bucket["profile_ids"].add(normalized_profile_id)
            bucket["sources"].add(source_label)
            if not bucket.get("name") and membership.get("name"):
                bucket["name"] = membership["name"]

    try:
        for profile_id, profile_json in db.query(MeiroProfileFact.profile_id, MeiroProfileFact.profile_json).all():
            _record(profile_id, profile_json, "profiles_webhook")
    except (SQLAlchemyError, UnicodeDecodeError, ValueError):
        pass
    try:
        for profile_id, profile_json in db.query(MeiroEventProfileState.profile_id, MeiroEventProfileState.profile_json).all():
            _record(profile_id, profile_json, "raw_events_replay")
    except (SQLAlchemyError, UnicodeDecodeError, ValueError):
        pass

    items: List[Dict[str, Any]] = []
    for payload in memberships.values():
        sources = sorted(str(item) for item in payload["sources"])
        segment = _normalize_meiro_segment(
            {
                "id": payload["id"],
                "name": payload["name"],
                "profiles_count": len(payload["profile_ids"]),
                "description": "Derived from Meiro Pipes webhook payloads",
            },
            workspace_id=workspace_id,
        )
        segment["definition"] = {
            "external_segment_id": payload["id"],
            "derived_from": "webhook_payload",
            "ingestion_sources": sources,
        }
        segment["audience_capability"] = {
            "level": "membership_backed",
            "label": "Membership backed",
            "membership_backed": True,
            "activation_ready": True,
            "measurement_ready": True,
            "profile_count": len(payload["profile_ids"]),
            "reason": "Profile membership was observed in Meiro Pipes webhook/profile state, so this audience can scope activation measurement when journeys carry membership.",
        }
        segment["criteria_label"] = "Operational audience from Meiro Pipes webhook payloads"
        segment["description"] = (
            f"Derived from Meiro Pipes webhook payloads ({', '.join(sources)})"
            if sources
            else "Derived from Meiro Pipes webhook payloads"
        )
        items.append(segment)
    return items


def list_membership_profile_ids_for_external_segment(db: Session, external_segment_id: str) -> List[str]:
    target_id = str(external_segment_id or "").strip()
    if not target_id:
        return []
    profile_ids: set[str] = set()

    def _record(profile_id: Any, profile_json: Any) -> None:
        normalized_profile_id = str(profile_id or "").strip()
        if not normalized_profile_id or not isinstance(profile_json, dict):
            return
        for membership in _extract_segment_memberships(profile_json):
            if str(membership.get("id") or "").strip() == target_id:
                profile_ids.add(normalized_profile_id)

    try:
        for profile_id, profile_json in db.query(MeiroProfileFact.profile_id, MeiroProfileFact.profile_json).all():
            _record(profile_id, profile_json)
    except (SQLAlchemyError, UnicodeDecodeError, ValueError):
        pass
    try:
        for profile_id, profile_json in db.query(MeiroEventProfileState.profile_id, MeiroEventProfileState.profile_json).all():
            _record(profile_id, profile_json)
    except (SQLAlchemyError, UnicodeDecodeError, ValueError):
        pass
    return sorted(profile_ids)


def _normalize_deciengine_events_config(payload: Any) -> Dict[str, Any]:
    raw = payload if isinstance(payload, dict) else {}
    source_url = str(raw.get("source_url") or "http://host.docker.internal:3001/v1/inapp/events").strip()
    user_email = str(raw.get("user_email") or "").strip()
    user_role = str(raw.get("user_role") or "").strip()
    user_id = str(raw.get("user_id") or "").strip()
    return {
        "source_url": source_url,
        "user_email": user_email or None,
        "user_role": user_role or None,
        "user_id": user_id or None,
    }


def _load_deciengine_events_config_for_segments() -> Dict[str, Any]:
    if not DECIENGINE_EVENTS_CONFIG_PATH.exists():
        return _normalize_deciengine_events_config({})
    try:
        return _normalize_deciengine_events_config(json.loads(DECIENGINE_EVENTS_CONFIG_PATH.read_text()))
    except Exception:
        return _normalize_deciengine_events_config({})


def _deciengine_base_url(source_url: str) -> Optional[str]:
    parsed = urllib.parse.urlparse(str(source_url or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, "", "", "", "")).rstrip("/")


def _fetch_deciengine_pipes_field_registry() -> Optional[Dict[str, Any]]:
    config = _load_deciengine_events_config_for_segments()
    base_url = _deciengine_base_url(str(config.get("source_url") or ""))
    if not base_url:
        return None
    headers = {"Accept": "application/json"}
    if config.get("user_email"):
        headers["X-User-Email"] = str(config["user_email"])
    if config.get("user_role"):
        headers["X-User-Role"] = str(config["user_role"])
    if config.get("user_id"):
        headers["X-User-Id"] = str(config["user_id"])
    request = urllib.request.Request(f"{base_url}/v1/settings/pipes-prism/field-registry", headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _load_cached_deciengine_pipes_field_registry() -> Optional[Dict[str, Any]]:
    if not DECIENGINE_PIPES_REGISTRY_CACHE_PATH.exists():
        return None
    try:
        payload = json.loads(DECIENGINE_PIPES_REGISTRY_CACHE_PATH.read_text())
    except Exception:
        return None
    registry = payload.get("registry") if isinstance(payload, dict) else None
    return registry if isinstance(registry, dict) else None


def _save_deciengine_pipes_field_registry_cache(registry: Dict[str, Any]) -> None:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        DECIENGINE_PIPES_REGISTRY_CACHE_PATH.write_text(
            json.dumps(
                {
                    "cached_at": datetime.utcnow().isoformat() + "Z",
                    "source": "deciengine_pipes_field_registry",
                    "registry": registry,
                },
                indent=2,
                sort_keys=True,
            )
        )
    except Exception:
        pass


def _list_deciengine_pipes_registry_segments(
    *,
    workspace_id: str,
    refresh: bool = False,
) -> List[Dict[str, Any]]:
    registry = _fetch_deciengine_pipes_field_registry() if refresh else None
    if registry:
        _save_deciengine_pipes_field_registry_cache(registry)
    else:
        registry = _load_cached_deciengine_pipes_field_registry()
    audiences = registry.get("audiences") if isinstance(registry, dict) else None
    if not isinstance(audiences, list):
        return []

    items: List[Dict[str, Any]] = []
    for raw in audiences:
        if not isinstance(raw, dict):
            continue
        normalized = _normalize_meiro_segment(raw, workspace_id=workspace_id)
        normalized["source"] = "meiro_pipes_registry"
        normalized["source_label"] = "Meiro Pipes registry"
        normalized["description"] = raw.get("description") or "Imported from deciEngine Pipes field registry"
        normalized["criteria_label"] = "Operational audience from deciEngine Pipes registry"
        normalized["definition"] = {
            **dict(normalized.get("definition") or {}),
            "derived_from": "deciengine_pipes_field_registry",
            "registry_source": "pipes_cli",
        }
        normalized["audience_capability"] = {
            "level": "definition_only",
            "label": "Definition only",
            "membership_backed": False,
            "activation_ready": True,
            "measurement_ready": False,
            "profile_count": normalized.get("size"),
            "reason": "Imported from the deciEngine Pipes registry. It is safe as an operational audience reference, but measurement scoping needs observed membership.",
        }
        items.append(normalized)
    return items


def _merge_meiro_registry_items(*collections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for collection in collections:
        for item in collection:
            key = str(item.get("external_segment_id") or item.get("id") or "").strip()
            if not key:
                continue
            existing = merged.get(key)
            if existing is None:
                merged[key] = dict(item)
                continue
            if not existing.get("description") and item.get("description"):
                existing["description"] = item.get("description")
            current_size = existing.get("size")
            incoming_size = item.get("size")
            if incoming_size is not None:
                if current_size is None:
                    existing["size"] = incoming_size
                else:
                    try:
                        existing["size"] = max(int(current_size), int(incoming_size))
                    except Exception:
                        existing["size"] = current_size
            existing_definition = dict(existing.get("definition") or {})
            incoming_definition = dict(item.get("definition") or {})
            ingestion_sources = {
                str(value)
                for value in [*(existing_definition.get("ingestion_sources") or []), *(incoming_definition.get("ingestion_sources") or [])]
                if str(value or "").strip()
            }
            if ingestion_sources:
                existing_definition["ingestion_sources"] = sorted(ingestion_sources)
            if incoming_definition.get("derived_from") and not existing_definition.get("derived_from"):
                existing_definition["derived_from"] = incoming_definition.get("derived_from")
            if incoming_definition.get("external_segment_id") and not existing_definition.get("external_segment_id"):
                existing_definition["external_segment_id"] = incoming_definition.get("external_segment_id")
            existing["definition"] = existing_definition
            existing_capability = dict(existing.get("audience_capability") or {})
            incoming_capability = dict(item.get("audience_capability") or {})
            if incoming_capability.get("membership_backed") or incoming_capability.get("measurement_ready"):
                existing["audience_capability"] = incoming_capability
            elif not existing_capability:
                existing["audience_capability"] = incoming_capability
            elif existing_capability.get("level") == "definition_only" and incoming_capability.get("level") == "estimated_reach":
                existing["audience_capability"] = incoming_capability
            if existing.get("audience_capability") and existing.get("size") is not None:
                existing["audience_capability"]["profile_count"] = existing.get("size")
            existing["criteria_label"] = "Operational audience from Meiro Pipes"
            merged[key] = existing
    return sorted(
        merged.values(),
        key=lambda item: (
            -int(item.get("size") or 0),
            str(item.get("name") or "").lower(),
        ),
    )


def _audience_capability_summary(items: List[Dict[str, Any]]) -> Dict[str, int]:
    definition_only = 0
    estimated_reach = 0
    membership_backed = 0
    activation_ready = 0
    measurement_ready = 0
    for item in items:
        capability = item.get("audience_capability") if isinstance(item.get("audience_capability"), dict) else {}
        level = str(capability.get("level") or "").strip().lower()
        if level == "definition_only":
            definition_only += 1
        if level == "estimated_reach":
            estimated_reach += 1
        if capability.get("membership_backed"):
            membership_backed += 1
        if capability.get("activation_ready") or item.get("supports_activation"):
            activation_ready += 1
        if capability.get("measurement_ready"):
            measurement_ready += 1
    return {
        "definition_only": definition_only,
        "estimated_reach": estimated_reach,
        "membership_backed": membership_backed,
        "activation_ready": activation_ready,
        "measurement_ready": measurement_ready,
    }


def list_meiro_segment_registry(
    db: Session,
    *,
    workspace_id: str,
    source: str = "all",
) -> Dict[str, Any]:
    normalized_source = str(source or "all").strip().lower()
    if normalized_source not in {"all", "cdp", "pipes_webhook", "pipes_registry"}:
        raise ValueError("source must be one of: all, cdp, pipes_webhook, pipes_registry")
    meiro_cdp_segments: List[Dict[str, Any]] = []
    if normalized_source in {"all", "cdp"} and meiro_cdp.is_connected():
        try:
            raw_segments = meiro_cdp.list_segments()
            if isinstance(raw_segments, list):
                meiro_cdp_segments = [
                    _normalize_meiro_segment(item, workspace_id=workspace_id) for item in raw_segments if isinstance(item, dict)
                ]
        except Exception:
            meiro_cdp_segments = []
    webhook_meiro_segments = (
        _list_webhook_derived_meiro_segments(db, workspace_id=workspace_id)
        if normalized_source in {"all", "pipes_webhook"}
        else []
    )
    pipes_registry_segments = (
        _list_deciengine_pipes_registry_segments(
            workspace_id=workspace_id,
            refresh=normalized_source == "pipes_registry",
        )
        if normalized_source in {"all", "pipes_registry"}
        else []
    )
    meiro_segments = _merge_meiro_registry_items(meiro_cdp_segments, webhook_meiro_segments, pipes_registry_segments)
    capability_summary = _audience_capability_summary(meiro_segments)
    return {
        "items": meiro_segments,
        "summary": {
            "source": normalized_source,
            "meiro_pipes": len(meiro_segments),
            "cdp_segments": len(meiro_cdp_segments),
            "pipes_webhook_segments": len(webhook_meiro_segments),
            "pipes_registry_segments": len(pipes_registry_segments),
            **capability_summary,
        },
    }


def import_meiro_segments(
    db: Session,
    *,
    workspace_id: str,
    source: str = "pipes_webhook",
) -> Dict[str, Any]:
    payload = list_meiro_segment_registry(db, workspace_id=workspace_id, source=source)
    items = payload.get("items") or []
    return {
        "status": "synced",
        "message": f"Synced {len(items)} Meiro audience segment{'s' if len(items) != 1 else ''} into the MMM segment registry.",
        "items": items,
        "summary": payload.get("summary") or {},
    }


def list_segment_registry(
    db: Session,
    *,
    workspace_id: str,
    include_archived: bool = False,
) -> Dict[str, Any]:
    local_segments = list_local_segments(db, workspace_id=workspace_id, include_archived=include_archived)
    meiro_payload = list_meiro_segment_registry(db, workspace_id=workspace_id, source="all")
    meiro_segments = meiro_payload.get("items") or []
    capability_summary = _audience_capability_summary(meiro_segments)
    return {
        "items": [*local_segments, *meiro_segments],
        "summary": {
            "local_analytical": len(local_segments),
            "meiro_pipes": len(meiro_segments),
            "analysis_ready": sum(1 for item in local_segments if item.get("supports_analysis")),
            **capability_summary,
        },
    }


def _top_segment_dimension_values(db: Session, column: Any, *, limit: int = 100) -> List[Dict[str, Any]]:
    rows = (
        db.query(column, func.count(JourneyDefinitionInstanceFact.id).label("count"))
        .filter(column.isnot(None))
        .filter(column != "")
        .group_by(column)
        .order_by(func.count(JourneyDefinitionInstanceFact.id).desc(), column.asc())
        .limit(max(1, min(limit, 500)))
        .all()
    )
    return [{"value": str(value), "count": int(count or 0)} for value, count in rows if str(value or "").strip()]


def build_segment_context(db: Session) -> Dict[str, Any]:
    total_rows = db.query(func.count(JourneyDefinitionInstanceFact.id)).scalar() or 0
    date_from, date_to = (
        db.query(
            func.min(JourneyDefinitionInstanceFact.date),
            func.max(JourneyDefinitionInstanceFact.date),
        ).first()
        or (None, None)
    )
    return {
        "summary": {
            "journey_rows": int(total_rows),
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
        },
        "channels": _top_segment_dimension_values(db, JourneyDefinitionInstanceFact.channel_group),
        "campaigns": _top_segment_dimension_values(db, JourneyDefinitionInstanceFact.campaign_id),
        "devices": _top_segment_dimension_values(db, JourneyDefinitionInstanceFact.device),
        "countries": _top_segment_dimension_values(db, JourneyDefinitionInstanceFact.country),
        "conversion_keys": _top_segment_dimension_values(db, JourneyDefinitionInstanceFact.conversion_key),
        "last_touch_channels": _top_segment_dimension_values(db, JourneyDefinitionInstanceFact.last_touch_channel),
        "path_types": _top_segment_dimension_values(db, JourneyDefinitionInstanceFact.interaction_path_type),
        "suggested_rules": {
            "lag_days": [
                {"label": "Lag >= 1 day", "field": "lag_days", "op": "gte", "value": 1},
                {"label": "Lag >= 3 days", "field": "lag_days", "op": "gte", "value": 3},
                {"label": "Lag >= 7 days", "field": "lag_days", "op": "gte", "value": 7},
            ],
            "path_length": [
                {"label": "Path length >= 3", "field": "path_length", "op": "gte", "value": 3},
                {"label": "Path length >= 5", "field": "path_length", "op": "gte", "value": 5},
                {"label": "Path length <= 2", "field": "path_length", "op": "lte", "value": 2},
            ],
            "value": [
                {"label": "Net revenue >= 100", "field": "net_revenue_total", "op": "gte", "value": 100},
                {"label": "Net conversions >= 2", "field": "net_conversions_total", "op": "gte", "value": 2},
            ],
        },
    }
