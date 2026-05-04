"""Activation-object measurement summaries for governed activation integrations."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from app.services_metrics import journey_revenue_value


ACTIVATION_FEEDBACK_EXPORTS_FILE = Path(__file__).resolve().parent / "data" / "activation_feedback_exports.json"
MAX_ACTIVATION_FEEDBACK_EXPORT_RUNS = 50

SUPPORTED_ACTIVATION_OBJECT_TYPES = {
    "campaign",
    "decision",
    "decision_stack",
    "asset",
    "offer",
    "content",
    "bundle",
    "experiment",
    "variant",
    "placement",
    "template",
}


def _as_text(value: Any) -> str:
    return str(value or "").strip()


def _activation_meta(touchpoint: Dict[str, Any]) -> Dict[str, Any]:
    meta = touchpoint.get("meta") if isinstance(touchpoint.get("meta"), dict) else {}
    activation = meta.get("activation") if isinstance(meta.get("activation"), dict) else {}
    return activation


def _touchpoint_campaign_name(touchpoint: Dict[str, Any]) -> str:
    campaign = touchpoint.get("campaign")
    if isinstance(campaign, dict):
        return _as_text(campaign.get("name") or campaign.get("id"))
    return _as_text(campaign)


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value in (None, "", []):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _date_bounds(date_from: Optional[str], date_to: Optional[str]) -> Tuple[Optional[datetime], Optional[datetime]]:
    start = _parse_datetime(f"{date_from}T00:00:00+00:00") if date_from else None
    end = _parse_datetime(f"{date_to}T23:59:59.999999+00:00") if date_to else None
    if start and end and end < start:
        start, end = end, start
    return start, end


def _in_bounds(value: Any, start: Optional[datetime], end: Optional[datetime]) -> bool:
    parsed = _parse_datetime(value)
    if parsed is None:
        return start is None and end is None
    if start and parsed < start:
        return False
    if end and parsed > end:
        return False
    return True


def _candidate_values_for_object(touchpoint: Dict[str, Any], object_type: str) -> List[str]:
    activation = _activation_meta(touchpoint)
    if object_type == "campaign":
        return [
            _as_text(activation.get("activation_campaign_id")),
            _as_text(activation.get("campaign_id")),
            _as_text(activation.get("native_meiro_campaign_id")),
            _as_text(touchpoint.get("campaign_id")),
            _touchpoint_campaign_name(touchpoint),
        ]
    if object_type == "decision":
        return [_as_text(activation.get("decision_key"))]
    if object_type == "decision_stack":
        return [_as_text(activation.get("decision_stack_key"))]
    if object_type == "asset":
        return [
            _as_text(activation.get("asset_id")),
            _as_text(activation.get("creative_asset_id")),
            _as_text(activation.get("native_meiro_asset_id")),
            _as_text(activation.get("offer_id")),
            _as_text(activation.get("content_block_id")),
            _as_text(activation.get("bundle_id")),
        ]
    if object_type == "offer":
        return [_as_text(activation.get("offer_id"))]
    if object_type == "content":
        return [
            _as_text(activation.get("content_block_id")),
            _as_text(activation.get("creative_asset_id")),
            _as_text(activation.get("native_meiro_asset_id")),
        ]
    if object_type == "bundle":
        return [
            _as_text(activation.get("bundle_id")),
            _as_text(activation.get("offer_catalog_id")),
            _as_text(activation.get("native_meiro_catalog_id")),
        ]
    if object_type == "experiment":
        return [_as_text(activation.get("experiment_key")), _as_text(activation.get("experiment_id"))]
    if object_type == "variant":
        return [_as_text(activation.get("variant_key")), _as_text(activation.get("variant_id")), _as_text(touchpoint.get("variant_id"))]
    if object_type == "placement":
        return [_as_text(activation.get("placement_key")), _as_text(touchpoint.get("placement_id"))]
    if object_type == "template":
        return [_as_text(activation.get("template_key"))]
    return []


def _match_targets(object_id: str, match_aliases: Optional[Sequence[str]] = None) -> set[str]:
    targets = {_as_text(object_id)}
    for alias in match_aliases or []:
        text = _as_text(alias)
        if text:
            targets.add(text)
    return {target for target in targets if target}


def _matches_object(
    touchpoint: Dict[str, Any],
    object_type: str,
    object_id: str,
    match_aliases: Optional[Sequence[str]] = None,
) -> bool:
    targets = _match_targets(object_id, match_aliases)
    return bool(targets) and bool(
        targets.intersection({value for value in _candidate_values_for_object(touchpoint, object_type) if value})
    )


def _journey_conversion_ts(journey: Dict[str, Any]) -> Any:
    conversions = journey.get("conversions") if isinstance(journey.get("conversions"), list) else []
    if conversions:
        first = conversions[0] if isinstance(conversions[0], dict) else {}
        return first.get("ts") or first.get("timestamp")
    touchpoints = journey.get("touchpoints") if isinstance(journey.get("touchpoints"), list) else []
    if touchpoints:
        last = touchpoints[-1] if isinstance(touchpoints[-1], dict) else {}
        return last.get("ts") or last.get("timestamp")
    return None


def _journey_is_converted(journey: Dict[str, Any]) -> bool:
    if isinstance(journey.get("converted"), bool):
        return bool(journey.get("converted"))
    conversions = journey.get("conversions")
    return isinstance(conversions, list) and len(conversions) > 0


def _iter_touchpoints(journeys: Iterable[Dict[str, Any]]) -> Iterable[Tuple[Dict[str, Any], Dict[str, Any], int]]:
    for journey in journeys:
        if not isinstance(journey, dict):
            continue
        for index, touchpoint in enumerate(journey.get("touchpoints") or []):
            if isinstance(touchpoint, dict):
                yield journey, touchpoint, index


def _journey_id(journey: Dict[str, Any]) -> str:
    return _as_text(journey.get("journey_id") or journey.get("id") or journey.get("customer_id") or id(journey))


def _profile_id(journey: Dict[str, Any]) -> str:
    customer = journey.get("customer") if isinstance(journey.get("customer"), dict) else {}
    return _as_text(customer.get("id") or journey.get("customer_id") or journey.get("profile_id"))


def _first_conversion(journey: Dict[str, Any]) -> Dict[str, Any]:
    conversions = journey.get("conversions") if isinstance(journey.get("conversions"), list) else []
    return conversions[0] if conversions and isinstance(conversions[0], dict) else {}


def _matches_filters(
    *,
    journey: Dict[str, Any],
    touchpoint: Dict[str, Any],
    object_type: str,
    object_id: str,
    match_aliases: Optional[Sequence[str]],
    start: Optional[datetime],
    end: Optional[datetime],
    conversion_key: Optional[str],
) -> bool:
    if conversion_key and _as_text(journey.get("kpi_type") or journey.get("conversion_key")) != _as_text(conversion_key):
        return False
    if not _in_bounds(touchpoint.get("ts") or touchpoint.get("timestamp"), start, end):
        return False
    return _matches_object(touchpoint, object_type, object_id, match_aliases)


def build_activation_measurement_summary(
    *,
    journeys: List[Dict[str, Any]],
    object_type: str,
    object_id: str,
    match_aliases: Optional[Sequence[str]] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    conversion_key: Optional[str] = None,
) -> Dict[str, Any]:
    resolved_type = _as_text(object_type).lower()
    resolved_id = _as_text(object_id)
    if resolved_type not in SUPPORTED_ACTIVATION_OBJECT_TYPES:
        raise ValueError(f"Unsupported activation object_type '{object_type}'")
    if not resolved_id:
        raise ValueError("object_id is required")

    start, end = _date_bounds(date_from, date_to)
    resolved_aliases = sorted(_match_targets("", match_aliases))
    matched_touchpoints: List[Dict[str, Any]] = []
    matched_journey_ids: set[str] = set()
    matched_profiles: set[str] = set()
    matched_converted_journeys: set[str] = set()
    revenue = 0.0
    variants: set[str] = set()
    experiments: set[str] = set()
    placements: set[str] = set()
    activation_meta_count = 0

    for journey, touchpoint, _index in _iter_touchpoints(journeys):
        if not _matches_filters(
            journey=journey,
            touchpoint=touchpoint,
            object_type=resolved_type,
            object_id=resolved_id,
            match_aliases=resolved_aliases,
            start=start,
            end=end,
            conversion_key=conversion_key,
        ):
            continue
        activation = _activation_meta(touchpoint)
        if activation:
            activation_meta_count += 1
        matched_touchpoints.append(touchpoint)
        journey_id = _journey_id(journey)
        matched_journey_ids.add(journey_id)
        profile_id = _profile_id(journey)
        if profile_id:
            matched_profiles.add(profile_id)
        if _journey_is_converted(journey) and _in_bounds(_journey_conversion_ts(journey), start, end):
            if journey_id not in matched_converted_journeys:
                matched_converted_journeys.add(journey_id)
                revenue += float(journey_revenue_value(journey) or 0.0)
        variant = _as_text(activation.get("variant_key") or activation.get("variant_id") or touchpoint.get("variant_id"))
        experiment = _as_text(activation.get("experiment_key") or activation.get("experiment_id"))
        placement = _as_text(activation.get("placement_key") or touchpoint.get("placement_id"))
        if variant:
            variants.add(variant)
        if experiment:
            experiments.add(experiment)
        if placement:
            placements.add(placement)

    touchpoint_count = len(matched_touchpoints)
    conversion_count = len(matched_converted_journeys)
    activation_coverage = (activation_meta_count / touchpoint_count) if touchpoint_count else 0.0

    return {
        "object": {
            "type": resolved_type,
            "id": resolved_id,
            "match_aliases": resolved_aliases,
        },
        "period": {
            "date_from": date_from,
            "date_to": date_to,
            "conversion_key": conversion_key,
        },
        "summary": {
            "matched_touchpoints": touchpoint_count,
            "matched_journeys": len(matched_journey_ids),
            "matched_profiles": len(matched_profiles),
            "conversions": conversion_count,
            "revenue": round(revenue, 6),
            "conversion_rate": round((conversion_count / len(matched_journey_ids)), 6) if matched_journey_ids else None,
            "activation_metadata_coverage": round(activation_coverage, 6),
            "variants": sorted(variants),
            "experiments": sorted(experiments),
            "placements": sorted(placements),
        },
        "evidence": {
            "attribution": {
                "available": touchpoint_count > 0,
                "method": "journey_touchpoint_summary",
                "basis": "matching activation identifiers on journey touchpoints",
                "limitations": [
                    "This is path-based attribution evidence, not causal lift.",
                    "Credit allocation still depends on the selected attribution model in attribution reports.",
                ],
            },
            "mmm": {
                "available": False,
                "reason": "MMM contribution is not yet linked to activation object IDs in this endpoint.",
            },
            "incrementality": {
                "available": False,
                "reason": "Holdout or experiment lift results are not yet joined to this activation object summary.",
            },
            "data_quality": {
                "status": "ready" if touchpoint_count > 0 and activation_coverage >= 0.8 else "warning" if touchpoint_count > 0 else "unavailable",
                "activation_metadata_coverage": round(activation_coverage, 6),
                "warnings": []
                if touchpoint_count > 0 and activation_coverage >= 0.8
                else ["No matching activation touchpoints found."]
                if touchpoint_count == 0
                else ["Some matched touchpoints lack activation metadata."],
            },
        },
        "recommended_actions": []
        if touchpoint_count > 0
        else [
            {
                "id": "verify_activation_event_mapping",
                "label": "Verify activation event mapping",
                "reason": "No journey touchpoints matched this activation object ID.",
            }
        ],
    }


def build_activation_measurement_evidence(
    *,
    journeys: List[Dict[str, Any]],
    object_type: str,
    object_id: str,
    match_aliases: Optional[Sequence[str]] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    conversion_key: Optional[str] = None,
    limit: int = 20,
) -> Dict[str, Any]:
    resolved_type = _as_text(object_type).lower()
    resolved_id = _as_text(object_id)
    if resolved_type not in SUPPORTED_ACTIVATION_OBJECT_TYPES:
        raise ValueError(f"Unsupported activation object_type '{object_type}'")
    if not resolved_id:
        raise ValueError("object_id is required")

    start, end = _date_bounds(date_from, date_to)
    resolved_aliases = sorted(_match_targets("", match_aliases))
    resolved_limit = max(1, min(int(limit or 20), 100))
    items: List[Dict[str, Any]] = []
    total_matches = 0

    for journey, touchpoint, index in _iter_touchpoints(journeys):
        if not _matches_filters(
            journey=journey,
            touchpoint=touchpoint,
            object_type=resolved_type,
            object_id=resolved_id,
            match_aliases=resolved_aliases,
            start=start,
            end=end,
            conversion_key=conversion_key,
        ):
            continue
        total_matches += 1
        if len(items) >= resolved_limit:
            continue
        activation = _activation_meta(touchpoint)
        conversion = _first_conversion(journey)
        items.append(
            {
                "journey_id": _journey_id(journey),
                "profile_id": _profile_id(journey),
                "touchpoint_index": index,
                "touchpoint_ts": touchpoint.get("ts") or touchpoint.get("timestamp"),
                "conversion_ts": _journey_conversion_ts(journey),
                "converted": _journey_is_converted(journey),
                "revenue": round(float(journey_revenue_value(journey) or 0.0), 6),
                "conversion_id": _as_text(conversion.get("id") or conversion.get("conversion_id") or journey.get("conversion_id")),
                "channel": _as_text(touchpoint.get("channel")),
                "campaign": _touchpoint_campaign_name(touchpoint),
                "campaign_id": _as_text(touchpoint.get("campaign_id") or activation.get("campaign_id") or activation.get("activation_campaign_id")),
                "activation": {
                    key: value
                    for key, value in {
                        "schema_version": activation.get("schema_version"),
                        "source_system": activation.get("source_system"),
                        "activation_campaign_id": activation.get("activation_campaign_id"),
                        "native_meiro_campaign_id": activation.get("native_meiro_campaign_id"),
                        "native_meiro_asset_id": activation.get("native_meiro_asset_id"),
                        "native_meiro_catalog_id": activation.get("native_meiro_catalog_id"),
                        "creative_asset_id": activation.get("creative_asset_id"),
                        "offer_catalog_id": activation.get("offer_catalog_id"),
                        "decision_key": activation.get("decision_key"),
                        "decision_stack_key": activation.get("decision_stack_key"),
                        "offer_id": activation.get("offer_id"),
                        "content_block_id": activation.get("content_block_id"),
                        "bundle_id": activation.get("bundle_id"),
                        "experiment_key": activation.get("experiment_key"),
                        "variant_key": activation.get("variant_key") or activation.get("variant_id"),
                        "placement_key": activation.get("placement_key"),
                        "template_key": activation.get("template_key"),
                    }.items()
                    if value not in (None, "", [])
                },
            }
        )

    return {
        "object": {
            "type": resolved_type,
            "id": resolved_id,
            "match_aliases": resolved_aliases,
        },
        "period": {
            "date_from": date_from,
            "date_to": date_to,
            "conversion_key": conversion_key,
        },
        "total_matches": total_matches,
        "limit": resolved_limit,
        "items": items,
    }


def build_activation_object_registry(
    *,
    journeys: List[Dict[str, Any]],
    object_type: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    resolved_type = _as_text(object_type).lower()
    if resolved_type and resolved_type not in SUPPORTED_ACTIVATION_OBJECT_TYPES:
        raise ValueError(f"Unsupported activation object_type '{object_type}'")
    query = _as_text(q).lower()
    resolved_limit = max(1, min(int(limit or 50), 200))
    objects: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def add_object(
        *,
        typ: str,
        object_id: Any,
        journey: Dict[str, Any],
        touchpoint: Dict[str, Any],
        aliases: Optional[Sequence[Any]] = None,
        label: Optional[str] = None,
    ) -> None:
        oid = _as_text(object_id)
        if not oid:
            return
        if resolved_type and typ != resolved_type:
            return
        activation = _activation_meta(touchpoint)
        haystack = " ".join([typ, oid, _as_text(label), *[_as_text(alias) for alias in aliases or []]]).lower()
        if query and query not in haystack:
            return
        key = (typ, oid)
        item = objects.setdefault(
            key,
            {
                "object_type": typ,
                "object_id": oid,
                "label": _as_text(label) or oid,
                "aliases": set(),
                "matched_touchpoints": 0,
                "journey_ids": set(),
                "profile_ids": set(),
                "converted_journey_ids": set(),
                "revenue": 0.0,
                "source_systems": set(),
                "last_touchpoint_at": None,
            },
        )
        item["matched_touchpoints"] += 1
        journey_id = _journey_id(journey)
        if journey_id:
            item["journey_ids"].add(journey_id)
        profile_id = _profile_id(journey)
        if profile_id:
            item["profile_ids"].add(profile_id)
        if _journey_is_converted(journey) and journey_id and journey_id not in item["converted_journey_ids"]:
            item["converted_journey_ids"].add(journey_id)
            item["revenue"] += float(journey_revenue_value(journey) or 0.0)
        for alias in aliases or []:
            text = _as_text(alias)
            if text and text != oid:
                item["aliases"].add(text)
        source_system = _as_text(activation.get("source_system"))
        if source_system:
            item["source_systems"].add(source_system)
        touchpoint_at = _as_text(touchpoint.get("ts") or touchpoint.get("timestamp"))
        if touchpoint_at and (not item["last_touchpoint_at"] or touchpoint_at > item["last_touchpoint_at"]):
            item["last_touchpoint_at"] = touchpoint_at

    for journey, touchpoint, _index in _iter_touchpoints(journeys):
        activation = _activation_meta(touchpoint)
        campaign_id = activation.get("activation_campaign_id") or activation.get("campaign_id") or touchpoint.get("campaign_id") or _touchpoint_campaign_name(touchpoint)
        add_object(
            typ="campaign",
            object_id=campaign_id,
            journey=journey,
            touchpoint=touchpoint,
            aliases=[activation.get("native_meiro_campaign_id"), touchpoint.get("campaign_id"), _touchpoint_campaign_name(touchpoint)],
            label=_touchpoint_campaign_name(touchpoint) or _as_text(campaign_id),
        )
        add_object(
            typ="asset",
            object_id=activation.get("creative_asset_id") or activation.get("native_meiro_asset_id") or activation.get("asset_id"),
            journey=journey,
            touchpoint=touchpoint,
            aliases=[activation.get("asset_id"), activation.get("native_meiro_asset_id"), activation.get("offer_id"), activation.get("content_block_id"), activation.get("bundle_id")],
        )
        add_object(
            typ="offer",
            object_id=activation.get("offer_id"),
            journey=journey,
            touchpoint=touchpoint,
        )
        add_object(
            typ="content",
            object_id=activation.get("content_block_id"),
            journey=journey,
            touchpoint=touchpoint,
            aliases=[activation.get("creative_asset_id"), activation.get("native_meiro_asset_id")],
        )
        add_object(
            typ="bundle",
            object_id=activation.get("bundle_id") or activation.get("offer_catalog_id") or activation.get("native_meiro_catalog_id"),
            journey=journey,
            touchpoint=touchpoint,
            aliases=[activation.get("offer_catalog_id"), activation.get("native_meiro_catalog_id")],
        )
        for typ, field in [
            ("decision", "decision_key"),
            ("decision_stack", "decision_stack_key"),
            ("experiment", "experiment_key"),
            ("variant", "variant_key"),
            ("placement", "placement_key"),
            ("template", "template_key"),
        ]:
            add_object(
                typ=typ,
                object_id=activation.get(field),
                journey=journey,
                touchpoint=touchpoint,
            )

    items = []
    for item in objects.values():
        items.append(
            {
                "object_type": item["object_type"],
                "object_id": item["object_id"],
                "label": item["label"],
                "aliases": sorted(item["aliases"]),
                "matched_touchpoints": item["matched_touchpoints"],
                "matched_journeys": len(item["journey_ids"]),
                "matched_profiles": len(item["profile_ids"]),
                "conversions": len(item["converted_journey_ids"]),
                "revenue": round(float(item["revenue"] or 0.0), 6),
                "source_systems": sorted(item["source_systems"]),
                "last_touchpoint_at": item["last_touchpoint_at"],
            }
        )
    items.sort(key=lambda item: (-(item["matched_touchpoints"] or 0), item["object_type"], item["object_id"]))
    return {"items": items[:resolved_limit], "total": len(items), "limit": resolved_limit}


def build_activation_feedback_recommendations(
    *,
    journeys: List[Dict[str, Any]],
    limit: int = 10,
) -> Dict[str, Any]:
    resolved_limit = max(1, min(int(limit or 10), 50))
    registry = build_activation_object_registry(journeys=journeys, limit=200)
    candidates = []
    for item in registry.get("items") or []:
        matched_journeys = int(item.get("matched_journeys") or 0)
        conversions = int(item.get("conversions") or 0)
        revenue = float(item.get("revenue") or 0.0)
        matched_touchpoints = int(item.get("matched_touchpoints") or 0)
        conversion_rate = conversions / matched_journeys if matched_journeys else 0.0
        source_systems = item.get("source_systems") or []
        object_type = item.get("object_type")

        if conversions > 0:
            recommendation = "compare"
            status = "ready"
            title = f"Compare {object_type} performance in MMM/MTA"
            reason = "This activation object has conversion evidence in imported journeys."
            action_label = "Use as measured activation input"
        elif matched_touchpoints >= 2:
            recommendation = "review"
            status = "warning"
            title = f"Review {object_type} before scaling"
            reason = "This activation object has repeated delivery evidence but no matched conversions yet."
            action_label = "Inspect audience, asset, and decision fit"
        else:
            recommendation = "collect_more_data"
            status = "setup"
            title = f"Collect more {object_type} evidence"
            reason = "This activation object has too little journey evidence for a reliable decision."
            action_label = "Keep collecting activation events"

        if not source_systems:
            status = "warning"
            reason = "Activation metadata is present, but the source system is missing; verify event mapping before using this object in decisions."

        candidates.append(
            {
                "object": {
                    "type": object_type,
                    "id": item.get("object_id"),
                    "label": item.get("label"),
                    "aliases": item.get("aliases") or [],
                    "source_systems": source_systems,
                },
                "recommendation": recommendation,
                "status": status,
                "title": title,
                "reason": reason,
                "action": {
                    "id": f"{recommendation}:{object_type}:{item.get('object_id')}",
                    "label": action_label,
                    "target": "activation_measurement",
                },
                "evidence": {
                    "matched_touchpoints": matched_touchpoints,
                    "matched_journeys": matched_journeys,
                    "matched_profiles": int(item.get("matched_profiles") or 0),
                    "conversions": conversions,
                    "conversion_rate": round(conversion_rate, 6),
                    "revenue": round(revenue, 6),
                    "last_touchpoint_at": item.get("last_touchpoint_at"),
                },
                "score": round((conversions * 4.0) + (conversion_rate * 3.0) + min(matched_touchpoints, 20) * 0.1 + min(revenue / 1000.0, 5.0), 6),
            }
        )

    candidates.sort(key=lambda item: (-float(item.get("score") or 0.0), item["object"]["type"], item["object"]["id"]))
    selected = candidates[:resolved_limit]
    ready_count = sum(1 for item in selected if item.get("status") == "ready")
    warning_count = sum(1 for item in selected if item.get("status") == "warning")
    if not candidates:
        decision = {
            "status": "blocked",
            "subtitle": "No activation objects are available from the current journey source.",
            "blockers": ["Import deciEngine activation events or replay Prism events before building feedback actions."],
            "warnings": [],
            "actions": [
                {
                    "id": "import-activation-events",
                    "label": "Import activation events",
                    "benefit": "Creates measurable campaign, asset, offer, and decision objects for MMM/MTA feedback.",
                    "domain": "measurement",
                    "target_page": "meiro",
                }
            ],
        }
    else:
        decision = {
            "status": "ready" if ready_count else "warning",
            "subtitle": "Activation feedback links deciEngine/Prism objects to journey evidence before using them in MMM/MTA decisions.",
            "blockers": [],
            "warnings": [
                f"{warning_count} activation object{'s' if warning_count != 1 else ''} need review before scaling."
            ] if warning_count else [],
            "actions": [
                {
                    "id": "review-activation-feedback",
                    "label": "Review activation feedback",
                    "benefit": "Prioritize measured campaigns, assets, offers, and decisions before creating budget or content actions.",
                    "domain": "measurement",
                    "target_page": "meiro",
                }
            ],
        }

    return {
        "items": selected,
        "total": len(candidates),
        "limit": resolved_limit,
        "summary": {
            "ready": ready_count,
            "warning": warning_count,
            "setup": sum(1 for item in selected if item.get("status") == "setup"),
        },
        "decision": decision,
    }


def build_activation_feedback_export(
    *,
    journeys: List[Dict[str, Any]],
    limit: int = 50,
    generated_by: str = "mmm",
) -> Dict[str, Any]:
    feedback = build_activation_feedback_recommendations(journeys=journeys, limit=limit)
    signals: List[Dict[str, Any]] = []
    for item in feedback.get("items") or []:
        obj = item.get("object") or {}
        evidence = item.get("evidence") or {}
        recommendation = item.get("recommendation") or "review"
        object_type = _as_text(obj.get("type"))
        object_id = _as_text(obj.get("id"))
        signal_id = f"activation_feedback:{object_type}:{object_id}"
        signals.append(
            {
                "signal_id": signal_id,
                "object": {
                    "type": object_type,
                    "id": object_id,
                    "label": obj.get("label") or object_id,
                    "aliases": list(obj.get("aliases") or []),
                    "source_systems": list(obj.get("source_systems") or []),
                },
                "recommendation": recommendation,
                "status": item.get("status"),
                "title": item.get("title"),
                "reason": item.get("reason"),
                "metrics": {
                    "matched_touchpoints": int(evidence.get("matched_touchpoints") or 0),
                    "matched_journeys": int(evidence.get("matched_journeys") or 0),
                    "matched_profiles": int(evidence.get("matched_profiles") or 0),
                    "conversions": int(evidence.get("conversions") or 0),
                    "conversion_rate": float(evidence.get("conversion_rate") or 0.0),
                    "revenue": float(evidence.get("revenue") or 0.0),
                    "last_touchpoint_at": evidence.get("last_touchpoint_at"),
                },
                "decision_engine_hint": {
                    "suggested_action": {
                        "compare": "prioritize_for_measurement_review",
                        "review": "inspect_before_scaling",
                        "collect_more_data": "keep_collecting_events",
                    }.get(recommendation, "review"),
                    "eligible_for_policy_input": recommendation == "compare",
                    "requires_human_review": recommendation != "compare",
                },
            }
        )

    return {
        "schema_version": "activation_feedback_export.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_by": generated_by,
        "source": {
            "system": "meiro_mmm_app",
            "journey_source": "activation_measurement",
            "intended_consumers": ["deciEngine", "Meiro Prism", "MMM/MTA"],
        },
        "summary": {
            **dict(feedback.get("summary") or {}),
            "signals": len(signals),
            "total_candidates": int(feedback.get("total") or 0),
        },
        "decision": dict(feedback.get("decision") or {}),
        "signals": signals,
    }


def _load_activation_feedback_export_runs() -> List[Dict[str, Any]]:
    if not ACTIVATION_FEEDBACK_EXPORTS_FILE.exists():
        return []
    try:
        payload = json.loads(ACTIVATION_FEEDBACK_EXPORTS_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []
    return payload if isinstance(payload, list) else []


def _save_activation_feedback_export_runs(runs: List[Dict[str, Any]]) -> None:
    ACTIVATION_FEEDBACK_EXPORTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    ACTIVATION_FEEDBACK_EXPORTS_FILE.write_text(
        json.dumps(runs[:MAX_ACTIVATION_FEEDBACK_EXPORT_RUNS], indent=2, default=str),
        encoding="utf-8",
    )


def record_activation_feedback_export(
    *,
    journeys: List[Dict[str, Any]],
    limit: int = 50,
    generated_by: str = "mmm",
) -> Dict[str, Any]:
    payload = build_activation_feedback_export(journeys=journeys, limit=limit, generated_by=generated_by)
    export_id = f"afe_{uuid.uuid4().hex[:12]}"
    run = {
        "id": export_id,
        "created_at": payload.get("generated_at"),
        "created_by": generated_by,
        "schema_version": payload.get("schema_version"),
        "summary": dict(payload.get("summary") or {}),
        "decision": dict(payload.get("decision") or {}),
        "payload": payload,
    }
    runs = _load_activation_feedback_export_runs()
    runs.insert(0, run)
    _save_activation_feedback_export_runs(runs)
    return run


def list_activation_feedback_exports(limit: int = 20) -> Dict[str, Any]:
    resolved_limit = max(1, min(int(limit or 20), MAX_ACTIVATION_FEEDBACK_EXPORT_RUNS))
    runs = _load_activation_feedback_export_runs()
    items = [
        {
            "id": run.get("id"),
            "created_at": run.get("created_at"),
            "created_by": run.get("created_by"),
            "schema_version": run.get("schema_version"),
            "summary": dict(run.get("summary") or {}),
            "decision": dict(run.get("decision") or {}),
        }
        for run in runs[:resolved_limit]
    ]
    return {"items": items, "total": len(runs), "limit": resolved_limit}
