"""Activation-object measurement summaries for governed activation integrations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from app.services_metrics import journey_revenue_value


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
