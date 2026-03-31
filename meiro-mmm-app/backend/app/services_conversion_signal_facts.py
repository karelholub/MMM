from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from .models_config_dq import ConversionKpiSignalFact, ConversionTaxonomyTouchpointFact


def _normalized_token(value: Any) -> Optional[str]:
    if value in (None, "", []):
        return None
    return str(value).strip().lower() or None


def _campaign_token(tp: Dict[str, Any]) -> Optional[str]:
    utm = tp.get("utm") if isinstance(tp.get("utm"), dict) else {}
    campaign = tp.get("utm_campaign") or utm.get("campaign") or tp.get("campaign")
    if isinstance(campaign, dict):
        campaign = campaign.get("name") or campaign.get("id")
    return _normalized_token(campaign)


def _source_token(tp: Dict[str, Any]) -> Optional[str]:
    utm = tp.get("utm") if isinstance(tp.get("utm"), dict) else {}
    return _normalized_token(tp.get("utm_source") or utm.get("source") or tp.get("source"))


def _medium_token(tp: Dict[str, Any]) -> Optional[str]:
    utm = tp.get("utm") if isinstance(tp.get("utm"), dict) else {}
    return _normalized_token(tp.get("utm_medium") or utm.get("medium") or tp.get("medium"))


def build_conversion_kpi_signal_fact(
    *,
    journey: Dict[str, Any],
    conversion_id: str,
    profile_id: str,
    conversion_key: Optional[str],
    conversion_ts: datetime,
    created_at: datetime,
) -> ConversionKpiSignalFact:
    event_names: List[str] = []
    for event in journey.get("events") or []:
        name = _normalized_token(event.get("name") or event.get("event_name"))
        if name:
            event_names.append(name)

    conversion_names: List[str] = []
    for conversion in journey.get("conversions") or []:
        name = _normalized_token(
            conversion.get("name")
            or conversion.get("event_name")
            or conversion.get("conversion_key")
        )
        if name:
            conversion_names.append(name)

    kpi_type = _normalized_token(journey.get("kpi_type"))
    normalized_profile_id = str(profile_id or "").strip() or None
    return ConversionKpiSignalFact(
        conversion_id=conversion_id,
        profile_id=normalized_profile_id,
        conversion_key=conversion_key,
        conversion_ts=conversion_ts,
        kpi_type=kpi_type,
        event_names_json=event_names,
        conversion_names_json=conversion_names,
        generic_conversion_fallback=(kpi_type == "conversion"),
        created_at=created_at,
        updated_at=created_at,
    )


def build_conversion_taxonomy_touchpoint_facts(
    *,
    journey: Dict[str, Any],
    conversion_id: str,
    profile_id: str,
    conversion_key: Optional[str],
    conversion_ts: datetime,
    created_at: datetime,
) -> List[ConversionTaxonomyTouchpointFact]:
    normalized_profile_id = str(profile_id or "").strip() or None
    rows: List[ConversionTaxonomyTouchpointFact] = []
    for ordinal, tp in enumerate(journey.get("touchpoints") or []):
        if not isinstance(tp, dict):
            continue
        rows.append(
            ConversionTaxonomyTouchpointFact(
                conversion_id=conversion_id,
                profile_id=normalized_profile_id,
                conversion_key=conversion_key,
                conversion_ts=conversion_ts,
                ordinal=ordinal,
                raw_channel=_normalized_token(tp.get("channel")),
                source=_source_token(tp),
                medium=_medium_token(tp),
                campaign=_campaign_token(tp),
                created_at=created_at,
                updated_at=created_at,
            )
        )
    return rows
