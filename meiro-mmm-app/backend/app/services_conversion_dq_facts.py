from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from .models_config_dq import ConversionDataQualityFact
from .services_revenue_config import extract_revenue_entries, get_revenue_config
from .utils.taxonomy import load_taxonomy


def build_conversion_dq_fact_row(
    *,
    journey: Dict[str, Any],
    conversion_id: str,
    profile_id: str,
    conversion_key: Optional[str],
    conversion_ts: datetime,
    created_at: datetime,
) -> ConversionDataQualityFact:
    taxonomy = load_taxonomy()
    parser_meta = ((journey.get("meta") or {}).get("parser") or {}) if isinstance(journey.get("meta"), dict) else {}
    touchpoints = [tp for tp in (journey.get("touchpoints") or []) if isinstance(tp, dict)]

    any_ts = False
    any_channel = False
    any_non_direct = False
    unknown_channel_touchpoints = 0
    unresolved_source_medium_touchpoints = 0

    for tp in touchpoints:
        if tp.get("timestamp") or tp.get("ts"):
            any_ts = True
        channel = tp.get("channel")
        if channel:
            any_channel = True
            if channel == "unknown":
                unknown_channel_touchpoints += 1
            if channel not in ("direct", "unknown"):
                any_non_direct = True

        raw_source = str(((tp.get("utm") or {}).get("source") or tp.get("source") or "")).strip().lower()
        raw_medium = str(((tp.get("utm") or {}).get("medium") or tp.get("medium") or "")).strip().lower()
        raw_campaign = tp.get("campaign")
        if isinstance(raw_campaign, dict):
            raw_campaign = raw_campaign.get("name")
        raw_campaign_text = str(raw_campaign or "").strip().lower()
        if raw_source or raw_medium:
            source_norm = taxonomy.source_aliases.get(raw_source, raw_source)
            medium_norm = taxonomy.medium_aliases.get(raw_medium, raw_medium)
            if not any(rule.matches(source_norm, medium_norm, raw_campaign_text) for rule in taxonomy.channel_rules):
                unresolved_source_medium_touchpoints += 1

    revenue_entries = journey.get("_revenue_entries")
    if not isinstance(revenue_entries, list):
        revenue_entries = extract_revenue_entries(
            journey,
            get_revenue_config(),
            fallback_conversion_id=conversion_id,
        )

    revenue_entry_count = 0
    defaulted_revenue_entry_count = 0
    raw_zero_revenue_entry_count = 0
    for entry in revenue_entries:
        if not isinstance(entry, dict):
            continue
        revenue_entry_count += 1
        if bool(entry.get("default_applied")):
            defaulted_revenue_entry_count += 1
        if bool(entry.get("raw_value_zero")):
            raw_zero_revenue_entry_count += 1

    normalized_profile_id = str(profile_id or "").strip()
    return ConversionDataQualityFact(
        conversion_id=conversion_id,
        profile_id=normalized_profile_id or None,
        conversion_key=conversion_key,
        conversion_ts=conversion_ts,
        missing_profile=not bool(normalized_profile_id),
        missing_timestamp=not any_ts,
        missing_channel=not any_channel,
        has_non_direct_touchpoint=bool(any_non_direct),
        used_inferred_mapping=bool(parser_meta.get("used_inferred_mapping")),
        touchpoint_count=len(touchpoints),
        unknown_channel_touchpoints=unknown_channel_touchpoints,
        unresolved_source_medium_touchpoints=unresolved_source_medium_touchpoints,
        revenue_entry_count=revenue_entry_count,
        defaulted_revenue_entry_count=defaulted_revenue_entry_count,
        raw_zero_revenue_entry_count=raw_zero_revenue_entry_count,
        created_at=created_at,
        updated_at=created_at,
    )
