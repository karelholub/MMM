from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models_config_dq import ConversionTaxonomyTouchpointFact
from app.services_canonical_facts import count_canonical_conversions
from app.services_taxonomy import compute_taxonomy_coverage, compute_unknown_share
from app.utils.taxonomy import Taxonomy, load_taxonomy


SOURCE_HINTS = {
    "fb": "facebook",
    "facebook_ads": "facebook",
    "facebook.com": "facebook",
    "ig": "instagram",
    "insta": "instagram",
    "meta_ads": "meta",
    "ln": "linkedin",
    "li": "linkedin",
    "newsletter": "email",
    "wa": "whatsapp",
}

MEDIUM_HINTS = {
    "paid": "cpc",
    "paid_social": "social",
    "social_paid": "social",
    "ppc": "cpc",
    "e-mail": "email",
    "mail": "email",
}

RAW_CHANNEL_TO_CANONICAL = {
    "meta_ads": "paid_social",
    "facebook": "paid_social",
    "instagram": "paid_social",
    "ig": "paid_social",
    "linkedin_ads": "paid_social",
    "linkedin": "paid_social",
    "pinterest": "paid_social",
    "google_ads": "paid_search",
    "bing": "paid_search",
    "seznam": "paid_search",
    "mapy.com": "referral",
    "chatgpt.com": "referral",
    "home_page": "direct",
    "direct": "direct",
}

SOURCE_ONLY_CHANNEL_HINTS = {
    "google": "organic_search",
    "bing": "organic_search",
    "duckduckgo": "organic_search",
    "yahoo": "organic_search",
    "seznam": "organic_search",
    "chatgpt.com": "referral",
    "facebook": "referral",
    "instagram": "referral",
    "linkedin": "referral",
}


def _normalize_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")


def _infer_channel(source: str, medium: str) -> Optional[str]:
    source_token = _normalize_token(source)
    medium_token = _normalize_token(medium)

    if medium_token in {"email"} or source_token in {"email", "newsletter"}:
        return "email"
    if medium_token in {"cpc", "ppc", "paid_search"} and source_token in {"google", "bing", "baidu"}:
        return "paid_search"
    if source_token in {"facebook", "instagram", "meta", "linkedin", "twitter", "tiktok"} and medium_token in {"cpc", "social", "paid_social", "paid"}:
        return "paid_social"
    if medium_token in {"display", "banner"}:
        return "display"
    if medium_token in {"affiliate"}:
        return "affiliate"
    if medium_token in {"referral"}:
        return "referral"
    if medium_token in {"direct", "none"}:
        return "direct"
    return None


def _confidence(score: float) -> Dict[str, Any]:
    band = "high" if score >= 0.85 else "medium" if score >= 0.6 else "low"
    return {"score": round(score, 3), "band": band}


def generate_taxonomy_suggestions(
    journeys: List[Dict[str, Any]],
    taxonomy: Optional[Taxonomy] = None,
    limit: int = 12,
) -> Dict[str, Any]:
    taxonomy = taxonomy or load_taxonomy()
    unknown_report = compute_unknown_share(journeys, taxonomy=taxonomy, sample_size=min(limit, 20))
    coverage = compute_taxonomy_coverage(journeys, taxonomy=taxonomy)

    suggestions: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    total_touchpoints = max(1, int(unknown_report.total_touchpoints or 0))

    for idx, pattern in enumerate(coverage.get("top_unmapped_patterns", [])):
        if len(suggestions) >= limit:
            break
        source = (pattern.get("source") or "").strip().lower()
        medium = (pattern.get("medium") or "").strip().lower()
        campaign = (pattern.get("campaign") or "").strip().lower()
        count = int(pattern.get("count") or 0)
        if count <= 0:
            continue

        if source and source not in taxonomy.source_aliases:
            canonical = SOURCE_HINTS.get(source)
            if canonical and canonical != source:
                suggestion_id = f"source_alias:{source}:{canonical}"
                if suggestion_id not in seen_ids:
                    seen_ids.add(suggestion_id)
                    suggestions.append(
                        {
                            "id": suggestion_id,
                            "type": "source_alias",
                            "title": f"Normalize source '{source}' to '{canonical}'",
                            "description": "Observed raw source looks like a noisy variant of an existing canonical source.",
                            "confidence": _confidence(0.93),
                            "impact_count": count,
                            "estimated_unknown_share_delta": count / total_touchpoints,
                            "channel": None,
                            "reasons": ["source_alias_hint", "high_volume_unmapped_pattern"],
                            "recommended_action": "Apply alias to the draft and recheck unresolved traffic.",
                            "sample": {"source": source, "medium": medium, "campaign": campaign or None},
                            "payload": {"source_alias": {"raw": source, "canonical": canonical}},
                        }
                    )
                    if len(suggestions) >= limit:
                        break

        if medium and medium not in taxonomy.medium_aliases:
            canonical = MEDIUM_HINTS.get(medium)
            if canonical and canonical != medium:
                suggestion_id = f"medium_alias:{medium}:{canonical}"
                if suggestion_id not in seen_ids:
                    seen_ids.add(suggestion_id)
                    suggestions.append(
                        {
                            "id": suggestion_id,
                            "type": "medium_alias",
                            "title": f"Normalize medium '{medium}' to '{canonical}'",
                            "description": "Observed raw medium is a strong candidate for alias normalization.",
                            "confidence": _confidence(0.9),
                            "impact_count": count,
                            "estimated_unknown_share_delta": count / total_touchpoints,
                            "channel": None,
                            "reasons": ["medium_alias_hint", "high_volume_unmapped_pattern"],
                            "recommended_action": "Apply alias to the draft and review coverage improvement.",
                            "sample": {"source": source, "medium": medium, "campaign": campaign or None},
                            "payload": {"medium_alias": {"raw": medium, "canonical": canonical}},
                        }
                    )
                    if len(suggestions) >= limit:
                        break

        if source and not medium:
            hinted_channel = SOURCE_ONLY_CHANNEL_HINTS.get(taxonomy.source_aliases.get(source, source))
            if hinted_channel:
                exists = any(
                    rule.channel == hinted_channel
                    and rule.source.normalize_operator() == "equals"
                    and (rule.source.value or "").strip().lower() == (taxonomy.source_aliases.get(source, source) or "").strip().lower()
                    and rule.medium.normalize_operator() == "any"
                    for rule in taxonomy.channel_rules
                )
                if not exists:
                    suggestion_id = f"source_only_rule:{source}:{hinted_channel}"
                    if suggestion_id not in seen_ids:
                        seen_ids.add(suggestion_id)
                        suggestions.append(
                            {
                                "id": suggestion_id,
                                "type": "channel_rule",
                                "title": f"Map source '{source}' to {hinted_channel} when medium is missing",
                                "description": "This source often arrives without medium information. A source-only fallback rule would reduce unknown traffic in a controlled way.",
                                "confidence": _confidence(0.72),
                                "impact_count": count,
                                "estimated_unknown_share_delta": count / total_touchpoints,
                                "channel": hinted_channel,
                                "reasons": ["source_only_pattern", "missing_medium_fallback"],
                                "recommended_action": "Apply the fallback rule to the draft and verify the preview impact before saving.",
                                "sample": {"source": source, "medium": medium, "campaign": campaign or None},
                                "payload": {
                                    "channel_rule": {
                                        "name": f"{hinted_channel.replace('_', ' ').title()} - {source}",
                                        "channel": hinted_channel,
                                        "source": {"operator": "equals", "value": taxonomy.source_aliases.get(source, source)},
                                        "medium": {"operator": "any", "value": ""},
                                        "campaign": {"operator": "any", "value": ""},
                                    }
                                },
                            }
                        )
                        if len(suggestions) >= limit:
                            break

        channel = _infer_channel(taxonomy.source_aliases.get(source, source), taxonomy.medium_aliases.get(medium, medium))
        if channel:
            exists = any(
                rule.channel == channel
                and rule.source.normalize_operator() == "equals"
                and (rule.source.value or "").strip().lower() == (taxonomy.source_aliases.get(source, source) or "").strip().lower()
                and rule.medium.normalize_operator() == "equals"
                and (rule.medium.value or "").strip().lower() == (taxonomy.medium_aliases.get(medium, medium) or "").strip().lower()
                for rule in taxonomy.channel_rules
            )
            if not exists:
                suggestion_id = f"rule:{source}:{medium}:{channel}"
                if suggestion_id not in seen_ids:
                    seen_ids.add(suggestion_id)
                    suggestions.append(
                        {
                            "id": suggestion_id,
                            "type": "channel_rule",
                            "title": f"Map {source or '—'} / {medium or '—'} to {channel}",
                            "description": "This high-volume source/medium pattern is currently unresolved and looks stable enough for a direct channel rule.",
                            "confidence": _confidence(0.78),
                            "impact_count": count,
                            "estimated_unknown_share_delta": count / total_touchpoints,
                            "channel": channel,
                            "reasons": ["source_medium_pattern", "channel_inference"],
                            "recommended_action": "Apply the rule to the draft and verify the preview delta.",
                            "sample": {"source": source, "medium": medium, "campaign": campaign or None},
                            "payload": {
                                "channel_rule": {
                                    "name": f"{channel.replace('_', ' ').title()} - {source or medium}",
                                    "channel": channel,
                                    "source": {"operator": "equals", "value": taxonomy.source_aliases.get(source, source)},
                                    "medium": {"operator": "equals", "value": taxonomy.medium_aliases.get(medium, medium)},
                                    "campaign": {"operator": "any", "value": ""},
                                }
                            },
                        }
                    )

    for (source, medium), count in sorted(unknown_report.by_source_medium.items(), key=lambda item: -item[1]):
        if len(suggestions) >= limit:
            break
        source = (source or "").strip().lower()
        medium = (medium or "").strip().lower()
        if not source or medium:
            continue
        hinted_channel = SOURCE_ONLY_CHANNEL_HINTS.get(taxonomy.source_aliases.get(source, source))
        if not hinted_channel:
            continue
        exists = any(
            rule.channel == hinted_channel
            and rule.source.normalize_operator() == "equals"
            and (rule.source.value or "").strip().lower() == (taxonomy.source_aliases.get(source, source) or "").strip().lower()
            and rule.medium.normalize_operator() == "any"
            for rule in taxonomy.channel_rules
        )
        if exists:
            continue
        suggestion_id = f"source_only_rule:{source}:{hinted_channel}"
        if suggestion_id in seen_ids:
            continue
        seen_ids.add(suggestion_id)
        suggestions.append(
            {
                "id": suggestion_id,
                "type": "channel_rule",
                "title": f"Map source '{source}' to {hinted_channel} when medium is missing",
                "description": "This source often arrives without medium information. A source-only fallback rule would reduce unknown traffic in a controlled way.",
                "confidence": _confidence(0.72),
                "impact_count": count,
                "estimated_unknown_share_delta": count / total_touchpoints,
                "channel": hinted_channel,
                "reasons": ["source_only_pattern", "missing_medium_fallback"],
                "recommended_action": "Apply the fallback rule to the draft and verify the preview impact before saving.",
                "sample": {"source": source, "medium": medium, "campaign": None},
                "payload": {
                    "channel_rule": {
                        "name": f"{hinted_channel.replace('_', ' ').title()} - {source}",
                        "channel": hinted_channel,
                        "source": {"operator": "equals", "value": taxonomy.source_aliases.get(source, source)},
                        "medium": {"operator": "any", "value": ""},
                        "campaign": {"operator": "any", "value": ""},
                    }
                },
            }
        )

    missing_source_campaigns: dict[str, Counter[str]] = defaultdict(Counter)
    for journey in journeys:
        for tp in journey.get("touchpoints", []):
            source = (tp.get("utm_source") or tp.get("source") or "").strip().lower()
            medium = (tp.get("utm_medium") or tp.get("medium") or "").strip().lower()
            if source or medium:
                continue
            campaign = str(tp.get("utm_campaign") or tp.get("campaign") or "").strip()
            raw_channel = str(tp.get("channel") or "").strip().lower()
            if not campaign or campaign.lower() == "not_set" or not raw_channel:
                continue
            missing_source_campaigns[campaign][raw_channel] += 1

    for campaign, counts in sorted(missing_source_campaigns.items(), key=lambda item: -sum(item[1].values())):
        if len(suggestions) >= limit:
            break
        total = sum(counts.values())
        raw_channel, count = counts.most_common(1)[0]
        dominance = count / total if total else 0.0
        if count < 3 or dominance < 0.85:
            continue
        channel = RAW_CHANNEL_TO_CANONICAL.get(raw_channel)
        if not channel:
            continue
        if channel == "direct" and (count < 25 or dominance < 0.97):
            continue
        exists = any(
            rule.channel == channel
            and rule.campaign.normalize_operator() in {"equals", "contains"}
            and (rule.campaign.value or "").strip().lower() == campaign.strip().lower()
            for rule in taxonomy.channel_rules
        )
        if exists:
            continue
        suggestion_id = f"campaign_rule:{campaign}:{channel}"
        if suggestion_id in seen_ids:
            continue
        seen_ids.add(suggestion_id)
        suggestions.append(
            {
                "id": suggestion_id,
                "type": "channel_rule",
                "title": f"Map campaign '{campaign}' to {channel}",
                "description": f"Source and medium are missing, but this campaign almost always arrives with raw channel '{raw_channel}'.",
                "confidence": _confidence(min(0.95, 0.65 + dominance * 0.3)),
                "impact_count": count,
                "estimated_unknown_share_delta": count / total_touchpoints,
                "channel": channel,
                "reasons": ["campaign_consistency", "missing_source_medium_fallback"],
                "recommended_action": "Apply the campaign rule to the draft and confirm the preview impact.",
                "sample": {"source": "", "medium": "", "campaign": campaign},
                "payload": {
                    "channel_rule": {
                        "name": f"{channel.replace('_', ' ').title()} - {campaign[:32]}",
                        "channel": channel,
                        "source": {"operator": "any", "value": ""},
                        "medium": {"operator": "any", "value": ""},
                        "campaign": {"operator": "equals", "value": campaign},
                    }
                },
            }
        )

    return {
        "summary": {
            "unknown_share": unknown_report.unknown_share,
            "unknown_count": unknown_report.unknown_count,
            "total_touchpoints": unknown_report.total_touchpoints,
            "source_coverage": coverage.get("source_coverage", 0.0),
            "medium_coverage": coverage.get("medium_coverage", 0.0),
            "active_rules": len([rule for rule in taxonomy.channel_rules if rule.enabled]),
            "source_aliases": len(taxonomy.source_aliases),
            "medium_aliases": len(taxonomy.medium_aliases),
        },
        "suggestions": sorted(
            suggestions[:limit],
            key=lambda item: (
                -float(item.get("estimated_unknown_share_delta") or 0),
                -float((item.get("confidence") or {}).get("score") or 0),
                -int(item.get("impact_count") or 0),
            ),
        ),
    }


def generate_taxonomy_suggestions_from_db(
    db: Session,
    *,
    taxonomy: Optional[Taxonomy] = None,
    limit: int = 12,
    sample_limit: int = 50000,
) -> Optional[Dict[str, Any]]:
    taxonomy = taxonomy or load_taxonomy()
    rows = (
        db.query(ConversionTaxonomyTouchpointFact)
        .order_by(ConversionTaxonomyTouchpointFact.conversion_ts.desc(), ConversionTaxonomyTouchpointFact.ordinal.asc())
        .limit(sample_limit)
        .all()
    )
    if not rows:
        return None
    raw_count = count_canonical_conversions(db, limit=sample_limit)
    observed_conversion_ids = {str(row.conversion_id or "") for row in rows}
    if raw_count > len(observed_conversion_ids):
        return None

    journeys_by_conversion: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        conversion_id = str(row.conversion_id or "")
        journey = journeys_by_conversion.setdefault(conversion_id, {"touchpoints": []})
        journey["touchpoints"].append(
            {
                "channel": row.raw_channel,
                "source": row.source or "",
                "medium": row.medium or "",
                "campaign": row.campaign,
            }
        )

    return generate_taxonomy_suggestions(list(journeys_by_conversion.values()), taxonomy=taxonomy, limit=limit)
