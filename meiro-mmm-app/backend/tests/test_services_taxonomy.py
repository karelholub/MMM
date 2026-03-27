from app.services_conversions import v2_to_legacy
from app.services_taxonomy import compute_unknown_share, map_to_channel, normalize_touchpoint_with_confidence
from app.services_taxonomy_decisions import build_taxonomy_overview
from app.utils.taxonomy import normalize_touchpoint


def test_map_to_channel_returns_match_for_active_rule_conditions():
    mapping = map_to_channel("google", "cpc")

    assert mapping.channel == "paid_search"
    assert mapping.confidence == 1.0


def test_compute_unknown_share_reads_nested_utm_fields():
    journeys = [
        {
            "touchpoints": [
                {
                    "utm": {
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "brand",
                    }
                }
            ]
        }
    ]

    report = compute_unknown_share(journeys)

    assert report.total_touchpoints == 1
    assert report.unknown_count == 0
    assert report.unknown_share == 0.0


def test_compute_unknown_share_on_v2_legacy_roundtrip_preserves_utm_mapping():
    v2_journey = {
        "_schema": "v2",
        "customer": {"id": "cust-1"},
        "touchpoints": [
            {
                "channel": "paid_search",
                "ts": "2026-03-01T00:00:00Z",
                "source": "google",
                "medium": "cpc",
                "campaign": {"name": "brand"},
                "utm": {"source": "google", "medium": "cpc", "campaign": "brand"},
            }
        ],
        "conversions": [{"name": "purchase", "value": 99.0}],
    }

    report = compute_unknown_share([v2_to_legacy(v2_journey)])

    assert report.total_touchpoints == 1
    assert report.unknown_count == 0
    assert report.unknown_share == 0.0


def test_build_taxonomy_overview_handles_dict_shaped_campaign_fields():
    v2_journey = {
        "_schema": "v2",
        "customer": {"id": "cust-2"},
        "touchpoints": [
            {
                "channel": "paid_search",
                "ts": "2026-03-01T00:00:00Z",
                "source": "google",
                "medium": "cpc",
                "campaign": {"name": "brand"},
                "utm": {"source": "google", "medium": "cpc", "campaign": "brand"},
            }
        ],
        "conversions": [{"name": "purchase", "value": 99.0}],
    }

    overview = build_taxonomy_overview([v2_to_legacy(v2_journey)], suggestion_count=0)

    assert overview["summary"]["unknown_share"] == 0.0
    assert overview["summary"]["unknown_count"] == 0


def test_normalize_touchpoint_infers_organic_from_external_search_referrer():
    normalized = normalize_touchpoint(
        {
            "page_location": "https://www.copygeneral.cz/brno",
            "page_referrer": "https://www.google.com/",
        }
    )

    assert normalized["source"] == "google"
    assert normalized["medium"] == "organic"
    assert normalized["meta"]["inferred_source_medium_from_referrer"] is True


def test_normalize_touchpoint_ignores_self_referrals():
    normalized = normalize_touchpoint(
        {
            "page_location": "https://www.copygeneral.cz/brno",
            "page_referrer": "https://www.copygeneral.cz/kontakty",
        }
    )

    assert "source" not in normalized
    assert "medium" not in normalized
    assert normalized.get("meta", {}).get("inferred_source_medium_from_referrer") is None


def test_normalize_touchpoint_with_confidence_marks_referrer_inference():
    normalized, confidence = normalize_touchpoint_with_confidence(
        {
            "page_location": "https://www.copygeneral.cz/brno",
            "page_referrer": "https://www.google.com/",
        }
    )

    assert normalized["source"] == "google"
    assert normalized["medium"] == "organic"
    assert normalized["_inference"]["source_medium_from_referrer"] is True
    assert confidence > 0
