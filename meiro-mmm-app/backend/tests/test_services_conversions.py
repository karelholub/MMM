from app.services_conversions import filter_journeys_by_quality, v2_to_legacy


def test_filter_journeys_by_quality_uses_ingest_quality_score():
    journeys = [
        {"customer_id": "high", "quality_score": 82},
        {"customer_id": "low", "quality_score": 41},
    ]

    filtered = filter_journeys_by_quality(journeys, min_quality_score=50)

    assert [j["customer_id"] for j in filtered] == ["high"]


def test_v2_to_legacy_preserves_quality_metadata():
    journey = {
        "_schema": "v2",
        "customer": {"id": "cust-1"},
        "touchpoints": [
            {
                "channel": "paid",
                "ts": "2026-03-01T00:00:00Z",
                "source": "google",
                "medium": "cpc",
                "campaign": {"name": "brand"},
                "utm": {"source": "google", "medium": "cpc", "campaign": "brand"},
            }
        ],
        "conversions": [{"name": "purchase", "value": 120.0}],
        "meta": {"quality": {"score": 73, "band": "medium", "drivers": ["unknown_channel"]}},
    }

    legacy = v2_to_legacy(journey)

    assert legacy["customer_id"] == "cust-1"
    assert legacy["quality_score"] == 73
    assert legacy["quality_band"] == "medium"
    assert legacy["meta"]["quality"]["drivers"] == ["unknown_channel"]
    assert legacy["touchpoints"][0]["source"] == "google"
    assert legacy["touchpoints"][0]["medium"] == "cpc"
    assert legacy["touchpoints"][0]["utm_source"] == "google"
    assert legacy["touchpoints"][0]["utm_medium"] == "cpc"
    assert legacy["touchpoints"][0]["utm_campaign"] == "brand"
    assert legacy["touchpoints"][0]["utm"] == {"source": "google", "medium": "cpc", "campaign": "brand"}
