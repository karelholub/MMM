from app.services_data_quality import compute_journeys_completeness
from app.services_quality import ConfidenceComponents, score_confidence


def test_compute_journeys_completeness_basic_counts():
    journeys = [
        {
            "customer_id": "c1",
            "touchpoints": [
                {"channel": "google", "timestamp": "2024-01-01T00:00:00"},
                {"channel": "meta", "timestamp": "2024-01-02T00:00:00"},
            ],
        },
        {
            # Missing customer_id should count towards missing_profile_pct
            "touchpoints": [{"channel": "unknown", "timestamp": ""}],
        },
        {
            "customer_id": "c1",  # duplicate id
            "touchpoints": [{"channel": "direct", "timestamp": "2024-01-03T00:00:00"}],
        },
    ]

    metrics = compute_journeys_completeness(journeys)
    keys = {m[1] for m in metrics}

    assert "missing_profile_pct" in keys
    assert "missing_timestamp_pct" in keys
    assert "missing_channel_pct" in keys
    assert "duplicate_id_pct" in keys
    assert "conversion_attributable_pct" in keys

    missing_profile = next(v for (_, k, v, _) in metrics if k == "missing_profile_pct")
    duplicate_pct = next(v for (_, k, v, _) in metrics if k == "duplicate_id_pct")

    # One of three journeys is missing a profile id
    assert missing_profile > 0
    # Duplicate id present
    assert duplicate_pct > 0


def test_score_confidence_ranges_and_labels():
    high = ConfidenceComponents(
        match_rate=0.95,
        join_rate=0.9,
        missing_rate=0.05,
        freshness_lag_minutes=10.0,
        dedup_rate=0.95,
        consent_share=0.9,
    )
    score_high, label_high = score_confidence(high)
    assert 0.0 <= score_high <= 100.0
    assert label_high in {"high", "medium", "low"}
    assert label_high != "low"

    low = ConfidenceComponents(
        match_rate=0.2,
        join_rate=0.2,
        missing_rate=0.8,
        freshness_lag_minutes=24 * 60.0,  # very stale
        dedup_rate=0.2,
        consent_share=0.2,
    )
    score_low, label_low = score_confidence(low)
    assert 0.0 <= score_low <= 100.0
    assert label_low == "low"
    assert score_low < score_high
