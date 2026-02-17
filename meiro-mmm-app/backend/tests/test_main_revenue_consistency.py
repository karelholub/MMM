from app.main import compute_campaign_trends, _compute_total_converted_value_for_period


def test_compute_campaign_trends_uses_revenue_entries_dedup():
    journeys = [
        {
            "converted": True,
            "touchpoints": [{"timestamp": "2026-02-01T10:00:00Z", "channel": "google_ads", "campaign": "Brand"}],
            "_revenue_entries": [
                {"dedup_key": "x-1", "value_in_base": 120.0},
                {"dedup_key": "x-1", "value_in_base": 999.0},
            ],
        }
    ]
    out = compute_campaign_trends(journeys)
    points = out["series"]["google_ads:Brand"]
    assert points[0]["revenue"] == 120.0


def test_total_converted_value_period_uses_revenue_entries_dedup():
    journeys = [
        {
            "converted": True,
            "touchpoints": [{"timestamp": "2026-02-01T10:00:00Z", "channel": "google_ads"}],
            "_revenue_entries": [
                {"dedup_key": "conv-1", "value_in_base": 100.0},
                {"dedup_key": "conv-1", "value_in_base": 100.0},
            ],
        },
        {
            "converted": True,
            "touchpoints": [{"timestamp": "2026-02-01T11:00:00Z", "channel": "google_ads"}],
            "_revenue_entries": [
                {"dedup_key": "conv-2", "value_in_base": 50.0},
            ],
        },
    ]
    total = _compute_total_converted_value_for_period(
        journeys=journeys,
        date_from="2026-02-01",
        date_to="2026-02-01",
        timezone_name="UTC",
        channels=["google_ads"],
        conversion_key=None,
    )
    assert total == 150.0
