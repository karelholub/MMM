from app.services_performance_trends import (
    build_campaign_trend_response,
    build_channel_trend_response,
    resolve_period_windows,
)


def test_resolve_period_windows_previous_and_grain_auto():
    out = resolve_period_windows("2026-02-01", "2026-02-14", grain="auto")
    assert out["current_period"]["date_from"] == "2026-02-01"
    assert out["current_period"]["date_to"] == "2026-02-14"
    assert out["current_period"]["grain"] == "daily"
    assert out["previous_period"]["date_from"] == "2026-01-18"
    assert out["previous_period"]["date_to"] == "2026-01-31"

    out_weekly = resolve_period_windows("2026-01-01", "2026-02-20", grain="auto")
    assert out_weekly["current_period"]["grain"] == "weekly"


def test_build_channel_trend_compare_and_null_buckets():
    journeys = [
        {
            "converted": True,
            "conversion_value": 100.0,
            "touchpoints": [{"timestamp": "2026-02-01T10:00:00Z", "channel": "google_ads"}],
        },
        {
            "converted": True,
            "conversion_value": 50.0,
            "touchpoints": [{"timestamp": "2026-02-02T10:00:00Z", "channel": "google_ads"}],
        },
        {
            "converted": True,
            "conversion_value": 40.0,
            "touchpoints": [{"timestamp": "2026-01-30T10:00:00Z", "channel": "google_ads"}],
        },
    ]
    expenses = [
        {"channel": "google_ads", "service_period_start": "2026-02-01T08:00:00Z", "amount": 80.0},
        {"channel": "google_ads", "service_period_start": "2026-02-02T08:00:00Z", "amount": 20.0},
        {"channel": "google_ads", "service_period_start": "2026-01-30T08:00:00Z", "amount": 30.0},
    ]

    out = build_channel_trend_response(
        journeys=journeys,
        expenses=expenses,
        date_from="2026-02-01",
        date_to="2026-02-02",
        timezone="UTC",
        kpi_key="roas",
        grain="daily",
        compare=True,
    )
    assert out["current_period"]["grain"] == "daily"
    assert out["previous_period"]["date_from"] == "2026-01-30"
    assert out["previous_period"]["date_to"] == "2026-01-31"

    curr = [r for r in out["series"] if r["channel"] == "google_ads"]
    prev = [r for r in out["series_prev"] if r["channel"] == "google_ads"]
    assert len(curr) == 2
    assert len(prev) == 2
    assert curr[0]["ts"] == "2026-02-01" and round(curr[0]["value"], 4) == 1.25
    assert curr[1]["ts"] == "2026-02-02" and round(curr[1]["value"], 4) == 2.5
    assert prev[0]["ts"] == "2026-01-30" and round(prev[0]["value"], 4) == round(40.0 / 30.0, 4)
    assert prev[1]["ts"] == "2026-01-31" and prev[1]["value"] is None


def test_build_campaign_trend_weekly_monday_buckets():
    journeys = [
        {
            "converted": True,
            "conversion_value": 75.0,
            "touchpoints": [
                {
                    "timestamp": "2026-03-04T11:00:00Z",  # Wednesday => week bucket Monday 2026-03-02
                    "channel": "meta_ads",
                    "campaign": "Spring Launch",
                }
            ],
        }
    ]
    expenses = [
        {"channel": "meta_ads", "service_period_start": "2026-03-05T00:00:00Z", "amount": 25.0},
    ]
    out = build_campaign_trend_response(
        journeys=journeys,
        expenses=expenses,
        date_from="2026-03-01",
        date_to="2026-04-20",  # >45 days => weekly
        timezone="UTC",
        kpi_key="revenue",
        grain="auto",
        compare=False,
    )
    assert out["current_period"]["grain"] == "weekly"
    rows = [r for r in out["series"] if r["campaign_id"] == "meta_ads:Spring Launch"]
    assert rows
    assert any(r["ts"] == "2026-03-02" and r["value"] == 75.0 for r in rows)


def test_build_trend_empty_data_returns_empty_series():
    out_channel = build_channel_trend_response(
        journeys=[],
        expenses=[],
        date_from="2026-02-01",
        date_to="2026-02-07",
        kpi_key="spend",
        compare=True,
    )
    assert out_channel["series"] == []
    assert out_channel["series_prev"] == []

    out_campaign = build_campaign_trend_response(
        journeys=[],
        expenses=[],
        date_from="2026-02-01",
        date_to="2026-02-07",
        kpi_key="revenue",
        compare=False,
    )
    assert out_campaign["series"] == []
    assert "series_prev" not in out_campaign

