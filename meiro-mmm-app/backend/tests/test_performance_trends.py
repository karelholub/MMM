from app.services_performance_trends import (
    build_campaign_summary_response,
    build_campaign_trend_response,
    build_channel_summary_response,
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


def test_build_channel_summary_uses_same_source_rollups():
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
    ]
    expenses = [
        {"channel": "google_ads", "service_period_start": "2026-02-01T08:00:00Z", "amount": 80.0},
        {"channel": "google_ads", "service_period_start": "2026-02-02T08:00:00Z", "amount": 20.0},
    ]
    out = build_channel_summary_response(
        journeys=journeys,
        expenses=expenses,
        date_from="2026-02-01",
        date_to="2026-02-02",
        compare=True,
    )
    assert out["items"]
    row = out["items"][0]
    assert row["channel"] == "google_ads"
    assert row["current"]["revenue"] == 150.0
    assert row["current"]["conversions"] == 2.0
    assert row["current"]["spend"] == 100.0
    assert round(row["derived"]["roas"], 4) == 1.5
    assert out["totals"]["current"]["spend"] == 100.0
    assert out["totals"]["current"]["revenue"] == 150.0


def test_build_campaign_summary_shape_and_notes():
    journeys = [
        {
            "converted": True,
            "conversion_value": 120.0,
            "touchpoints": [{"timestamp": "2026-02-01T10:00:00Z", "channel": "meta_ads", "campaign": "Spring"}],
        }
    ]
    expenses = [
        {"channel": "meta_ads", "service_period_start": "2026-02-01T08:00:00Z", "amount": 30.0},
    ]
    out = build_campaign_summary_response(
        journeys=journeys,
        expenses=expenses,
        date_from="2026-02-01",
        date_to="2026-02-02",
        compare=False,
    )
    assert "notes" in out and out["notes"]
    assert out["items"]
    row = out["items"][0]
    assert row["campaign_id"] == "meta_ads:Spring"
    assert row["current"]["revenue"] == 120.0
    assert row["current"]["conversions"] == 1.0
    assert out["totals"]["current"]["spend"] == 30.0
    assert out["totals"]["current"]["revenue"] == 120.0


def test_campaign_summary_spend_is_allocated_without_double_counting():
    journeys = [
        {
            "converted": True,
            "conversion_value": 100.0,
            "touchpoints": [{"timestamp": "2026-02-01T10:00:00Z", "channel": "meta_ads", "campaign": "A"}],
        },
        {
            "converted": True,
            "conversion_value": 300.0,
            "touchpoints": [{"timestamp": "2026-02-01T11:00:00Z", "channel": "meta_ads", "campaign": "B"}],
        },
    ]
    expenses = [
        {"channel": "meta_ads", "service_period_start": "2026-02-01T08:00:00Z", "amount": 80.0},
    ]
    out = build_campaign_summary_response(
        journeys=journeys,
        expenses=expenses,
        date_from="2026-02-01",
        date_to="2026-02-01",
        compare=False,
    )
    rows = {r["campaign_id"]: r for r in out["items"]}
    assert round(rows["meta_ads:A"]["current"]["spend"], 4) == 20.0
    assert round(rows["meta_ads:B"]["current"]["spend"], 4) == 60.0
    assert round(out["totals"]["current"]["spend"], 4) == 80.0


def test_trend_and_summary_respect_conversion_key_filter():
    journeys = [
        {
            "converted": True,
            "kpi_type": "purchase",
            "conversion_value": 100.0,
            "touchpoints": [{"timestamp": "2026-02-01T10:00:00Z", "channel": "google_ads", "campaign": "A"}],
        },
        {
            "converted": True,
            "kpi_type": "signup",
            "conversion_value": 30.0,
            "touchpoints": [{"timestamp": "2026-02-01T11:00:00Z", "channel": "google_ads", "campaign": "B"}],
        },
    ]
    expenses = [{"channel": "google_ads", "service_period_start": "2026-02-01T08:00:00Z", "amount": 50.0}]

    trend = build_campaign_trend_response(
        journeys=journeys,
        expenses=expenses,
        date_from="2026-02-01",
        date_to="2026-02-02",
        kpi_key="revenue",
        compare=False,
        conversion_key="purchase",
    )
    values = [r["value"] for r in trend["series"] if r["campaign_id"] == "google_ads:A" and r["value"] is not None]
    assert values and values[0] == 100.0

    summary = build_campaign_summary_response(
        journeys=journeys,
        expenses=expenses,
        date_from="2026-02-01",
        date_to="2026-02-02",
        compare=False,
        conversion_key="purchase",
    )
    ids = {r["campaign_id"] for r in summary["items"]}
    assert "google_ads:A" in ids
    assert "google_ads:B" not in ids
