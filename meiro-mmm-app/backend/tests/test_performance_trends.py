from datetime import datetime

from app.services_performance_trends import (
    build_campaign_aggregate_overlay,
    build_campaign_summary_response,
    build_campaign_trend_response,
    build_channel_aggregate_overlay,
    build_channel_summary_response,
    build_channel_trend_response,
    resolve_period_windows,
)
from app.db import Base
from app.models_config_dq import ChannelPerformanceDaily, JourneyDefinition, JourneyPathDaily
from app.services_conversions import persist_journeys_as_conversion_paths
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


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
    assert row["current"]["visits"] == 2.0
    assert row["current"]["spend"] == 100.0
    assert round(row["derived"]["roas"], 4) == 1.5
    assert out["totals"]["current"]["spend"] == 100.0
    assert out["totals"]["current"]["visits"] == 2.0
    assert out["totals"]["current"]["revenue"] == 150.0


def test_build_channel_aggregate_overlay_uses_unfiltered_visits_and_keyed_conversions():
    db = _unit_db_session()
    try:
        db.add_all(
            [
                ChannelPerformanceDaily(
                    date=datetime(2026, 2, 1).date(),
                    channel="google_ads",
                    conversion_key=None,
                    visits_total=5,
                    count_conversions=2,
                    gross_conversions_total=2.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=250.0,
                    net_revenue_total=200.0,
                    click_through_conversions_total=1.0,
                    view_through_conversions_total=0.0,
                    mixed_path_conversions_total=0.0,
                    created_at=datetime(2026, 2, 1, 0, 0),
                    updated_at=datetime(2026, 2, 1, 0, 0),
                ),
                ChannelPerformanceDaily(
                    date=datetime(2026, 2, 1).date(),
                    channel="google_ads",
                    conversion_key="purchase",
                    visits_total=0,
                    count_conversions=1,
                    gross_conversions_total=1.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=120.0,
                    net_revenue_total=100.0,
                    click_through_conversions_total=1.0,
                    view_through_conversions_total=0.0,
                    mixed_path_conversions_total=0.0,
                    created_at=datetime(2026, 2, 1, 0, 0),
                    updated_at=datetime(2026, 2, 1, 0, 0),
                ),
            ]
        )
        db.commit()

        overlay = build_channel_aggregate_overlay(
            db,
            date_from="2026-02-01",
            date_to="2026-02-02",
            timezone="UTC",
            compare=False,
            conversion_key="purchase",
        )

        assert overlay is not None
        assert overlay["current_store"]["google_ads"]["2026-02-01"]["visits"] == 5.0
        assert overlay["current_store"]["google_ads"]["2026-02-01"]["conversions"] == 1.0
        assert overlay["current_store"]["google_ads"]["2026-02-01"]["revenue"] == 120.0
        assert overlay["current_outcomes"]["google_ads"]["net_revenue"] == 100.0
    finally:
        db.close()


def test_build_channel_aggregate_overlay_falls_back_to_silver_when_channel_daily_facts_absent():
    db = _unit_db_session()
    try:
        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-prev"},
                    "touchpoints": [{"channel": "email", "interaction_type": "click", "ts": "2026-01-31T10:00:00Z"}],
                    "conversions": [{"id": "conv-prev", "name": "purchase", "ts": "2026-01-31T12:00:00Z", "value": 80.0}],
                },
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-current"},
                    "touchpoints": [{"channel": "google_ads", "interaction_type": "click", "ts": "2026-02-01T10:00:00Z"}],
                    "conversions": [{"id": "conv-current", "name": "purchase", "ts": "2026-02-01T12:00:00Z", "value": 120.0}],
                },
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="silver-overlay-batch",
        )
        assert inserted == 2

        overlay = build_channel_aggregate_overlay(
            db,
            date_from="2026-02-01",
            date_to="2026-02-01",
            timezone="UTC",
            compare=True,
            conversion_key="purchase",
        )

        assert overlay is not None
        assert overlay["current_store"]["google_ads"]["2026-02-01"]["visits"] == 1.0
        assert overlay["current_store"]["google_ads"]["2026-02-01"]["conversions"] == 1.0
        assert overlay["current_store"]["google_ads"]["2026-02-01"]["revenue"] == 120.0
        assert overlay["previous_store"]["email"]["2026-01-31"]["revenue"] == 80.0
        assert overlay["current_outcomes"]["google_ads"]["gross_revenue"] == 120.0
    finally:
        db.close()


def test_build_channel_summary_supports_aggregate_overlay():
    overlay = {
        "current_store": {"google_ads": {"2026-02-01": {"visits": 5.0, "conversions": 2.0, "revenue": 250.0}}},
        "previous_store": {},
        "current_outcomes": {
            "google_ads": {
                "gross_conversions": 2.0,
                "net_conversions": 1.0,
                "gross_revenue": 250.0,
                "net_revenue": 200.0,
                "refunded_value": 0.0,
                "cancelled_value": 0.0,
                "invalid_leads": 0.0,
                "valid_leads": 1.0,
                "click_through_conversions": 1.0,
                "view_through_conversions": 0.0,
                "mixed_path_conversions": 0.0,
            }
        },
        "previous_outcomes": {},
    }
    expenses = [{"channel": "google_ads", "service_period_start": "2026-02-01T08:00:00Z", "amount": 40.0}]

    out = build_channel_summary_response(
        journeys=[],
        expenses=expenses,
        date_from="2026-02-01",
        date_to="2026-02-01",
        compare=False,
        aggregate_overlay=overlay,
    )

    row = out["items"][0]
    assert row["channel"] == "google_ads"
    assert row["current"]["visits"] == 5.0
    assert row["current"]["conversions"] == 2.0
    assert row["current"]["revenue"] == 250.0
    assert row["current"]["spend"] == 40.0
    assert row["outcomes"]["current"]["net_revenue"] == 200.0


def test_build_channel_trend_supports_aggregate_overlay():
    overlay = {
        "current_store": {"google_ads": {"2026-02-01": {"visits": 5.0, "conversions": 2.0, "revenue": 250.0}}},
        "previous_store": {"google_ads": {"2026-01-31": {"visits": 4.0, "conversions": 1.0, "revenue": 100.0}}},
        "current_outcomes": {},
        "previous_outcomes": {},
    }

    out = build_channel_trend_response(
        journeys=[],
        expenses=[],
        date_from="2026-02-01",
        date_to="2026-02-01",
        timezone="UTC",
        kpi_key="revenue",
        grain="daily",
        compare=True,
        aggregate_overlay=overlay,
    )

    assert out["series"] == [{"ts": "2026-02-01", "channel": "google_ads", "value": 250.0}]
    assert out["series_prev"] == [{"ts": "2026-01-31", "channel": "google_ads", "value": 100.0}]


def test_build_channel_summary_exposes_outcomes_and_notes():
    journeys = [
        {
            "converted": True,
            "interaction_path_type": "view_through",
            "_revenue_entries": [
                {
                    "dedup_key": "order:1",
                    "value_in_base": 120.0,
                    "gross_value": 120.0,
                    "net_value": 80.0,
                    "gross_conversions": 1.0,
                    "net_conversions": 1.0,
                    "refunded_value": 40.0,
                    "cancelled_value": 0.0,
                    "invalid_leads": 0.0,
                    "valid_leads": 1.0,
                }
            ],
            "touchpoints": [{"timestamp": "2026-02-01T10:00:00Z", "channel": "google_ads", "interaction_type": "impression"}],
        },
        {
            "converted": True,
            "interaction_path_type": "click_through",
            "_revenue_entries": [
                {
                    "dedup_key": "lead:1",
                    "value_in_base": 0.0,
                    "gross_value": 0.0,
                    "net_value": 0.0,
                    "gross_conversions": 1.0,
                    "net_conversions": 0.0,
                    "refunded_value": 0.0,
                    "cancelled_value": 0.0,
                    "invalid_leads": 1.0,
                    "valid_leads": 0.0,
                }
            ],
            "touchpoints": [{"timestamp": "2026-02-01T12:00:00Z", "channel": "google_ads", "interaction_type": "click"}],
        },
    ]
    out = build_channel_summary_response(
        journeys=journeys,
        expenses=[],
        date_from="2026-02-01",
        date_to="2026-02-01",
        compare=False,
    )
    assert out["totals"]["outcomes_current"]["net_revenue"] == 80.0
    assert out["totals"]["outcomes_current"]["refunded_value"] == 40.0
    assert out["totals"]["outcomes_current"]["view_through_conversions"] == 1.0
    assert out["totals"]["outcomes_current"]["invalid_leads"] == 1.0
    assert out["notes"]


def test_build_channel_summary_keeps_visit_rows_when_conversion_key_has_no_matches():
    journeys = [
        {
            "converted": True,
            "kpi_type": "conversion",
            "conversion_value": 50.0,
            "touchpoints": [{"timestamp": "2026-02-02T10:00:00Z", "channel": "google_ads"}],
        },
        {
            "converted": False,
            "touchpoints": [{"timestamp": "2026-02-01T10:00:00Z", "channel": "email"}],
        },
    ]
    out = build_channel_summary_response(
        journeys=journeys,
        expenses=[],
        date_from="2026-02-01",
        date_to="2026-02-02",
        compare=False,
        conversion_key="form_submit",
    )
    assert out["items"]
    by_channel = {row["channel"]: row for row in out["items"]}
    assert by_channel["google_ads"]["current"]["visits"] == 1.0
    assert by_channel["google_ads"]["current"]["conversions"] == 0.0
    assert by_channel["email"]["current"]["visits"] == 1.0
    assert by_channel["email"]["current"]["conversions"] == 0.0


def test_build_channel_trend_supports_visits_for_all_touchpoints():
    journeys = [
        {
            "converted": False,
            "touchpoints": [
                {"timestamp": "2026-02-01T10:00:00Z", "channel": "google_ads"},
                {"timestamp": "2026-02-01T11:00:00Z", "channel": "email"},
            ],
        },
        {
            "converted": True,
            "conversion_value": 50.0,
            "touchpoints": [{"timestamp": "2026-02-02T10:00:00Z", "channel": "google_ads"}],
        },
    ]
    out = build_channel_trend_response(
        journeys=journeys,
        expenses=[],
        date_from="2026-02-01",
        date_to="2026-02-02",
        timezone="UTC",
        kpi_key="visits",
        grain="daily",
        compare=False,
    )
    rows = [r for r in out["series"] if r["channel"] == "google_ads"]
    assert any(r["ts"] == "2026-02-01" and r["value"] == 1.0 for r in rows)
    assert any(r["ts"] == "2026-02-02" and r["value"] == 1.0 for r in rows)


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
    assert "outcomes_current" in out["totals"]


def test_build_campaign_summary_keeps_visit_rows_when_conversion_key_has_no_matches():
    journeys = [
        {
            "converted": True,
            "kpi_type": "conversion",
            "conversion_value": 50.0,
            "touchpoints": [{"timestamp": "2026-02-02T10:00:00Z", "channel": "google_ads", "campaign": "Brand"}],
        },
        {
            "converted": False,
            "touchpoints": [{"timestamp": "2026-02-01T10:00:00Z", "channel": "email", "campaign": "Newsletter"}],
        },
    ]
    out = build_campaign_summary_response(
        journeys=journeys,
        expenses=[],
        date_from="2026-02-01",
        date_to="2026-02-02",
        compare=False,
        conversion_key="form_submit",
    )
    assert out["items"]
    by_campaign = {row["campaign_id"]: row for row in out["items"]}
    assert by_campaign["google_ads:Brand"]["current"]["visits"] == 1.0
    assert by_campaign["google_ads:Brand"]["current"]["conversions"] == 0.0
    assert by_campaign["email:Newsletter"]["current"]["visits"] == 1.0
    assert by_campaign["email:Newsletter"]["current"]["conversions"] == 0.0


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
    rows = {r["campaign_id"]: r for r in summary["items"]}
    assert "google_ads:A" in rows
    assert "google_ads:B" in rows
    assert rows["google_ads:A"]["current"]["conversions"] == 1.0
    assert rows["google_ads:B"]["current"]["visits"] == 1.0
    assert rows["google_ads:B"]["current"]["conversions"] == 0.0


def _unit_db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def test_build_campaign_aggregate_overlay_uses_last_touch_channel_and_campaign():
    db = _unit_db_session()
    try:
        db.add(
            JourneyDefinition(
                id="jd-campaign",
                name="Campaign Journey",
                conversion_kpi_id="purchase",
                lookback_window_days=30,
                mode_default="conversion_only",
                created_by="test",
                updated_by="test",
                is_archived=False,
                created_at=datetime(2026, 2, 1, 0, 0),
                updated_at=datetime(2026, 2, 1, 0, 0),
            )
        )
        db.add(
            JourneyPathDaily(
                date=datetime(2026, 2, 1).date(),
                journey_definition_id="jd-campaign",
                path_hash="path-1",
                path_steps=["Paid Landing", "Purchase / Lead Won (conversion)"],
                path_length=2,
                count_journeys=2,
                count_conversions=2,
                gross_conversions_total=2.0,
                net_conversions_total=1.0,
                gross_revenue_total=250.0,
                net_revenue_total=200.0,
                click_through_conversions_total=1.0,
                view_through_conversions_total=0.0,
                mixed_path_conversions_total=0.0,
                channel_group="paid",
                last_touch_channel="meta_ads",
                campaign_id="Spring",
                created_at=datetime(2026, 2, 1, 0, 0),
                updated_at=datetime(2026, 2, 1, 0, 0),
            )
        )
        db.commit()

        overlay = build_campaign_aggregate_overlay(
            db,
            date_from="2026-02-01",
            date_to="2026-02-02",
            timezone="UTC",
            compare=False,
        )

        assert overlay is not None
        assert overlay["current_store"]["meta_ads:Spring"]["2026-02-01"]["conversions"] == 2.0
        assert overlay["current_store"]["meta_ads:Spring"]["2026-02-01"]["revenue"] == 250.0
        assert overlay["current_outcomes"]["meta_ads:Spring"]["net_revenue"] == 200.0
    finally:
        db.close()


def test_build_campaign_aggregate_overlay_falls_back_to_silver_when_journey_daily_facts_absent():
    db = _unit_db_session()
    try:
        db.add(
            JourneyDefinition(
                id="jd-silver-campaign",
                name="Campaign Journey",
                conversion_kpi_id="purchase",
                lookback_window_days=30,
                mode_default="conversion_only",
                created_by="test",
                updated_by="test",
                is_archived=False,
                created_at=datetime(2026, 2, 1, 0, 0),
                updated_at=datetime(2026, 2, 1, 0, 0),
            )
        )
        db.commit()

        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-prev"},
                    "touchpoints": [{"channel": "meta_ads", "campaign": "Winter", "interaction_type": "click", "ts": "2026-01-31T10:00:00Z"}],
                    "conversions": [{"id": "conv-prev", "name": "purchase", "ts": "2026-01-31T12:00:00Z", "value": 90.0}],
                },
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-current"},
                    "touchpoints": [{"channel": "meta_ads", "campaign": "Spring", "interaction_type": "click", "ts": "2026-02-01T10:00:00Z"}],
                    "conversions": [{"id": "conv-current", "name": "purchase", "ts": "2026-02-01T12:00:00Z", "value": 150.0}],
                },
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="silver-campaign-overlay-batch",
        )
        assert inserted == 2

        overlay = build_campaign_aggregate_overlay(
            db,
            date_from="2026-02-01",
            date_to="2026-02-01",
            timezone="UTC",
            compare=True,
            conversion_key="purchase",
        )

        assert overlay is not None
        assert overlay["current_store"]["meta_ads:Spring"]["2026-02-01"]["conversions"] == 1.0
        assert overlay["current_store"]["meta_ads:Spring"]["2026-02-01"]["revenue"] == 150.0
        assert overlay["previous_store"]["meta_ads:Winter"]["2026-01-31"]["revenue"] == 90.0
        assert overlay["meta"]["meta_ads:Spring"]["campaign_name"] == "Spring"
    finally:
        db.close()


def test_build_campaign_summary_supports_aggregate_overlay():
    journeys = [
        {
            "converted": False,
            "touchpoints": [{"timestamp": "2026-02-01T10:00:00Z", "channel": "meta_ads", "campaign": "Spring"}],
        }
    ]
    overlay = {
        "current_store": {"meta_ads:Spring": {"2026-02-01": {"conversions": 2.0, "revenue": 250.0}}},
        "previous_store": {},
        "current_outcomes": {
            "meta_ads:Spring": {
                "gross_conversions": 2.0,
                "net_conversions": 1.0,
                "gross_revenue": 250.0,
                "net_revenue": 200.0,
                "refunded_value": 0.0,
                "cancelled_value": 0.0,
                "invalid_leads": 0.0,
                "valid_leads": 1.0,
                "click_through_conversions": 1.0,
                "view_through_conversions": 0.0,
                "mixed_path_conversions": 0.0,
            }
        },
        "previous_outcomes": {},
        "meta": {
            "meta_ads:Spring": {
                "campaign_id": "meta_ads:Spring",
                "campaign_name": "Spring",
                "channel": "meta_ads",
                "platform": None,
            }
        },
    }
    expenses = [{"channel": "meta_ads", "service_period_start": "2026-02-01T08:00:00Z", "amount": 40.0}]

    out = build_campaign_summary_response(
        journeys=journeys,
        expenses=expenses,
        date_from="2026-02-01",
        date_to="2026-02-01",
        compare=False,
        aggregate_overlay=overlay,
    )

    row = out["items"][0]
    assert row["campaign_id"] == "meta_ads:Spring"
    assert row["current"]["visits"] == 1.0
    assert row["current"]["conversions"] == 2.0
    assert row["current"]["revenue"] == 250.0
    assert row["current"]["spend"] == 40.0
    assert row["outcomes"]["current"]["net_revenue"] == 200.0


def test_build_campaign_trend_supports_aggregate_overlay():
    journeys = [
        {
            "converted": False,
            "touchpoints": [{"timestamp": "2026-02-01T10:00:00Z", "channel": "meta_ads", "campaign": "Spring"}],
        }
    ]
    overlay = {
        "current_store": {"meta_ads:Spring": {"2026-02-01": {"conversions": 2.0, "revenue": 250.0}}},
        "previous_store": {"meta_ads:Spring": {"2026-01-31": {"conversions": 1.0, "revenue": 100.0}}},
        "current_outcomes": {},
        "previous_outcomes": {},
        "meta": {
            "meta_ads:Spring": {
                "campaign_id": "meta_ads:Spring",
                "campaign_name": "Spring",
                "channel": "meta_ads",
                "platform": None,
            }
        },
    }

    out = build_campaign_trend_response(
        journeys=journeys,
        expenses=[],
        date_from="2026-02-01",
        date_to="2026-02-01",
        timezone="UTC",
        kpi_key="revenue",
        grain="daily",
        compare=True,
        aggregate_overlay=overlay,
    )

    current_values = [r for r in out["series"] if r["campaign_id"] == "meta_ads:Spring" and r["value"] is not None]
    previous_values = [r for r in out["series_prev"] if r["campaign_id"] == "meta_ads:Spring" and r["value"] is not None]
    assert current_values == [
        {
            "ts": "2026-02-01",
            "campaign_id": "meta_ads:Spring",
            "campaign_name": "Spring",
            "channel": "meta_ads",
            "platform": None,
            "value": 250.0,
        }
    ]
    assert previous_values == [
        {
            "ts": "2026-01-31",
            "campaign_id": "meta_ads:Spring",
            "campaign_name": "Spring",
            "channel": "meta_ads",
            "platform": None,
            "value": 100.0,
        }
    ]
