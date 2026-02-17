from app.services_performance_helpers import (
    PerformanceQueryContext,
    build_performance_meta,
    build_mapping_coverage,
    build_performance_query_context,
    compute_campaign_trends,
    compute_total_converted_value_for_period,
    normalize_channel_filter,
    summarize_mapped_current,
)


def test_normalize_channel_filter_handles_csv_and_all():
    assert normalize_channel_filter(["email,paid", " direct "]) == ["direct", "email", "paid"]
    assert normalize_channel_filter(["all", "email"]) is None


def test_build_performance_query_context_normalizes_period_and_kpi():
    ctx = build_performance_query_context(
        date_from="2026-01-01",
        date_to="2026-01-31",
        timezone="UTC",
        currency=None,
        workspace=None,
        account=None,
        model_id=None,
        kpi_key=" Revenue ",
        grain="daily",
        compare=True,
        channels=["email,paid"],
        conversion_key="purchase",
    )
    assert ctx.kpi_key == "revenue"
    assert ctx.channels == ["email", "paid"]
    assert ctx.current_period is not None
    assert ctx.previous_period is not None


def test_compute_total_converted_value_for_period_uses_deduped_revenue_entries():
    journeys = [
        {
            "converted": True,
            "kpi_type": "purchase",
            "touchpoints": [{"channel": "email", "timestamp": "2026-01-10T10:00:00+00:00"}],
            "_revenue_entries": [{"dedup_key": "order:1", "value_in_base": 120.0}],
        },
        {
            "converted": True,
            "kpi_type": "purchase",
            "touchpoints": [{"channel": "email", "timestamp": "2026-01-11T10:00:00+00:00"}],
            "_revenue_entries": [{"dedup_key": "order:1", "value_in_base": 120.0}],
        },
    ]
    total = compute_total_converted_value_for_period(
        journeys=journeys,
        date_from="2026-01-01",
        date_to="2026-01-31",
        timezone_name="UTC",
        channels=["email"],
        conversion_key="purchase",
    )
    assert total == 120.0


def test_compute_campaign_trends_uses_revenue_entries():
    journeys = [
        {
            "converted": True,
            "touchpoints": [{"channel": "email", "campaign": "welcome", "timestamp": "2026-01-10T00:00:00+00:00"}],
            "_revenue_entries": [{"dedup_key": "cv:1", "value_in_base": 70.0}],
        }
    ]
    out = compute_campaign_trends(journeys)
    assert out["campaigns"] == ["email:welcome"]
    assert out["series"]["email:welcome"][0]["revenue"] == 70.0


def test_build_mapping_coverage_calculates_percentages():
    coverage = build_mapping_coverage(
        mapped_spend=50.0,
        mapped_value=80.0,
        expenses=[
            {"channel": "email", "amount": 100.0, "service_period_start": "2026-01-10T00:00:00+00:00", "status": "active"}
        ],
        journeys=[
            {
                "converted": True,
                "touchpoints": [{"channel": "email", "timestamp": "2026-01-10T00:00:00+00:00"}],
                "_revenue_entries": [{"dedup_key": "cv:1", "value_in_base": 100.0}],
            }
        ],
        date_from="2026-01-01",
        date_to="2026-01-31",
        timezone_name="UTC",
        channels=["email"],
        conversion_key=None,
    )
    assert coverage["spend_total"] == 100.0
    assert coverage["value_total"] == 100.0
    assert coverage["spend_mapped_pct"] == 50.0
    assert coverage["value_mapped_pct"] == 80.0


def test_summarize_mapped_current_and_meta_builder():
    mapped = summarize_mapped_current(
        [
            {"current": {"spend": 10.0, "revenue": 40.0}},
            {"current": {"spend": 5.0, "revenue": 20.0}},
        ]
    )
    assert mapped == {"mapped_spend": 15.0, "mapped_value": 60.0}

    ctx = PerformanceQueryContext(
        date_from="2026-01-01",
        date_to="2026-01-31",
        timezone="UTC",
        currency="EUR",
        workspace="w1",
        account="a1",
        model_id="m1",
        kpi_key="revenue",
        grain="daily",
        compare=True,
        channels=["email"],
        conversion_key="purchase",
        current_period={"date_from": "2026-01-01", "date_to": "2026-01-31", "days": 31},
        previous_period={"date_from": "2025-12-01", "date_to": "2025-12-31", "days": 31},
    )
    meta = build_performance_meta(ctx=ctx, include_kpi=True)
    assert meta["kpi_key"] == "revenue"
    assert meta["query_context"]["grain"] == "daily"
