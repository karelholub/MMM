from app.services_metrics import (
    delta_pct,
    derive_efficiency,
    journey_revenue_value,
    metric_value,
    summarize_rows,
)


def test_metric_value_and_efficiency_are_consistent():
    row = {"spend": 80.0, "conversions": 20.0, "revenue": 200.0}
    eff = derive_efficiency(spend=row["spend"], conversions=row["conversions"], revenue=row["revenue"])
    assert metric_value(row, "spend") == 80.0
    assert metric_value(row, "conversions") == 20.0
    assert metric_value(row, "revenue") == 200.0
    assert metric_value(row, "roas") == eff["roas"] == 2.5
    assert metric_value(row, "cpa") == eff["cpa"] == 4.0


def test_delta_pct_zero_baseline_behavior():
    assert delta_pct(0.0, 0.0) == 0.0
    assert delta_pct(10.0, 0.0) == 100.0
    assert delta_pct(12.0, 10.0) == 20.0


def test_journey_revenue_value_dedup_entries():
    journey = {
        "_revenue_entries": [
            {"dedup_key": "a", "value_in_base": 100.0},
            {"dedup_key": "a", "value_in_base": 200.0},
            {"dedup_key": "b", "value_in_base": 50.0},
        ]
    }
    seen = set()
    assert journey_revenue_value(journey, dedupe_seen=seen) == 150.0
    assert journey_revenue_value(journey, dedupe_seen=seen) == 0.0


def test_summarize_rows_totals_and_derived():
    totals, derived = summarize_rows(
        {
            "d1": {"spend": 50.0, "conversions": 5.0, "revenue": 100.0},
            "d2": {"spend": 25.0, "conversions": 5.0, "revenue": 75.0},
        }
    )
    assert totals == {"spend": 75.0, "conversions": 10.0, "revenue": 175.0}
    assert round(derived["roas"] or 0.0, 4) == round(175.0 / 75.0, 4)
    assert round(derived["cpa"] or 0.0, 4) == 7.5
