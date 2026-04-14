import pandas as pd

from app.mmm_engine import fit_model
from app.mmm_version import CURRENT_MMM_ENGINE_VERSION


def test_ridge_wide_uses_total_spend_for_roi_and_channel_summary():
    df = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-05", periods=6, freq="W-MON"),
            "paid_search": [0.0, 100.0, 200.0, 300.0, 100.0, 0.0],
            "facebook_ads": [200.0, 100.0, 0.0, 0.0, 0.0, 100.0],
            "conversions": [5.0, 15.0, 25.0, 35.0, 15.0, 5.0],
        }
    )

    result = fit_model(
        df,
        target_column="conversions",
        channel_columns=["paid_search", "facebook_ads"],
        force_engine="ridge",
    )

    paid_roi = next(row["roi"] for row in result["roi"] if row["channel"] == "paid_search")
    summary_spend = {row["channel"]: row["spend"] for row in result["channel_summary"]}
    contribution_sum = sum(row["mean_share"] for row in result["contrib"])

    assert result["engine"] == "ridge-fallback"
    assert result["engine_version"] == CURRENT_MMM_ENGINE_VERSION
    assert paid_roi > 0
    assert summary_spend["paid_search"] == 700.0
    assert summary_spend["facebook_ads"] == 400.0
    assert contribution_sum == 1.0
    assert result["diagnostics"]["contribution_basis"] == "coefficient_x_total_spend"


def test_ridge_tall_aggregates_campaign_outputs_to_channel_totals():
    df = pd.DataFrame(
        {
            "date": [
                "2026-01-05",
                "2026-01-05",
                "2026-01-12",
                "2026-01-12",
                "2026-01-19",
                "2026-01-19",
                "2026-01-26",
                "2026-01-26",
            ],
            "channel": ["paid_search", "facebook_ads"] * 4,
            "campaign": ["brand", "prospecting"] * 4,
            "spend": [100.0, 10.0, 200.0, 20.0, 300.0, 30.0, 400.0, 40.0],
            "conversions": [12.0, 12.0, 22.0, 22.0, 32.0, 32.0, 42.0, 42.0],
        }
    )

    result = fit_model(
        df,
        target_column="conversions",
        channel_columns=["paid_search"],
        force_engine="ridge",
    )

    paid_summary = next(row for row in result["channel_summary"] if row["channel"] == "paid_search")
    paid_roi = next(row["roi"] for row in result["roi"] if row["channel"] == "paid_search")
    paid_contrib = next(row for row in result["contrib"] if row["channel"] == "paid_search")

    assert paid_summary["spend"] == 1000.0
    assert paid_summary["roi"] == paid_roi
    assert paid_roi > 0
    assert paid_contrib["mean_share"] > 0
    assert result["campaigns"][0]["spend"] > 0
