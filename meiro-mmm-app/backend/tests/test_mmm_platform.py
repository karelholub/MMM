from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from app.modules.mmm.service import fit_model
from app.modules.mmm.schemas import ModelConfig
from app.services_mmm_platform import build_mmm_dataset_from_platform


def _journey(timestamp: str, *, revenue: float = 100.0):
    return {
        "converted": True,
        "value": revenue,
        "touchpoints": [
            {"timestamp": timestamp, "channel": "paid_search"},
        ],
    }


def test_platform_mmm_allocates_service_period_expenses_to_monday_weeks():
    df, coverage = build_mmm_dataset_from_platform(
        journeys=[
            _journey("2026-04-02T10:00:00Z"),
            _journey("2026-04-08T10:00:00Z"),
            _journey("2026-04-14T10:00:00Z"),
        ],
        expenses=[
            {
                "channel": "paid_search",
                "status": "active",
                "converted_amount": 3000.0,
                "service_period_start": "2026-04-01",
                "service_period_end": "2026-04-30",
            }
        ],
        date_start="2026-03-01",
        date_end="2026-04-14",
        kpi_target="attribution",
        spend_channels=["paid_search"],
    )

    rows = df.set_index("date")

    assert float(df["paid_search"].sum()) > 0
    assert rows.loc["2026-03-30", "paid_search"] > 0
    assert rows.loc["2026-04-06", "paid_search"] > 0
    assert rows.loc["2026-04-13", "paid_search"] > 0
    assert rows.loc["2026-03-30", "conversions"] == 1.0
    assert rows.loc["2026-04-06", "conversions"] == 1.0
    assert rows.loc["2026-04-13", "conversions"] == 1.0
    assert coverage["total_spend"] > 0
    assert coverage["channels_with_spend"] == ["paid_search"]
    assert coverage["all_zero_spend_channels"] == []


def test_platform_mmm_ignores_deleted_dict_expenses_and_rejects_zero_spend():
    with pytest.raises(ValueError, match="No spend was found"):
        build_mmm_dataset_from_platform(
            journeys=[_journey("2026-04-08T10:00:00Z")],
            expenses=[
                {
                    "channel": "paid_search",
                    "status": "deleted",
                    "converted_amount": 3000.0,
                    "service_period_start": "2026-04-01",
                    "service_period_end": "2026-04-30",
                }
            ],
            date_start="2026-04-01",
            date_end="2026-04-14",
            kpi_target="attribution",
            spend_channels=["paid_search"],
        )


def test_fit_model_rejects_all_zero_wide_spend_dataset(tmp_path):
    dataset_path = tmp_path / "zero-spend.csv"
    pd.DataFrame(
        {
            "date": ["2026-04-06", "2026-04-13"],
            "paid_search": [0.0, 0.0],
            "conversions": [10.0, 20.0],
        }
    ).to_csv(dataset_path, index=False)
    runs = {"run-1": {"status": "queued"}}
    saved = []

    fit_model(
        run_id="run-1",
        cfg=ModelConfig(dataset_id="dataset-1", kpi="conversions", spend_channels=["paid_search"]),
        runs_obj=runs,
        datasets_obj={"dataset-1": {"path": str(dataset_path)}},
        now_iso_fn=lambda: "2026-04-14T12:00:00Z",
        save_runs_fn=lambda: saved.append(True),
        mmm_fit_model_fn=lambda **_: pytest.fail("fit should not run for zero-spend data"),
    )

    assert runs["run-1"]["status"] == "error"
    assert "zero spend" in runs["run-1"]["detail"]
    assert saved


def test_fit_model_rejects_all_zero_tall_spend_dataset(tmp_path):
    dataset_path = tmp_path / "zero-spend-tall.csv"
    pd.DataFrame(
        {
            "date": ["2026-04-06", "2026-04-13"],
            "channel": ["paid_search", "paid_search"],
            "campaign": ["brand", "brand"],
            "spend": [0.0, 0.0],
            "conversions": [10.0, 20.0],
        }
    ).to_csv(dataset_path, index=False)
    runs = {"run-1": {"status": "queued"}}

    fit_model(
        run_id="run-1",
        cfg=SimpleNamespace(
            dataset_id="dataset-1",
            kpi="conversions",
            spend_channels=["paid_search"],
            covariates=[],
            priors={},
            mcmc={},
        ),
        runs_obj=runs,
        datasets_obj={"dataset-1": {"path": str(dataset_path)}},
        now_iso_fn=lambda: "2026-04-14T12:00:00Z",
        save_runs_fn=lambda: None,
        mmm_fit_model_fn=lambda **_: pytest.fail("fit should not run for zero-spend data"),
    )

    assert runs["run-1"]["status"] == "error"
    assert "zero spend" in runs["run-1"]["detail"]
