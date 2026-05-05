from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
import app.main as main_module
from app.models_config_dq import MeiroEventProfileState
from app.modules.mmm.service import fit_model
from app.modules.mmm.schemas import ModelConfig
from app.services_conversions import persist_journeys_as_conversion_paths
from app.services_journey_cache import invalidate_journey_cache
from app.services_mmm_platform import build_mmm_dataset_from_platform
from app.services_walled_garden import calculate_synthetic_impressions, synthetic_column


def _journey(timestamp: str, *, revenue: float = 100.0):
    return {
        "converted": True,
        "value": revenue,
        "touchpoints": [
            {"timestamp": timestamp, "channel": "paid_search"},
        ],
    }


def _v2_journey(profile_id: str, timestamp: str, *, revenue: float, journey_id: str):
    return {
        "_schema": "journey_v2",
        "journey_id": journey_id,
        "customer": {"id": profile_id},
        "touchpoints": [{"ts": timestamp, "channel": "paid_search"}],
        "conversions": [{"ts": timestamp, "name": "purchase", "value": revenue}],
    }


@pytest.fixture
def mmm_client(tmp_path, monkeypatch: pytest.MonkeyPatch):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)

    old_expenses = dict(main_module.EXPENSES)
    old_datasets = dict(main_module.DATASETS)
    monkeypatch.setattr(main_module, "MMM_PLATFORM_DIR", tmp_path)
    main_module.EXPENSES.clear()
    main_module.EXPENSES.update(
        {
            "paid-search-april": {
                "channel": "paid_search",
                "status": "active",
                "converted_amount": 1400.0,
                "service_period_start": "2026-04-01",
                "service_period_end": "2026-04-14",
            }
        }
    )
    main_module.DATASETS.clear()

    def override_get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides.clear()
    app.dependency_overrides[get_db] = override_get_db
    invalidate_journey_cache()
    try:
        with TestClient(app) as test_client:
            yield test_client, SessionLocal
    finally:
        app.dependency_overrides.clear()
        invalidate_journey_cache()
        main_module.EXPENSES.clear()
        main_module.EXPENSES.update(old_expenses)
        main_module.DATASETS.clear()
        main_module.DATASETS.update(old_datasets)
        engine.dispose()


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


def test_platform_mmm_adds_synthetic_impression_columns_from_delivery_rows():
    df, coverage = build_mmm_dataset_from_platform(
        journeys=[
            _journey("2026-04-08T10:00:00Z", revenue=250.0),
            _journey("2026-04-14T10:00:00Z", revenue=300.0),
        ],
        expenses=[
            {
                "channel": "meta_ads",
                "status": "active",
                "converted_amount": 1200.0,
                "service_period_start": "2026-04-01",
                "service_period_end": "2026-04-14",
            }
        ],
        delivery_rows=[
            {
                "date": "2026-04-08",
                "channel": "meta_ads",
                "spend": 500.0,
                "impressions": 10000,
                "reach": 4000,
                "clicks": 120,
            },
            {
                "date": "2026-04-14",
                "channel": "meta_ads",
                "spend": 700.0,
                "impressions": 14000,
                "frequency": 4.0,
                "clicks": 180,
            },
        ],
        date_start="2026-04-01",
        date_end="2026-04-14",
        kpi_target="sales",
        spend_channels=["meta_ads"],
    )

    col = synthetic_column("meta_ads")

    assert col in df.columns
    assert float(df[col].sum()) > 0
    assert coverage["synthetic_impression_totals"][col] == float(df[col].sum())
    assert coverage["delivery"]["channels"]["meta_ads"]["confidence"] in {"medium", "high"}


def test_synthetic_impressions_apply_quality_and_frequency_damping():
    low_frequency, _ = calculate_synthetic_impressions(
        {"channel": "meta_ads", "impressions": 10000, "frequency": 2, "clicks": 0}
    )
    high_frequency, components = calculate_synthetic_impressions(
        {"channel": "meta_ads", "impressions": 10000, "frequency": 8, "clicks": 0}
    )

    assert low_frequency > high_frequency
    assert components["frequency_damping"] < 1


def test_build_platform_dataset_endpoint_filters_to_measurement_audience(mmm_client):
    client, SessionLocal = mmm_client
    db = SessionLocal()
    try:
        persist_journeys_as_conversion_paths(
            db,
            [
                _v2_journey("profile-in", "2026-04-08T10:00:00Z", revenue=100.0, journey_id="journey-in"),
                _v2_journey("profile-out", "2026-04-08T10:00:00Z", revenue=900.0, journey_id="journey-out"),
            ],
            replace=True,
            import_source="test",
        )
        db.add(
            MeiroEventProfileState(
                profile_id="profile-in",
                profile_json={"segments": [{"id": "vip", "name": "VIP buyers"}]},
            )
        )
        db.commit()
    finally:
        db.close()
    invalidate_journey_cache()

    resp = client.post(
        "/api/mmm/datasets/build-from-platform",
        json={
            "date_start": "2026-04-01",
            "date_end": "2026-04-14",
            "kpi_target": "sales",
            "spend_channels": ["paid_search"],
            "media_input_mode": "spend",
            "measurement_audience": {
                "id": "meiro:vip",
                "name": "VIP buyers",
                "external_segment_id": "vip",
            },
            "source_contract": {
                "attribution_source": "Pipes raw events -> replay/import -> live journeys",
                "spend_source": "Platform expenses",
            },
        },
    )

    assert resp.status_code == 200
    payload = resp.json()
    sales_total = sum(float(row.get("sales") or 0.0) for row in payload["preview_rows"])
    assert sales_total == 100.0
    audience = payload["metadata"]["measurement_audience"]
    assert audience["materialization_status"] == "journey_rows_filtered"
    assert audience["profile_count"] == 1
    assert audience["journey_rows"] == 1
    assert payload["metadata"]["source_contract"]["measurement_audience"]["external_segment_id"] == "vip"


def test_build_platform_dataset_endpoint_rejects_audience_without_matching_journeys(mmm_client):
    client, SessionLocal = mmm_client
    db = SessionLocal()
    try:
        persist_journeys_as_conversion_paths(
            db,
            [_v2_journey("profile-out", "2026-04-08T10:00:00Z", revenue=900.0, journey_id="journey-out")],
            replace=True,
            import_source="test",
        )
        db.add(
            MeiroEventProfileState(
                profile_id="profile-ghost",
                profile_json={"segments": [{"id": "vip", "name": "VIP buyers"}]},
            )
        )
        db.commit()
    finally:
        db.close()
    invalidate_journey_cache()

    resp = client.post(
        "/api/mmm/datasets/build-from-platform",
        json={
            "date_start": "2026-04-01",
            "date_end": "2026-04-14",
            "kpi_target": "sales",
            "spend_channels": ["paid_search"],
            "measurement_audience": {
                "id": "meiro:vip",
                "name": "VIP buyers",
                "external_segment_id": "vip",
            },
        },
    )

    assert resp.status_code == 400
    assert "no matching journeys" in resp.json()["detail"]


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


def test_fit_model_rejects_tall_dataset_when_selected_channels_have_no_spend(tmp_path):
    dataset_path = tmp_path / "other-channel-spend.csv"
    pd.DataFrame(
        {
            "date": ["2026-04-06", "2026-04-13"],
            "channel": ["facebook_ads", "facebook_ads"],
            "campaign": ["prospecting", "prospecting"],
            "spend": [100.0, 120.0],
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
        mmm_fit_model_fn=lambda **_: pytest.fail("fit should not run when selected channels have no spend"),
    )

    assert runs["run-1"]["status"] == "error"
    assert "selected spend channels" in runs["run-1"]["detail"]
