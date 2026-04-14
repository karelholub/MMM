from __future__ import annotations

import copy

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.connectors.ads_ops.base import AdsApplyResult, AdsProviderError
from app.db import Base, get_db
from app.main import app
from app import main as main_module


class _FakeAdsAdapter:
    key = "google_ads"

    def build_deep_link(self, *, account_id: str, entity_type: str, entity_id: str) -> str:
        return f"https://ads.example.com/{account_id}/{entity_type}/{entity_id}"

    def fetch_entity_state(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str):
        return {"status": "enabled", "budget": 100.0, "currency": "USD", "name": entity_id}

    def pause_entity(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str, idempotency_key: str):
        return AdsApplyResult(ok=True, provider_request_id=f"pause-{idempotency_key}")

    def enable_entity(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str, idempotency_key: str):
        return AdsApplyResult(ok=True, provider_request_id=f"enable-{idempotency_key}")

    def update_budget(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str, daily_budget: float, currency: str | None, idempotency_key: str):
        return AdsApplyResult(ok=True, provider_request_id=f"budget-{idempotency_key}")

    def normalize_error(self, exc: Exception):
        return AdsProviderError(code="provider_error", message=str(exc), retryable=False, needs_reauth=False)

    def supports(self, action_type: str, entity_type: str) -> bool:
        return action_type in {"pause", "enable", "update_budget"}


def test_budget_recommendations_and_scenarios(tmp_path, monkeypatch):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)

    def override_get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    dataset_path = tmp_path / "mmm-budget.csv"
    dataset_path.write_text(
        "\n".join(
            [
                "date,google_spend,meta_spend,email_spend,conversions",
                "2026-01-01,1000,800,200,120",
                "2026-01-08,1050,820,220,125",
                "2026-01-15,1100,840,250,130",
                "2026-01-22,1080,860,240,128",
                "2026-01-29,1125,900,260,134",
                "2026-02-05,1110,920,255,133",
                "2026-02-12,1150,930,265,137",
                "2026-02-19,1175,940,280,139",
                "2026-02-26,1180,960,290,141",
                "2026-03-05,1200,980,300,144",
                "2026-03-12,1225,995,305,147",
                "2026-03-19,1210,1005,315,149",
            ]
        ),
        encoding="utf-8",
    )

    original_runs = copy.deepcopy(main_module.RUNS)
    original_datasets = copy.deepcopy(main_module.DATASETS)
    original_mmm_enabled = getattr(main_module.SETTINGS.feature_flags, "mmm_enabled", False)

    main_module.RUNS.clear()
    main_module.DATASETS.clear()
    main_module.SETTINGS.feature_flags.mmm_enabled = True
    main_module.DATASETS["budget-test-dataset"] = {
        "path": dataset_path,
        "type": "sales",
        "metadata": {"period_start": "2026-01-01", "period_end": "2026-03-19"},
    }
    monkeypatch.setattr("app.services_ads_ops.get_ads_adapter", lambda _provider: _FakeAdsAdapter())
    monkeypatch.setattr("app.services_ads_ops.get_access_token_for_provider", lambda *_args, **_kwargs: "token-1")
    main_module.RUNS["mmm_budget_test"] = {
        "status": "finished",
        "dataset_id": "budget-test-dataset",
        "config": {
            "dataset_id": "budget-test-dataset",
            "kpi": "conversions",
            "spend_channels": ["google_spend", "meta_spend", "email_spend"],
        },
        "roi": [
            {"channel": "google_spend", "roi": 2.1},
            {"channel": "meta_spend", "roi": 0.9},
            {"channel": "email_spend", "roi": 2.6},
        ],
        "contrib": [
            {"channel": "google_spend", "mean_share": 0.42},
            {"channel": "meta_spend", "mean_share": 0.36},
            {"channel": "email_spend", "mean_share": 0.22},
        ],
        "channel_summary": [
            {"channel": "google_spend", "spend": 13605, "roi": 2.1, "mroas": 1.7, "elasticity": 0.32},
            {"channel": "meta_spend", "spend": 10950, "roi": 0.9, "mroas": 0.8, "elasticity": 0.12},
            {"channel": "email_spend", "spend": 3180, "roi": 2.6, "mroas": 2.2, "elasticity": 0.41},
        ],
        "diagnostics": {"rhat_max": 1.01, "ess_bulk_min": 450, "divergences": 0},
    }

    app.dependency_overrides.clear()
    app.dependency_overrides[get_db] = override_get_db

    try:
        with TestClient(app) as client:
            recommendation_resp = client.get(
                "/api/models/mmm_budget_test/budget/recommendations",
                params={"objective": "protect_efficiency", "total_budget_change_pct": 0},
            )
            assert recommendation_resp.status_code == 200
            recommendation_body = recommendation_resp.json()
            assert recommendation_body["recommendations"]
            top = recommendation_body["recommendations"][0]
            assert top["objective"] == "protect_efficiency"
            assert top["actions"]
            assert any(action["action"] == "increase" for action in top["actions"])

            create_resp = client.post(
                "/api/models/mmm_budget_test/budget/scenarios",
                json={
                    "objective": recommendation_body["objective"],
                    "total_budget_change_pct": 0,
                    "multipliers": {"google_spend": 1.08, "meta_spend": 0.92, "email_spend": 1.06},
                    "recommendations": recommendation_body["recommendations"],
                },
            )
            assert create_resp.status_code == 200
            created = create_resp.json()
            assert created["objective"] == "protect_efficiency"
            assert len(created["recommendations"]) == 1

            list_resp = client.get("/api/models/mmm_budget_test/budget/scenarios")
            assert list_resp.status_code == 200
            listed = list_resp.json()
            assert listed["total"] == 1
            assert listed["items"][0]["id"] == created["id"]

            get_resp = client.get(f"/api/models/mmm_budget_test/budget/scenarios/{created['id']}")
            assert get_resp.status_code == 200
            loaded = get_resp.json()
            assert loaded["recommendations"][0]["actions"]

            create_requests_resp = client.post(
                "/api/ads/change-requests/from-budget-recommendation",
                json={
                    "run_id": "mmm_budget_test",
                    "scenario_id": created["id"],
                    "recommendation_id": created["recommendations"][0]["id"],
                    "currency": "USD",
                    "targets": [
                        {
                            "channel": "google_spend",
                            "provider": "google_ads",
                            "account_id": "acct-1",
                            "entity_id": "brand-search",
                            "entity_name": "Brand Search",
                            "delta_pct": 0.08,
                            "reason": "Top efficiency channel",
                        }
                    ],
                },
                headers={"X-User-Role": "admin", "X-User-Id": "qa-admin"},
            )
            assert create_requests_resp.status_code == 200
            assert create_requests_resp.json()["total"] == 1

            realization_resp = client.get("/api/models/mmm_budget_test/budget/realization")
            assert realization_resp.status_code == 200
            realization = realization_resp.json()
            assert realization["total"] == 1
            assert realization["items"][0]["execution"]["counts"]["pending_approval"] == 1
            assert realization["items"][0]["execution"]["proposed_budget_delta_total"] == 8.0
    finally:
        app.dependency_overrides.clear()
        main_module.RUNS.clear()
        main_module.RUNS.update(original_runs)
        main_module._save_runs()
        main_module.DATASETS.clear()
        main_module.DATASETS.update(original_datasets)
        main_module.SETTINGS.feature_flags.mmm_enabled = original_mmm_enabled
        engine.dispose()


def test_mmm_run_with_missing_dataset_is_readout_only(tmp_path):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)

    def override_get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    original_runs = copy.deepcopy(main_module.RUNS)
    original_datasets = copy.deepcopy(main_module.DATASETS)
    original_mmm_enabled = getattr(main_module.SETTINGS.feature_flags, "mmm_enabled", False)

    main_module.RUNS.clear()
    main_module.DATASETS.clear()
    main_module.SETTINGS.feature_flags.mmm_enabled = True
    main_module.DATASETS["missing-mmm-dataset"] = {
        "path": tmp_path / "missing-mmm-dataset.csv",
        "type": "sales",
        "source": "platform",
        "metadata": {"period_start": "2026-01-01", "period_end": "2026-03-19"},
    }
    main_module.RUNS["mmm_missing_dataset"] = {
        "status": "finished",
        "dataset_id": "missing-mmm-dataset",
        "created_at": "2026-04-01T10:00:00Z",
        "updated_at": "2026-04-02T10:00:00Z",
        "config": {
            "dataset_id": "missing-mmm-dataset",
            "kpi": "conversions",
            "spend_channels": ["google_spend", "meta_spend"],
        },
        "r2": 0.71,
        "roi": [
            {"channel": "google_spend", "roi": 2.1},
            {"channel": "meta_spend", "roi": 0.9},
        ],
        "contrib": [
            {"channel": "google_spend", "mean_share": 0.62},
            {"channel": "meta_spend", "mean_share": 0.38},
        ],
    }

    app.dependency_overrides.clear()
    app.dependency_overrides[get_db] = override_get_db

    try:
        with TestClient(app) as client:
            list_resp = client.get("/api/models")
            assert list_resp.status_code == 200
            listed_run = next(item for item in list_resp.json() if item["run_id"] == "mmm_missing_dataset")
            assert listed_run["dataset_available"] is False
            assert listed_run["scenario_count"] == 0
            assert listed_run["latest_scenario_at"] is None
            assert listed_run["quality"]["label"] == "Readout only"
            assert listed_run["quality"]["can_use_results"] is True
            assert listed_run["quality"]["can_use_budget"] is False

            get_resp = client.get("/api/models/mmm_missing_dataset")
            assert get_resp.status_code == 200
            loaded_run = get_resp.json()
            assert loaded_run["dataset_available"] is False
            assert loaded_run["roi"]
            assert loaded_run["quality"]["label"] == "Readout only"

            dataset_resp = client.get("/api/datasets/missing-mmm-dataset", params={"preview_only": True})
            assert dataset_resp.status_code == 200
            assert dataset_resp.json()["available"] is False

            recommendation_resp = client.get(
                "/api/models/mmm_missing_dataset/budget/recommendations",
                params={"objective": "protect_efficiency", "total_budget_change_pct": 0},
            )
            assert recommendation_resp.status_code == 200
            recommendation_body = recommendation_resp.json()
            assert recommendation_body["recommendations"] == []
            assert recommendation_body["decision"]["status"] == "blocked"
            assert recommendation_body["decision"]["actions"][0]["id"] == "rebuild_mmm_dataset"
            assert recommendation_body["summary"]["channels_considered"] == 2
    finally:
        app.dependency_overrides.clear()
        main_module.RUNS.clear()
        main_module.RUNS.update(original_runs)
        main_module.DATASETS.clear()
        main_module.DATASETS.update(original_datasets)
        main_module.SETTINGS.feature_flags.mmm_enabled = original_mmm_enabled
        engine.dispose()


def test_models_list_prioritizes_finished_runs_and_marks_stale_jobs(tmp_path):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)

    def override_get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    available_path = tmp_path / "available-mmm-dataset.csv"
    available_path.write_text(
        "\n".join(
            [
                "date,paid_search_spend,conversions",
                "2026-01-01,100,10",
                "2026-01-08,120,12",
            ]
        ),
        encoding="utf-8",
    )

    original_runs = copy.deepcopy(main_module.RUNS)
    original_datasets = copy.deepcopy(main_module.DATASETS)
    original_mmm_enabled = getattr(main_module.SETTINGS.feature_flags, "mmm_enabled", False)

    main_module.RUNS.clear()
    main_module.DATASETS.clear()
    main_module.SETTINGS.feature_flags.mmm_enabled = True
    main_module.DATASETS["available-mmm-dataset"] = {
        "path": available_path,
        "type": "sales",
        "metadata": {"period_start": "2026-01-01", "period_end": "2026-01-08"},
    }
    main_module.DATASETS["missing-mmm-dataset"] = {
        "path": tmp_path / "missing-mmm-dataset.csv",
        "type": "sales",
        "metadata": {"period_start": "2026-01-01", "period_end": "2026-01-08"},
    }
    main_module.RUNS.update(
        {
            "mmm_stale_running": {
                "status": "running",
                "dataset_id": "available-mmm-dataset",
                "created_at": "2020-01-01T00:00:00Z",
                "updated_at": "2020-01-01T01:00:00Z",
                "config": {
                    "dataset_id": "available-mmm-dataset",
                    "kpi": "conversions",
                    "spend_channels": ["paid_search_spend"],
                },
            },
            "mmm_finished_missing_dataset": {
                "status": "finished",
                "dataset_id": "missing-mmm-dataset",
                "created_at": "2026-04-01T00:00:00Z",
                "updated_at": "2026-04-01T01:00:00Z",
                "config": {
                    "dataset_id": "missing-mmm-dataset",
                    "kpi": "conversions",
                    "spend_channels": ["paid_search_spend"],
                },
                "roi": [{"channel": "paid_search_spend", "roi": 1.2}],
                "contrib": [{"channel": "paid_search_spend", "mean_share": 1.0}],
            },
            "mmm_finished_available_dataset": {
                "status": "finished",
                "dataset_id": "available-mmm-dataset",
                "created_at": "2026-03-01T00:00:00Z",
                "updated_at": "2026-03-01T01:00:00Z",
                "config": {
                    "dataset_id": "available-mmm-dataset",
                    "kpi": "conversions",
                    "spend_channels": ["paid_search_spend"],
                },
                "roi": [{"channel": "paid_search_spend", "roi": 1.4}],
                "contrib": [{"channel": "paid_search_spend", "mean_share": 1.0}],
            },
            "mmm_error_newer": {
                "status": "error",
                "created_at": "2026-04-10T00:00:00Z",
                "updated_at": "2026-04-10T01:00:00Z",
                "config": {"kpi": "conversions", "spend_channels": []},
            },
        }
    )

    app.dependency_overrides.clear()
    app.dependency_overrides[get_db] = override_get_db

    try:
        with TestClient(app) as client:
            list_resp = client.get("/api/models")
            assert list_resp.status_code == 200
            body = list_resp.json()
            ordered_ids = [item["run_id"] for item in body]

            assert ordered_ids.index("mmm_finished_available_dataset") < ordered_ids.index("mmm_finished_missing_dataset")
            assert ordered_ids.index("mmm_finished_missing_dataset") < ordered_ids.index("mmm_stale_running")
            assert ordered_ids.index("mmm_stale_running") < ordered_ids.index("mmm_error_newer")

            stale_item = next(item for item in body if item["run_id"] == "mmm_stale_running")
            assert stale_item["status"] == "stale"
            assert stale_item["stale_from_status"] == "running"
            assert stale_item["stale_reason"] == "run_heartbeat_expired"
            assert stale_item["detail"]

            get_resp = client.get("/api/models/mmm_stale_running")
            assert get_resp.status_code == 200
            loaded = get_resp.json()
            assert loaded["status"] == "stale"
            assert loaded["dataset_available"] is True
    finally:
        app.dependency_overrides.clear()
        main_module.RUNS.clear()
        main_module.RUNS.update(original_runs)
        main_module.DATASETS.clear()
        main_module.DATASETS.update(original_datasets)
        main_module.SETTINGS.feature_flags.mmm_enabled = original_mmm_enabled
        engine.dispose()


def test_not_usable_mmm_run_blocks_budget_actions(tmp_path):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)

    def override_get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    dataset_path = tmp_path / "zero-signal-mmm.csv"
    dataset_path.write_text(
        "\n".join(
            [
                "date,paid_search_spend,conversions",
                "2026-01-01,100,0",
                "2026-01-08,120,0",
            ]
        ),
        encoding="utf-8",
    )

    original_runs = copy.deepcopy(main_module.RUNS)
    original_datasets = copy.deepcopy(main_module.DATASETS)
    original_mmm_enabled = getattr(main_module.SETTINGS.feature_flags, "mmm_enabled", False)

    main_module.RUNS.clear()
    main_module.DATASETS.clear()
    main_module.SETTINGS.feature_flags.mmm_enabled = True
    main_module.DATASETS["zero-signal-dataset"] = {
        "path": dataset_path,
        "type": "sales",
        "metadata": {"period_start": "2026-01-01", "period_end": "2026-01-08"},
    }
    main_module.RUNS["mmm_zero_signal"] = {
        "status": "finished",
        "dataset_id": "zero-signal-dataset",
        "created_at": "2026-04-01T00:00:00Z",
        "updated_at": "2026-04-01T01:00:00Z",
        "config": {
            "dataset_id": "zero-signal-dataset",
            "kpi": "conversions",
            "spend_channels": ["paid_search_spend"],
        },
        "r2": 0.0,
        "roi": [{"channel": "paid_search_spend", "roi": 0.0}],
        "contrib": [{"channel": "paid_search_spend", "mean_share": 0.0}],
        "channel_summary": [{"channel": "paid_search_spend", "spend": 220.0}],
    }

    app.dependency_overrides.clear()
    app.dependency_overrides[get_db] = override_get_db

    try:
        with TestClient(app) as client:
            get_resp = client.get("/api/models/mmm_zero_signal")
            assert get_resp.status_code == 200
            assert get_resp.json()["quality"]["level"] == "not_usable"

            recommendation_resp = client.get("/api/models/mmm_zero_signal/budget/recommendations")
            assert recommendation_resp.status_code == 200
            recommendation_body = recommendation_resp.json()
            assert recommendation_body["decision"]["status"] == "blocked"
            assert recommendation_body["recommendations"] == []
            assert "no usable media signal" in " ".join(recommendation_body["decision"]["blockers"])

            optimize_resp = client.post("/api/models/mmm_zero_signal/optimize", json={"paid_search_spend": 1.0})
            assert optimize_resp.status_code == 400
    finally:
        app.dependency_overrides.clear()
        main_module.RUNS.clear()
        main_module.RUNS.update(original_runs)
        main_module.DATASETS.clear()
        main_module.DATASETS.update(original_datasets)
        main_module.SETTINGS.feature_flags.mmm_enabled = original_mmm_enabled
        engine.dispose()
