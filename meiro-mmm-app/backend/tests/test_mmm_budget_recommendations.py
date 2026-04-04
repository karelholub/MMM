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
        main_module.DATASETS.clear()
        main_module.DATASETS.update(original_datasets)
        main_module.SETTINGS.feature_flags.mmm_enabled = original_mmm_enabled
        engine.dispose()
