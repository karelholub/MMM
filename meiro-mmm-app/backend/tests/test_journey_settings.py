from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient
from datetime import datetime, timezone

from app.db import Base
from app.main import app
from app.services_journey_settings import (
    JourneySettingsStatus,
    activate_journey_settings_version,
    build_journey_settings_context,
    build_journey_settings_impact_preview,
    build_journey_settings_validation_report,
    create_journey_settings_draft,
    ensure_active_journey_settings,
    validate_journey_settings,
)
from app.models_config_dq import JourneyInstanceFact, JourneyStepFact, SilverTouchpointFact
from app.utils.kpi_config import default_kpi_config


def _unit_db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def test_validate_journey_settings_reports_range_errors():
    payload = {
        "schema_version": "1.0",
        "flow_defaults": {"max_depth": 99},
    }
    out = validate_journey_settings(payload)
    assert out["valid"] is False
    assert out["errors"]
    assert any("flow_defaults.max_depth" in err["path"] for err in out["errors"])


def test_activate_journey_settings_archives_previous_active():
    db = _unit_db_session()
    try:
        first = ensure_active_journey_settings(db, actor="test")
        assert first.status == JourneySettingsStatus.ACTIVE

        second = create_journey_settings_draft(
            db,
            created_by="test",
            description="Second draft",
            settings_json=first.settings_json,
        )
        activated = activate_journey_settings_version(
            db,
            version_id=second.id,
            actor="test",
        )
        db.refresh(first)
        assert activated.status == JourneySettingsStatus.ACTIVE
        assert first.status == JourneySettingsStatus.ARCHIVED
    finally:
        db.close()


def test_activate_route_requires_permission_and_confirm():
    client = TestClient(app)
    created = client.post(
        "/api/settings/journeys/versions",
        json={"created_by": "test-ui", "description": "route test"},
        headers={"X-User-Role": "admin"},
    )
    assert created.status_code == 200
    version_id = created.json()["id"]

    denied = client.post(
        "/api/settings/journeys/activate",
        json={"version_id": version_id, "confirm": True, "actor": "test-ui"},
        headers={"X-User-Role": "viewer"},
    )
    assert denied.status_code == 403

    not_confirmed = client.post(
        "/api/settings/journeys/activate",
        json={"version_id": version_id, "confirm": False, "actor": "test-ui"},
        headers={"X-User-Role": "admin"},
    )
    assert not_confirmed.status_code == 400


def test_build_journey_settings_impact_preview_uses_canonical_output_counts(monkeypatch):
    db = _unit_db_session()
    try:
        monkeypatch.setattr(
            "app.services_journey_settings.count_recent_path_outputs",
            lambda db, date_from=None, date_to=None: 12,
        )
        monkeypatch.setattr(
            "app.services_journey_settings.count_recent_transition_outputs",
            lambda db, date_from=None, date_to=None: 34,
        )
        monkeypatch.setattr(
            "app.services_journey_settings.count_recent_transition_from_steps",
            lambda db, date_from=None, date_to=None: 5,
        )

        preview = build_journey_settings_impact_preview(
            db,
            draft_settings_json={"schema_version": "1.0"},
        )

        assert preview["preview_available"] is True
        assert preview["baseline"]["recent_paths_7d"] == 12
        assert preview["baseline"]["recent_transitions_7d"] == 34
        assert preview["baseline"]["distinct_steps_7d"] == 5
    finally:
        db.close()


def test_build_journey_settings_context_uses_observed_workspace_values():
    db = _unit_db_session()
    try:
        ensure_active_journey_settings(db, actor="test")
        db.add_all(
            [
                JourneyInstanceFact(
                    conversion_id="c1",
                    profile_id="p1",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 1, tzinfo=timezone.utc),
                    path_hash="a",
                    path_length=3,
                    steps_json=["Paid Landing", "Product / Content View", "Conversion"],
                    channel_group="paid_search",
                    gross_conversions_total=1.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=10.0,
                    net_revenue_total=10.0,
                ),
                JourneyInstanceFact(
                    conversion_id="c2",
                    profile_id="p2",
                    conversion_key="lead",
                    conversion_ts=datetime(2026, 4, 2, tzinfo=timezone.utc),
                    path_hash="b",
                    path_length=2,
                    steps_json=["Organic / Direct Landing", "Conversion"],
                    channel_group="organic_search",
                    gross_conversions_total=1.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=0.0,
                    net_revenue_total=0.0,
                ),
                SilverTouchpointFact(
                    conversion_id="c1",
                    profile_id="p1",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 1, tzinfo=timezone.utc),
                    ordinal=1,
                    channel="paid_search",
                    event_name="product_view",
                ),
                SilverTouchpointFact(
                    conversion_id="c2",
                    profile_id="p2",
                    conversion_key="lead",
                    conversion_ts=datetime(2026, 4, 2, tzinfo=timezone.utc),
                    ordinal=1,
                    channel="organic_search",
                    event_name="form_submit",
                ),
                JourneyStepFact(
                    conversion_id="c1",
                    profile_id="p1",
                    conversion_key="purchase",
                    ordinal=1,
                    step_name="Paid Landing",
                ),
                JourneyStepFact(
                    conversion_id="c2",
                    profile_id="p2",
                    conversion_key="lead",
                    ordinal=1,
                    step_name="Organic / Direct Landing",
                ),
            ]
        )
        db.commit()

        context = build_journey_settings_context(db, kpi_config=default_kpi_config())
        assert context["workspace_summary"]["journeys_loaded"] == 2
        assert any(item["value"] == "paid_search" for item in context["observed_channels"])
        assert any(item["value"] == "product_view" for item in context["observed_event_names"])
        assert any(item["value"] == "Paid Landing" for item in context["observed_steps"])
        assert any(item["id"] == "purchase" and item["observed_count"] >= 1 for item in context["observed_kpis"])
        assert context["scaffold_settings_json"]["step_canonicalization"]["rules"]
    finally:
        db.close()


def test_build_journey_settings_validation_report_returns_rule_evidence():
    db = _unit_db_session()
    try:
        db.add_all(
            [
                SilverTouchpointFact(
                    conversion_id="c1",
                    profile_id="p1",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 1, tzinfo=timezone.utc),
                    ordinal=1,
                    channel="paid_search",
                    event_name="product_view",
                ),
                SilverTouchpointFact(
                    conversion_id="c2",
                    profile_id="p2",
                    conversion_key="lead",
                    conversion_ts=datetime(2026, 4, 2, tzinfo=timezone.utc),
                    ordinal=1,
                    channel="organic_search",
                    event_name="form_submit",
                ),
            ]
        )
        db.commit()

        report = build_journey_settings_validation_report(
            db,
            settings_json={
                "schema_version": "1.0",
                "step_canonicalization": {
                    "rules": [
                        {
                            "step_name": "Paid Landing",
                            "priority": 10,
                            "enabled": True,
                            "channel_group_equals": ["paid_search"],
                            "event_name_equals": ["product_view"],
                        },
                        {
                            "step_name": "Unknown Rule",
                            "priority": 10,
                            "enabled": True,
                            "channel_group_equals": ["unknown_channel"],
                        },
                    ]
                },
            },
        )

        assert report["valid"] is True
        assert report["rule_evidence"]["summary"]["total_touchpoints"] == 2
        assert report["rule_evidence"]["rules"][0]["matched_touchpoints"] == 1
        assert report["rule_evidence"]["rules"][1]["matched_touchpoints"] == 0
        assert any("unknown_channel" in warning for warning in report["rule_evidence"]["rules"][1]["warnings"])
    finally:
        db.close()


def test_journey_settings_context_route_returns_scaffold():
    client = TestClient(app)
    resp = client.get(
        "/api/settings/journeys/context",
        headers={"X-User-Role": "viewer"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "scaffold_settings_json" in body
    assert "workspace_summary" in body


def test_journey_settings_validate_route_returns_rule_evidence():
    client = TestClient(app)
    resp = client.post(
        "/api/settings/journeys/validate",
        json={
            "settings_json": {
                "schema_version": "1.0",
                "step_canonicalization": {
                    "rules": [
                        {
                            "step_name": "Paid Landing",
                            "priority": 10,
                            "enabled": True,
                            "channel_group_equals": ["paid_search"],
                            "event_name_equals": ["product_view"],
                        }
                    ]
                },
            }
        },
        headers={"X-User-Role": "viewer"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "rule_evidence" in body
    assert "rules" in body["rule_evidence"]
