from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient

from app.db import Base
from app.main import app
from app.services_journey_settings import (
    JourneySettingsStatus,
    activate_journey_settings_version,
    create_journey_settings_draft,
    ensure_active_journey_settings,
    validate_journey_settings,
)


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
