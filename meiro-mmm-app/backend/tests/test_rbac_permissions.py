import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app


@pytest.fixture
def client():
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

    app.dependency_overrides.clear()
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app, base_url="https://testserver") as test_client:
        yield test_client
    app.dependency_overrides.clear()
    engine.dispose()


def test_permission_denial_has_standard_error_shape(client: TestClient):
    # No session, no legacy role header => viewer-level only; roles.manage should be denied.
    res = client.get("/api/admin/roles")
    assert res.status_code == 403
    body = res.json()
    assert isinstance(body.get("detail"), dict)
    detail = body["detail"]
    assert detail.get("code") == "permission_denied"
    assert detail.get("permission") == "roles.manage"


def test_session_permissions_override_legacy_role_header(client: TestClient):
    login = client.post(
        "/api/auth/login",
        json={"email": "viewer-rbac@example.com", "workspace_id": "default"},
    )
    assert login.status_code == 200
    csrf = login.json()["csrf_token"]
    session_cookie = login.cookies.get("mmm_session")
    assert session_cookie

    # With active viewer session, spoofed editor header must not grant settings.manage.
    res = client.post(
        "/api/settings",
        headers={
            "X-CSRF-Token": csrf,
            "X-User-Role": "admin",
            "X-User-Id": "spoofed-admin",
            "Cookie": f"mmm_session={session_cookie}",
        },
        json={
            "attribution": {
                "lookback_window_days": 30,
                "use_converted_flag": True,
                "min_conversion_value": 0,
                "time_decay_half_life_days": 7,
                "position_first_pct": 0.4,
                "position_last_pct": 0.4,
                "markov_min_paths": 5,
            },
            "mmm": {"frequency": "W"},
            "nba": {
                "min_prefix_support": 5,
                "min_conversion_rate": 0.01,
                "max_prefix_depth": 5,
                "min_next_support": 5,
                "max_suggestions_per_prefix": 3,
                "min_uplift_pct": None,
                "excluded_channels": ["direct"],
            },
            "feature_flags": {
                "journeys_enabled": False,
                "journey_examples_enabled": False,
                "funnel_builder_enabled": False,
                "funnel_diagnostics_enabled": False,
                "access_control_enabled": False,
                "custom_roles_enabled": False,
                "audit_log_enabled": False,
                "scim_enabled": False,
                "sso_enabled": False,
            },
        },
    )
    assert res.status_code in (401, 403)
    if res.status_code == 403:
        assert res.json()["detail"]["permission"] == "settings.manage"


def test_viewer_cannot_access_admin_access_control_endpoints(client: TestClient):
    login = client.post(
        "/api/auth/login",
        json={"email": "viewer-admin-deny@example.com", "workspace_id": "default"},
    )
    assert login.status_code == 200

    users_res = client.get("/api/admin/users")
    assert users_res.status_code == 403
    assert users_res.json()["detail"]["permission"] == "users.manage"

    roles_res = client.get("/api/admin/roles")
    assert roles_res.status_code == 403
    assert roles_res.json()["detail"]["permission"] == "roles.manage"
