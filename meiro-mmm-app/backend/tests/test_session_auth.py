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


def test_login_sets_secure_session_cookie_and_me_works(client: TestClient):
    res = client.post(
        "/api/auth/login",
        json={"email": "viewer@example.com", "name": "Viewer User", "workspace_id": "default"},
    )
    assert res.status_code == 200
    body = res.json()
    assert body["user"]["email"] == "viewer@example.com"
    assert body["workspace"]["id"] == "default"
    assert isinstance(body["csrf_token"], str) and len(body["csrf_token"]) > 10

    set_cookie = res.headers.get("set-cookie", "")
    assert "mmm_session=" in set_cookie
    assert "HttpOnly" in set_cookie
    assert "Secure" in set_cookie
    assert "SameSite=lax" in set_cookie or "SameSite=Lax" in set_cookie

    me = client.get("/api/auth/me")
    assert me.status_code == 200
    assert me.json()["authenticated"] is True


def test_csrf_required_for_cookie_authenticated_state_changes(client: TestClient):
    login = client.post(
        "/api/auth/login",
        json={"email": "csrf@example.com", "workspace_id": "default"},
    )
    assert login.status_code == 200
    csrf = login.json()["csrf_token"]
    session_cookie = login.cookies.get("mmm_session")
    assert session_cookie

    blocked = client.post(
        "/api/auth/logout-all",
        cookies={"mmm_session": session_cookie},
    )
    assert blocked.status_code in (401, 403)
    if blocked.status_code == 403:
        assert "CSRF" in str(blocked.json()["detail"])

    allowed = client.post(
        "/api/auth/logout-all",
        headers={"X-CSRF-Token": csrf},
        cookies={"mmm_session": session_cookie},
    )
    assert allowed.status_code == 200
    assert allowed.json()["ok"] is True


def test_session_auth_context_not_overridden_by_spoofed_role_header(client: TestClient):
    login = client.post(
        "/api/auth/login",
        json={"email": "viewer2@example.com", "workspace_id": "default"},
    )
    assert login.status_code == 200
    csrf = login.json()["csrf_token"]
    assert login.cookies.get("mmm_session")

    # Viewer membership should not gain editor permissions from forged header.
    create_resp = client.post(
        "/api/journeys/definitions",
        headers={
            "X-CSRF-Token": csrf,
            "X-User-Role": "editor",
            "X-User-Id": "spoofed-editor",
        },
        json={
            "name": "Should be blocked",
            "conversion_kpi_id": "purchase",
            "lookback_window_days": 30,
            "mode_default": "conversion_only",
        },
    )
    assert create_resp.status_code in (401, 403)
    if create_resp.status_code == 403:
        detail = create_resp.json().get("detail")
        if isinstance(detail, dict):
            assert detail.get("permission") == "journeys.manage"
