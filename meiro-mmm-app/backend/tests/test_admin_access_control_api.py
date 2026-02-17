import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app.models_config_dq import SecurityAuditLog
from app.services_access_control import ensure_access_control_seed_data


@pytest.fixture
def client():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        ensure_access_control_seed_data(db)
    finally:
        db.close()

    def override_get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides.clear()
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client, SessionLocal
    app.dependency_overrides.clear()
    engine.dispose()


def _admin_headers():
    return {"X-User-Role": "admin", "X-User-Id": "qa-admin"}


def _enable_access_flags(test_client: TestClient):
    current = test_client.get("/api/settings", headers=_admin_headers())
    assert current.status_code == 200
    payload = current.json()
    payload["feature_flags"]["audit_log_enabled"] = True
    payload["feature_flags"]["custom_roles_enabled"] = True
    updated = test_client.post("/api/settings", headers=_admin_headers(), json=payload)
    assert updated.status_code == 200


def test_admin_invitation_accept_flow_and_audit_log(client):
    test_client, SessionLocal = client
    _enable_access_flags(test_client)

    roles = test_client.get("/api/admin/roles", headers=_admin_headers())
    assert roles.status_code == 200
    viewer = next(r for r in roles.json()["items"] if r["name"] == "Viewer")

    inv = test_client.post(
        "/api/admin/invitations",
        headers=_admin_headers(),
        json={"email": "new.user@example.com", "role_id": viewer["id"], "workspace_id": "default"},
    )
    assert inv.status_code == 200
    token = inv.json()["token"]

    accepted = test_client.post("/api/invitations/accept", json={"token": token, "name": "New User"})
    assert accepted.status_code == 200
    assert accepted.json()["ok"] is True

    users = test_client.get("/api/admin/users", headers=_admin_headers(), params={"search": "new.user@example.com"})
    assert users.status_code == 200
    assert len(users.json()["items"]) == 1

    db = SessionLocal()
    try:
        action_keys = {row.action_key for row in db.query(SecurityAuditLog).all()}
        assert "invitation.created" in action_keys
        assert "invitation.accepted" in action_keys
    finally:
        db.close()


def test_admin_custom_role_and_membership_role_change(client):
    test_client, _SessionLocal = client
    _enable_access_flags(test_client)

    create_role = test_client.post(
        "/api/admin/roles",
        headers=_admin_headers(),
        json={
            "name": "Campaign Analyst",
            "description": "Limited campaign analytics role",
            "permission_keys": ["journeys.view", "funnels.view", "attribution.view", "alerts.view"],
            "workspace_id": "default",
        },
    )
    assert create_role.status_code == 200
    role_id = create_role.json()["id"]

    roles = test_client.get("/api/admin/roles", headers=_admin_headers())
    assert roles.status_code == 200
    viewer = next(r for r in roles.json()["items"] if r["name"] == "Viewer")

    inv = test_client.post(
        "/api/admin/invitations",
        headers=_admin_headers(),
        json={"email": "member.role@example.com", "role_id": viewer["id"], "workspace_id": "default"},
    )
    assert inv.status_code == 200

    accepted = test_client.post("/api/invitations/accept", json={"token": inv.json()["token"]})
    assert accepted.status_code == 200
    membership_id = accepted.json()["membership_id"]

    patch = test_client.patch(
        f"/api/admin/memberships/{membership_id}",
        headers=_admin_headers(),
        json={"role_id": role_id},
    )
    assert patch.status_code == 200
    assert patch.json()["role_id"] == role_id


def test_admin_audit_log_endpoint_filters_and_pagination(client):
    test_client, _SessionLocal = client
    _enable_access_flags(test_client)

    roles = test_client.get("/api/admin/roles", headers=_admin_headers())
    assert roles.status_code == 200
    viewer = next(r for r in roles.json()["items"] if r["name"] == "Viewer")

    created = test_client.post(
        "/api/admin/invitations",
        headers=_admin_headers(),
        json={"email": "audit.filter@example.com", "role_id": viewer["id"], "workspace_id": "default"},
    )
    assert created.status_code == 200

    page_1 = test_client.get(
        "/api/admin/audit-log",
        headers=_admin_headers(),
        params={"workspaceId": "default", "page": 1, "page_size": 1},
    )
    assert page_1.status_code == 200
    payload = page_1.json()
    assert payload["total"] >= 1
    assert len(payload["items"]) == 1

    filtered = test_client.get(
        "/api/admin/audit-log",
        headers=_admin_headers(),
        params={"workspaceId": "default", "action": "invitation.created"},
    )
    assert filtered.status_code == 200
    assert filtered.json()["items"]
    assert all("invitation.created" in row["action_key"] for row in filtered.json()["items"])
