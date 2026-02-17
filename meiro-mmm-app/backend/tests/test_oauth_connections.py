from datetime import timedelta
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.connectors.oauth.base import OAuthError
from app.connectors.oauth.registry import get_oauth_provider, list_oauth_provider_keys
from app.db import Base, get_db
from app.main import app
from app.models_config_dq import OAuthSession, SecretStore
from app.services_oauth_connections import (
    build_pkce_challenge,
    complete_oauth_callback,
    consume_oauth_session,
    create_oauth_session,
    list_oauth_connections,
    list_provider_accounts,
    select_accounts,
    utcnow,
)
from app.utils.encrypt import decrypt


@pytest.fixture
def db():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    sess = SessionLocal()
    try:
        yield sess
    finally:
        sess.close()
        engine.dispose()


class _FakeProvider:
    key = "google_ads"

    def scopes(self):
        return ["scope.a"]

    def build_auth_url(self, *, client_id: str, redirect_uri: str, state: str, code_challenge: str):
        return f"https://provider.test/auth?client_id={client_id}&state={state}&challenge={code_challenge}&redirect_uri={redirect_uri}"

    def exchange_code_for_token(self, *, code: str, code_verifier: str, redirect_uri: str, client_id: str, client_secret: str):
        if code == "bad-code":
            raise RuntimeError("invalid_grant")
        return {
            "access_token": "access-token-1",
            "refresh_token": "refresh-token-1",
            "expires_in": 3600,
            "token_type": "Bearer",
            "scope": "scope.a",
        }

    def refresh_access_token(self, *, refresh_token: str, client_id: str, client_secret: str):
        return {
            "access_token": "access-token-2",
            "refresh_token": refresh_token,
            "expires_in": 3600,
            "token_type": "Bearer",
            "scope": "scope.a",
        }

    def fetch_accounts(self, *, access_token: str, credentials):
        return [
            {"id": "acct-1", "name": "Account 1", "provider": self.key},
            {"id": "acct-2", "name": "Account 2", "provider": self.key},
        ]

    def normalize_error(self, exc: Exception):
        msg = str(exc)
        if "invalid_grant" in msg:
            return OAuthError(code="invalid_grant", message="token invalid", needs_reauth=True)
        return OAuthError(code="provider_error", message=msg)


def test_registry_contains_expected_oauth_providers():
    keys = list_oauth_provider_keys()
    assert "google_ads" in keys
    assert "meta_ads" in keys
    assert "linkedin_ads" in keys
    assert get_oauth_provider("google_ads").key == "google_ads"
    with pytest.raises(ValueError):
        get_oauth_provider("unknown_provider")


def test_list_connections_includes_readiness_metadata(db):
    out = list_oauth_connections(db, workspace_id="ws-1")
    assert out["total"] == 3
    row = next((i for i in out["items"] if i["provider_key"] == "google_ads"), None)
    assert row is not None
    assert "can_start" in row
    assert "missing_config_keys" in row
    assert "missing_config_reason" in row


def test_pkce_challenge_generation_is_stable():
    verifier = "abc123_verifier"
    challenge = build_pkce_challenge(verifier)
    assert isinstance(challenge, str)
    assert challenge
    assert challenge == build_pkce_challenge(verifier)
    assert "=" not in challenge


def test_oauth_session_state_verification_and_expiry(db):
    session_data = create_oauth_session(
        db,
        workspace_id="ws-1",
        user_id="user-1",
        provider_key="google_ads",
        return_url="/datasources",
    )
    sess, verifier = consume_oauth_session(db, provider_key="google_ads", state=session_data["state"])
    assert sess.workspace_id == "ws-1"
    assert verifier == session_data["code_verifier"]

    with pytest.raises(ValueError, match="already used"):
        consume_oauth_session(db, provider_key="google_ads", state=session_data["state"])

    expired = create_oauth_session(
        db,
        workspace_id="ws-1",
        user_id="user-1",
        provider_key="google_ads",
        return_url="/datasources",
    )
    row = db.query(OAuthSession).filter(OAuthSession.state == expired["state"]).first()
    assert row is not None
    row.expires_at = utcnow() - timedelta(minutes=1)
    db.add(row)
    db.commit()

    with pytest.raises(ValueError, match="expired"):
        consume_oauth_session(db, provider_key="google_ads", state=expired["state"])


def test_callback_flow_creates_connection_fetches_accounts_and_never_leaks_tokens(db, monkeypatch):
    fake = _FakeProvider()
    monkeypatch.setattr("app.services_oauth_connections.get_oauth_provider", lambda _k: fake)
    monkeypatch.setattr(
        "app.services_oauth_connections._ensure_provider_credentials",
        lambda _k: {"client_id": "cid", "client_secret": "csecret"},
    )

    started = create_oauth_session(
        db,
        workspace_id="ws-1",
        user_id="user-1",
        provider_key="google_ads",
        return_url="/datasources",
    )
    row, return_url, err = complete_oauth_callback(
        db,
        provider_key="google_ads",
        code="ok-code",
        state=started["state"],
        redirect_uri="https://app.example.com/oauth/google_ads/callback",
    )
    assert err is None
    assert return_url == "/datasources"
    assert row.status == "connected"
    assert row.secret_ref

    stored = db.get(SecretStore, row.secret_ref)
    assert stored is not None
    decrypted = decrypt(stored.secret_encrypted)
    assert "access-token-1" in decrypted

    listed = list_oauth_connections(db, workspace_id="ws-1")
    text = str(listed)
    assert "access-token-1" not in text
    assert "refresh-token-1" not in text

    accounts = list_provider_accounts(db, workspace_id="ws-1", provider_key="google_ads")
    assert len(accounts) == 2

    selected = select_accounts(
        db,
        workspace_id="ws-1",
        provider_key="google_ads",
        account_ids=["acct-1", "acct-does-not-exist"],
    )
    assert selected["selected_accounts"] == ["acct-1"]


def test_api_oauth_flow_start_callback_accounts_and_selection(monkeypatch):
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

    fake = _FakeProvider()
    monkeypatch.setattr("app.services_oauth_connections.get_oauth_provider", lambda _k: fake)
    monkeypatch.setattr(
        "app.services_oauth_connections._ensure_provider_credentials",
        lambda _k: {"client_id": "cid", "client_secret": "csecret"},
    )

    app.dependency_overrides.clear()
    app.dependency_overrides[get_db] = override_get_db
    try:
        with TestClient(app, base_url="https://testserver") as client:
            start = client.post(
                "/api/connections/google_ads/start",
                headers={"X-User-Role": "admin", "X-User-Id": "qa-admin"},
                json={"return_url": "/datasources"},
            )
            assert start.status_code == 200, start.text
            auth_url = start.json()["authorization_url"]
            qs = parse_qs(urlparse(auth_url).query)
            state = (qs.get("state") or [None])[0]
            assert state

            callback = client.get(
                f"/oauth/google_ads/callback?code=ok-code&state={state}",
                follow_redirects=False,
            )
            assert callback.status_code in (302, 307)
            location = callback.headers.get("location", "")
            assert "oauth_status=connected" in location

            accounts = client.get(
                "/api/connections/google_ads/accounts",
                headers={"X-User-Role": "admin", "X-User-Id": "qa-admin"},
            )
            assert accounts.status_code == 200
            assert len(accounts.json()["accounts"]) == 2

            selected = client.post(
                "/api/connections/google_ads/select-accounts",
                headers={"X-User-Role": "admin", "X-User-Id": "qa-admin"},
                json={"account_ids": ["acct-2"]},
            )
            assert selected.status_code == 200
            assert selected.json()["selected_accounts"] == ["acct-2"]

            listed = client.get(
                "/api/connections",
                headers={"X-User-Role": "admin", "X-User-Id": "qa-admin"},
            )
            assert listed.status_code == 200
            payload = str(listed.json())
            assert "access-token-1" not in payload
            assert "refresh-token-1" not in payload
    finally:
        app.dependency_overrides.clear()
        engine.dispose()
