from __future__ import annotations

import base64
import hashlib
import json
import secrets
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.connectors.oauth.base import OAuthError
from app.connectors.oauth.registry import get_oauth_provider
from app.models_config_dq import DataSource, OAuthSession, SecretStore
from app.utils.datasource_config import get_effective
from app.utils.encrypt import decrypt, encrypt


OAUTH_PROVIDER_LABELS: Dict[str, str] = {
    "google_ads": "Google Ads",
    "meta_ads": "Meta Ads",
    "linkedin_ads": "LinkedIn Ads",
}

OAUTH_PROVIDER_REQUIRED_KEYS: Dict[str, List[str]] = {
    "google_ads": ["client_id", "client_secret"],
    "meta_ads": ["client_id", "client_secret"],
    "linkedin_ads": ["client_id", "client_secret"],
}

OAUTH_SESSION_TTL_MINUTES = 15


def utcnow() -> datetime:
    return datetime.utcnow()


def normalize_provider_key(provider_key: str) -> str:
    key = (provider_key or "").strip().lower()
    aliases = {
        "google": "google_ads",
        "meta": "meta_ads",
        "facebook": "meta_ads",
        "linkedin": "linkedin_ads",
    }
    return aliases.get(key, key)


def _urlsafe_b64_no_pad(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def generate_state() -> str:
    return secrets.token_urlsafe(48)


def generate_pkce_verifier() -> str:
    return _urlsafe_b64_no_pad(secrets.token_bytes(64))


def build_pkce_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("utf-8")).digest()
    return _urlsafe_b64_no_pad(digest)


def _credentials_for_provider(provider_key: str) -> Dict[str, str]:
    if provider_key == "google_ads":
        return {
            "client_id": get_effective("google", "client_id"),
            "client_secret": get_effective("google", "client_secret"),
            "developer_token": (get_effective("google", "developer_token") or "").strip(),
        }
    if provider_key == "meta_ads":
        return {
            "client_id": get_effective("meta", "app_id"),
            "client_secret": get_effective("meta", "app_secret"),
        }
    if provider_key == "linkedin_ads":
        return {
            "client_id": get_effective("linkedin", "client_id"),
            "client_secret": get_effective("linkedin", "client_secret"),
        }
    return {}


def _ensure_provider_credentials(provider_key: str) -> Dict[str, str]:
    creds = _credentials_for_provider(provider_key)
    required = OAUTH_PROVIDER_REQUIRED_KEYS.get(provider_key, ["client_id", "client_secret"])
    missing = [k for k in required if not (creds.get(k) or "").strip()]
    if missing:
        raise ValueError(f"{provider_key} OAuth credentials are not configured. Missing: {', '.join(missing)}")
    return creds


def _provider_readiness(provider_key: str) -> Dict[str, Any]:
    creds = _credentials_for_provider(provider_key)
    required = OAUTH_PROVIDER_REQUIRED_KEYS.get(provider_key, ["client_id", "client_secret"])
    missing = [k for k in required if not (creds.get(k) or "").strip()]
    reason = None
    if missing:
        reason = f"Missing required configuration: {', '.join(missing)}"
    return {
        "can_start": len(missing) == 0,
        "missing_config_keys": missing,
        "missing_config_reason": reason,
    }


def _upsert_secret(db: Session, *, workspace_id: str, kind: str, payload: Dict[str, Any], secret_ref: Optional[str]) -> str:
    encrypted = encrypt(json.dumps(payload or {}, separators=(",", ":"), ensure_ascii=True))
    now = utcnow()
    if secret_ref:
        row = db.get(SecretStore, secret_ref)
        if row:
            row.secret_encrypted = encrypted
            row.updated_at = now
            db.add(row)
            db.flush()
            return row.id
    row = SecretStore(
        id=str(uuid.uuid4()),
        workspace_id=workspace_id,
        kind=kind,
        secret_encrypted=encrypted,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row.id


def _load_secret(db: Session, secret_ref: Optional[str]) -> Dict[str, Any]:
    if not secret_ref:
        return {}
    row = db.get(SecretStore, secret_ref)
    if not row:
        return {}
    try:
        return json.loads(decrypt(row.secret_encrypted) or "{}")
    except Exception:
        return {}


def _get_connection(db: Session, *, workspace_id: str, provider_key: str) -> Optional[DataSource]:
    return (
        db.query(DataSource)
        .filter(
            DataSource.workspace_id == workspace_id,
            DataSource.category == "ad_platform",
            DataSource.type == provider_key,
        )
        .first()
    )


def _serialize_connection(item: Optional[DataSource]) -> Dict[str, Any]:
    if not item:
        return {
            "provider_key": None,
            "display_name": None,
            "status": "not_connected",
            "selected_accounts": [],
            "available_accounts": [],
            "selected_accounts_count": 0,
            "last_tested_at": None,
            "last_connected_at": None,
            "last_error": None,
        }
    cfg = item.config_json or {}
    selected = cfg.get("selected_accounts") or []
    available = cfg.get("available_accounts") or []
    return {
        "id": item.id,
        "provider_key": item.type,
        "display_name": item.name,
        "status": item.status,
        "selected_accounts": selected,
        "available_accounts": available,
        "selected_accounts_count": len(selected),
        "last_tested_at": item.last_tested_at.isoformat() if item.last_tested_at else None,
        "last_connected_at": cfg.get("last_connected_at"),
        "last_refreshed_at": cfg.get("last_refreshed_at"),
        "last_error": item.last_error,
        "updated_at": item.updated_at.isoformat() if item.updated_at else None,
    }


def list_oauth_connections(db: Session, *, workspace_id: str) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    for provider_key, label in OAUTH_PROVIDER_LABELS.items():
        row = _get_connection(db, workspace_id=workspace_id, provider_key=provider_key)
        payload = _serialize_connection(row)
        payload["provider_key"] = provider_key
        payload["display_name"] = label
        if row is None:
            payload["status"] = "not_connected"
        payload.update(_provider_readiness(provider_key))
        items.append(payload)
    return {"items": items, "total": len(items)}


def create_oauth_session(
    db: Session,
    *,
    workspace_id: str,
    user_id: str,
    provider_key: str,
    return_url: Optional[str],
) -> Dict[str, str]:
    provider = get_oauth_provider(provider_key)
    state = generate_state()
    verifier = generate_pkce_verifier()
    challenge = build_pkce_challenge(verifier)
    now = utcnow()
    sess = OAuthSession(
        id=str(uuid.uuid4()),
        workspace_id=workspace_id,
        user_id=user_id,
        provider_key=provider.key,
        state=state,
        pkce_verifier_encrypted=encrypt(verifier),
        return_url=(return_url or "").strip() or None,
        created_at=now,
        expires_at=now + timedelta(minutes=OAUTH_SESSION_TTL_MINUTES),
    )
    db.add(sess)
    db.commit()
    return {
        "oauth_session_id": sess.id,
        "state": state,
        "code_verifier": verifier,
        "code_challenge": challenge,
    }


def consume_oauth_session(
    db: Session,
    *,
    provider_key: str,
    state: str,
) -> Tuple[OAuthSession, str]:
    sess = (
        db.query(OAuthSession)
        .filter(OAuthSession.provider_key == provider_key, OAuthSession.state == state)
        .first()
    )
    if not sess:
        raise ValueError("Invalid OAuth state")
    if sess.consumed_at is not None:
        raise ValueError("OAuth state already used")
    if sess.expires_at < utcnow():
        raise ValueError("OAuth session expired")
    verifier = decrypt(sess.pkce_verifier_encrypted)
    if not verifier:
        raise ValueError("OAuth session verifier missing")
    sess.consumed_at = utcnow()
    db.add(sess)
    db.commit()
    return sess, verifier


def _persist_connection_success(
    db: Session,
    *,
    workspace_id: str,
    provider_key: str,
    tokens: Dict[str, Any],
    accounts: List[Dict[str, Any]],
) -> DataSource:
    row = _get_connection(db, workspace_id=workspace_id, provider_key=provider_key)
    now = utcnow()
    if not row:
        row = DataSource(
            id=str(uuid.uuid4()),
            workspace_id=workspace_id,
            category="ad_platform",
            type=provider_key,
            name=OAUTH_PROVIDER_LABELS.get(provider_key, provider_key),
            status="connected",
            config_json={},
            created_at=now,
            updated_at=now,
        )
        db.add(row)
        db.flush()

    secret_payload = {
        "access_token": tokens.get("access_token"),
        "refresh_token": tokens.get("refresh_token"),
        "expires_in": tokens.get("expires_in"),
        "token_type": tokens.get("token_type") or "Bearer",
        "scope": tokens.get("scope"),
        "updated_at": now.isoformat(),
    }
    row.secret_ref = _upsert_secret(
        db,
        workspace_id=workspace_id,
        kind=f"oauth:{provider_key}",
        payload=secret_payload,
        secret_ref=row.secret_ref,
    )

    cfg = dict(row.config_json or {})
    existing_selected = cfg.get("selected_accounts") or []
    cfg["available_accounts"] = accounts
    cfg["selected_accounts"] = existing_selected
    cfg["last_connected_at"] = now.isoformat()
    cfg["oauth_provider"] = provider_key
    row.config_json = cfg
    row.status = "connected"
    row.last_error = None
    row.last_tested_at = now
    row.updated_at = now
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _persist_connection_error(
    db: Session,
    *,
    workspace_id: str,
    provider_key: str,
    message: str,
    needs_reauth: bool,
) -> None:
    row = _get_connection(db, workspace_id=workspace_id, provider_key=provider_key)
    now = utcnow()
    if not row:
        row = DataSource(
            id=str(uuid.uuid4()),
            workspace_id=workspace_id,
            category="ad_platform",
            type=provider_key,
            name=OAUTH_PROVIDER_LABELS.get(provider_key, provider_key),
            status="needs_reauth" if needs_reauth else "error",
            config_json={"oauth_provider": provider_key},
            created_at=now,
            updated_at=now,
            last_error=message,
        )
    else:
        row.status = "needs_reauth" if needs_reauth else "error"
        row.last_error = message
        row.updated_at = now
    db.add(row)
    db.commit()


def build_authorization_url(
    *,
    provider_key: str,
    state: str,
    code_challenge: str,
    redirect_uri: str,
) -> str:
    provider = get_oauth_provider(provider_key)
    creds = _ensure_provider_credentials(provider_key)
    return provider.build_auth_url(
        client_id=creds["client_id"],
        redirect_uri=redirect_uri,
        state=state,
        code_challenge=code_challenge,
    )


def complete_oauth_callback(
    db: Session,
    *,
    provider_key: str,
    code: str,
    state: str,
    redirect_uri: str,
) -> Tuple[DataSource, Optional[str], Optional[OAuthError]]:
    provider = get_oauth_provider(provider_key)
    creds = _ensure_provider_credentials(provider_key)
    sess, verifier = consume_oauth_session(db, provider_key=provider_key, state=state)
    try:
        tokens = provider.exchange_code_for_token(
            code=code,
            code_verifier=verifier,
            redirect_uri=redirect_uri,
            client_id=creds["client_id"],
            client_secret=creds["client_secret"],
        )
        accounts = provider.fetch_accounts(access_token=str(tokens.get("access_token") or ""), credentials=creds)
        row = _persist_connection_success(
            db,
            workspace_id=sess.workspace_id,
            provider_key=provider_key,
            tokens=tokens,
            accounts=accounts,
        )
        return row, sess.return_url, None
    except Exception as exc:
        normalized = provider.normalize_error(exc)
        _persist_connection_error(
            db,
            workspace_id=sess.workspace_id,
            provider_key=provider_key,
            message=normalized.message,
            needs_reauth=normalized.needs_reauth,
        )
        row = _get_connection(db, workspace_id=sess.workspace_id, provider_key=provider_key)
        if row is None:
            raise
        return row, sess.return_url, normalized


def get_connection_or_404(db: Session, *, workspace_id: str, provider_key: str) -> DataSource:
    row = _get_connection(db, workspace_id=workspace_id, provider_key=provider_key)
    if not row:
        raise ValueError("Connection not found")
    return row


def list_provider_accounts(db: Session, *, workspace_id: str, provider_key: str) -> List[Dict[str, Any]]:
    row = get_connection_or_404(db, workspace_id=workspace_id, provider_key=provider_key)
    creds = _ensure_provider_credentials(provider_key)
    secret = _load_secret(db, row.secret_ref)
    access_token = str(secret.get("access_token") or "")
    if not access_token:
        raise RuntimeError("No access token stored")
    provider = get_oauth_provider(provider_key)
    accounts = provider.fetch_accounts(access_token=access_token, credentials=creds)
    cfg = dict(row.config_json or {})
    cfg["available_accounts"] = accounts
    row.config_json = cfg
    row.last_tested_at = utcnow()
    row.updated_at = utcnow()
    db.add(row)
    db.commit()
    return accounts


def select_accounts(
    db: Session,
    *,
    workspace_id: str,
    provider_key: str,
    account_ids: List[str],
) -> Dict[str, Any]:
    row = get_connection_or_404(db, workspace_id=workspace_id, provider_key=provider_key)
    cfg = dict(row.config_json or {})
    available = cfg.get("available_accounts") or []
    available_ids = {str(a.get("id")) for a in available if a.get("id") is not None}
    clean_ids = [str(v).strip() for v in account_ids if str(v).strip()]
    if available_ids:
        clean_ids = [v for v in clean_ids if v in available_ids]
    cfg["selected_accounts"] = clean_ids
    row.config_json = cfg
    row.updated_at = utcnow()
    db.add(row)
    db.commit()
    db.refresh(row)
    return _serialize_connection(row)


def test_connection_health(db: Session, *, workspace_id: str, provider_key: str) -> Dict[str, Any]:
    row = get_connection_or_404(db, workspace_id=workspace_id, provider_key=provider_key)
    provider = get_oauth_provider(provider_key)
    creds = _ensure_provider_credentials(provider_key)
    secret = _load_secret(db, row.secret_ref)
    access_token = str(secret.get("access_token") or "")
    refresh_token = str(secret.get("refresh_token") or "")
    now = utcnow()

    def _set_status(message: Optional[str], status: str) -> None:
        row.status = status
        row.last_error = message
        row.last_tested_at = now
        row.updated_at = now
        db.add(row)
        db.commit()

    try:
        accounts = provider.fetch_accounts(access_token=access_token, credentials=creds)
        cfg = dict(row.config_json or {})
        cfg["available_accounts"] = accounts
        cfg["last_refreshed_at"] = now.isoformat()
        row.config_json = cfg
        _set_status(None, "connected")
        return {"ok": True, "status": row.status, "accounts": accounts}
    except Exception as exc:
        normalized = provider.normalize_error(exc)
        if normalized.needs_reauth and refresh_token:
            try:
                refreshed = provider.refresh_access_token(
                    refresh_token=refresh_token,
                    client_id=creds["client_id"],
                    client_secret=creds["client_secret"],
                )
                merged_secret = dict(secret)
                merged_secret["access_token"] = refreshed.get("access_token")
                if refreshed.get("refresh_token"):
                    merged_secret["refresh_token"] = refreshed.get("refresh_token")
                merged_secret["expires_in"] = refreshed.get("expires_in")
                merged_secret["token_type"] = refreshed.get("token_type") or merged_secret.get("token_type") or "Bearer"
                merged_secret["scope"] = refreshed.get("scope") or merged_secret.get("scope")
                merged_secret["updated_at"] = now.isoformat()
                row.secret_ref = _upsert_secret(
                    db,
                    workspace_id=workspace_id,
                    kind=f"oauth:{provider_key}",
                    payload=merged_secret,
                    secret_ref=row.secret_ref,
                )
                accounts = provider.fetch_accounts(access_token=str(merged_secret.get("access_token") or ""), credentials=creds)
                cfg = dict(row.config_json or {})
                cfg["available_accounts"] = accounts
                cfg["last_refreshed_at"] = now.isoformat()
                row.config_json = cfg
                _set_status(None, "connected")
                return {"ok": True, "status": row.status, "accounts": accounts, "refreshed": True}
            except Exception as refresh_exc:
                refresh_err = provider.normalize_error(refresh_exc)
                _set_status(refresh_err.message, "needs_reauth" if refresh_err.needs_reauth else "error")
                return {
                    "ok": False,
                    "status": row.status,
                    "error": {
                        "code": refresh_err.code,
                        "message": refresh_err.message,
                        "retryable": refresh_err.retryable,
                        "needs_reauth": refresh_err.needs_reauth,
                        "configuration": refresh_err.configuration,
                    },
                }

        _set_status(normalized.message, "needs_reauth" if normalized.needs_reauth else "error")
        return {
            "ok": False,
            "status": row.status,
            "error": {
                "code": normalized.code,
                "message": normalized.message,
                "retryable": normalized.retryable,
                "needs_reauth": normalized.needs_reauth,
                "configuration": normalized.configuration,
            },
        }


def disconnect_connection(db: Session, *, workspace_id: str, provider_key: str) -> Dict[str, Any]:
    row = get_connection_or_404(db, workspace_id=workspace_id, provider_key=provider_key)
    row.status = "disabled"
    row.last_error = None
    cfg = dict(row.config_json or {})
    cfg["selected_accounts"] = []
    row.config_json = cfg
    row.updated_at = utcnow()
    db.add(row)
    db.commit()
    db.refresh(row)
    return _serialize_connection(row)


def get_access_token_for_provider(db: Session, *, workspace_id: str, provider_key: str) -> Optional[str]:
    row = _get_connection(db, workspace_id=workspace_id, provider_key=provider_key)
    if not row:
        return None
    secret = _load_secret(db, row.secret_ref)
    token = str(secret.get("access_token") or "").strip()
    return token or None
