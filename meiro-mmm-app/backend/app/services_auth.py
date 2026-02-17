"""Session auth and workspace authorization helpers."""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional, Set

from fastapi import HTTPException, Request
from sqlalchemy.orm import Session

from .models_config_dq import (
    AuthSession,
    Role,
    RolePermission,
    User,
    Workspace,
    WorkspaceMembership,
)
from .services_access_control import DEFAULT_WORKSPACE_ID, ensure_access_control_seed_data

SESSION_TTL_HOURS = 24 * 7
SESSION_COOKIE_NAME = "mmm_session"
CSRF_HEADER_NAME = "X-CSRF-Token"
PASSWORD_SCHEME = "pbkdf2_sha256_v1"
PASSWORD_ITERATIONS = 240_000


def _now() -> datetime:
    return datetime.utcnow()


def _token(length: int = 48) -> str:
    return secrets.token_urlsafe(length)[:length]


def _hash_token(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _normalize_username(raw: Optional[str]) -> str:
    return (raw or "").strip().lower()


def _derive_username_from_email(email: str) -> str:
    local = email.split("@")[0].strip().lower()
    safe = "".join(ch for ch in local if ch.isalnum() or ch in {"_", "-", "."})
    return safe[:64] or f"user-{_token(8)}"


def _unique_username(db: Session, preferred: str) -> str:
    base = _normalize_username(preferred)[:64] or f"user-{_token(8)}"
    probe = base
    idx = 1
    while db.query(User.id).filter(User.username == probe).first():
        suffix = str(idx)
        probe = f"{base[: max(1, 64 - len(suffix) - 1)]}-{suffix}"
        idx += 1
    return probe


def hash_password(password: str) -> str:
    if not password:
        raise ValueError("password is required")
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        PASSWORD_ITERATIONS,
    )
    return f"{PASSWORD_SCHEME}${PASSWORD_ITERATIONS}${salt}${dk.hex()}"


def verify_password(password: str, encoded_hash: Optional[str]) -> bool:
    if not password or not encoded_hash:
        return False
    try:
        scheme, iters_raw, salt, digest = encoded_hash.split("$", 3)
        if scheme != PASSWORD_SCHEME:
            return False
        iters = int(iters_raw)
        probe = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt.encode("utf-8"),
            iters,
        ).hex()
        return hmac.compare_digest(probe, digest)
    except Exception:
        return False


@dataclass
class AuthContext:
    user: User
    workspace: Workspace
    membership: WorkspaceMembership
    role: Optional[Role]
    permissions: Set[str]
    session: AuthSession


def ensure_user_and_membership(
    db: Session,
    *,
    email: str,
    name: Optional[str],
    workspace_id: str = DEFAULT_WORKSPACE_ID,
) -> User:
    ensure_access_control_seed_data(db)

    normalized_email = (email or "").strip().lower()
    if not normalized_email:
        raise ValueError("email is required")
    user = db.query(User).filter(User.email == normalized_email).first()
    if not user:
        preferred_username = _derive_username_from_email(normalized_email)
        user = User(
            id=_token(36),
            username=_unique_username(db, preferred_username),
            email=normalized_email,
            name=(name or "").strip() or normalized_email.split("@")[0],
            status="active",
            auth_provider="local",
            created_at=_now(),
            updated_at=_now(),
        )
        db.add(user)
        db.flush()
    elif not user.username:
        user.username = _unique_username(db, _derive_username_from_email(normalized_email))
        user.updated_at = _now()
        db.add(user)

    workspace = db.get(Workspace, workspace_id)
    if not workspace:
        workspace = Workspace(
            id=workspace_id,
            name="Default Workspace",
            slug="default",
            created_at=_now(),
            updated_at=_now(),
        )
        db.add(workspace)
        db.flush()

    membership = (
        db.query(WorkspaceMembership)
        .filter(
            WorkspaceMembership.workspace_id == workspace_id,
            WorkspaceMembership.user_id == user.id,
        )
        .first()
    )
    if not membership:
        default_role = (
            db.query(Role)
            .filter(Role.workspace_id.is_(None), Role.name == "Viewer", Role.is_system == True)  # noqa: E712
            .first()
        )
        membership = WorkspaceMembership(
            id=_token(36),
            workspace_id=workspace_id,
            user_id=user.id,
            role_id=default_role.id if default_role else None,
            status="active",
            created_at=_now(),
            updated_at=_now(),
        )
        db.add(membership)
    elif membership.status != "active":
        membership.status = "active"
        membership.updated_at = _now()
        db.add(membership)

    db.commit()
    db.refresh(user)
    return user


def ensure_local_password_seed_users(db: Session, *, workspace_id: str = DEFAULT_WORKSPACE_ID) -> None:
    """Ensure default local credential users exist for admin/editor/viewer flows."""
    ensure_access_control_seed_data(db)
    workspace = db.get(Workspace, workspace_id)
    if not workspace:
        workspace = Workspace(
            id=workspace_id,
            name="Default Workspace",
            slug="default",
            created_at=_now(),
            updated_at=_now(),
        )
        db.add(workspace)
        db.flush()

    role_by_name = {
        role.name: role
        for role in db.query(Role).filter(Role.workspace_id.is_(None), Role.is_system == True).all()  # noqa: E712
    }

    seed_rows = [
        {
            "username": os.getenv("BOOTSTRAP_ADMIN_USERNAME", "admin"),
            "email": os.getenv("BOOTSTRAP_ADMIN_EMAIL", "admin@example.com"),
            "name": "Admin",
            "password": os.getenv("BOOTSTRAP_ADMIN_PASSWORD", "admin"),
            "role": "Admin",
        },
        {
            "username": os.getenv("BOOTSTRAP_EDITOR_USERNAME", "editor"),
            "email": os.getenv("BOOTSTRAP_EDITOR_EMAIL", "editor@example.com"),
            "name": "Editor",
            "password": os.getenv("BOOTSTRAP_EDITOR_PASSWORD", "editor"),
            "role": "Analyst",
        },
        {
            "username": os.getenv("BOOTSTRAP_VIEWER_USERNAME", "viewer"),
            "email": os.getenv("BOOTSTRAP_VIEWER_EMAIL", "viewer@example.com"),
            "name": "Viewer",
            "password": os.getenv("BOOTSTRAP_VIEWER_PASSWORD", "viewer"),
            "role": "Viewer",
        },
    ]

    now = _now()
    for row in seed_rows:
        username = _normalize_username(row["username"])
        email = row["email"].strip().lower()
        user = (
            db.query(User)
            .filter((User.username == username) | (User.email == email))
            .first()
        )
        if not user:
            user = User(
                id=_token(36),
                username=_unique_username(db, username),
                email=email,
                name=row["name"],
                status="active",
                auth_provider="password",
                password_hash=hash_password(row["password"]),
                password_updated_at=now,
                created_at=now,
                updated_at=now,
            )
            db.add(user)
            db.flush()
        else:
            changed = False
            if not user.username:
                user.username = username
                changed = True
            if not user.password_hash:
                user.password_hash = hash_password(row["password"])
                user.password_updated_at = now
                changed = True
            if user.auth_provider in (None, "", "local"):
                user.auth_provider = "password"
                changed = True
            if changed:
                user.updated_at = now
                db.add(user)

        role = role_by_name.get(row["role"])
        membership = (
            db.query(WorkspaceMembership)
            .filter(
                WorkspaceMembership.workspace_id == workspace_id,
                WorkspaceMembership.user_id == user.id,
            )
            .first()
        )
        if not membership:
            membership = WorkspaceMembership(
                id=_token(36),
                workspace_id=workspace_id,
                user_id=user.id,
                role_id=role.id if role else None,
                status="active",
                created_at=now,
                updated_at=now,
            )
            db.add(membership)
        else:
            if membership.status != "active":
                membership.status = "active"
            if role and membership.role_id != role.id:
                membership.role_id = role.id
            membership.updated_at = now
            db.add(membership)

    db.commit()


def authenticate_local_user(
    db: Session,
    *,
    identifier: str,
    password: str,
    workspace_id: str = DEFAULT_WORKSPACE_ID,
) -> Optional[User]:
    ident = (identifier or "").strip().lower()
    if not ident or not password:
        return None
    user = (
        db.query(User)
        .filter((User.username == ident) | (User.email == ident))
        .first()
    )
    if not user or user.status != "active" or not verify_password(password, user.password_hash):
        return None
    membership = (
        db.query(WorkspaceMembership)
        .filter(
            WorkspaceMembership.workspace_id == workspace_id,
            WorkspaceMembership.user_id == user.id,
            WorkspaceMembership.status == "active",
        )
        .first()
    )
    if not membership:
        return None
    return user


def create_session(
    db: Session,
    *,
    user: User,
    workspace_id: str,
    request: Optional[Request] = None,
    ttl_hours: int = SESSION_TTL_HOURS,
) -> Dict[str, str]:
    raw_session_id = _token(64)
    raw_csrf = _token(48)
    now = _now()
    row = AuthSession(
        id=_hash_token(raw_session_id),
        user_id=user.id,
        workspace_id=workspace_id,
        csrf_token=_hash_token(raw_csrf),
        expires_at=now + timedelta(hours=max(1, int(ttl_hours))),
        created_at=now,
        last_seen_at=now,
        revoked_at=None,
        ip=(request.client.host if request and request.client else None),
        user_agent=(request.headers.get("user-agent")[:512] if request else None),
    )
    db.add(row)
    user.last_login_at = now
    user.updated_at = now
    db.add(user)
    db.commit()
    return {"session_id": raw_session_id, "csrf_token": raw_csrf}


def revoke_session(db: Session, raw_session_id: str) -> bool:
    sid = _hash_token(raw_session_id)
    row = db.get(AuthSession, sid)
    if not row:
        return False
    row.revoked_at = _now()
    db.add(row)
    db.commit()
    return True


def revoke_all_user_sessions(db: Session, user_id: str) -> int:
    rows = (
        db.query(AuthSession)
        .filter(AuthSession.user_id == user_id, AuthSession.revoked_at.is_(None))
        .all()
    )
    now = _now()
    for row in rows:
        row.revoked_at = now
        db.add(row)
    db.commit()
    return len(rows)


def _permissions_for_role(db: Session, role_id: Optional[str]) -> Set[str]:
    if not role_id:
        return set()
    perms = db.query(RolePermission.permission_key).filter(RolePermission.role_id == role_id).all()
    return {p[0] for p in perms}


def resolve_auth_context(
    db: Session,
    *,
    raw_session_id: Optional[str],
) -> Optional[AuthContext]:
    if not raw_session_id:
        return None
    sid = _hash_token(raw_session_id)
    session_row = db.get(AuthSession, sid)
    if not session_row:
        return None
    now = _now()
    if session_row.revoked_at is not None or session_row.expires_at <= now:
        return None

    user = db.get(User, session_row.user_id)
    workspace = db.get(Workspace, session_row.workspace_id)
    if not user or user.status != "active" or not workspace:
        return None
    membership = (
        db.query(WorkspaceMembership)
        .filter(
            WorkspaceMembership.workspace_id == workspace.id,
            WorkspaceMembership.user_id == user.id,
            WorkspaceMembership.status == "active",
        )
        .first()
    )
    if not membership:
        return None
    role = db.get(Role, membership.role_id) if membership.role_id else None
    perms = _permissions_for_role(db, membership.role_id)
    session_row.last_seen_at = now
    db.add(session_row)
    db.commit()
    db.refresh(session_row)
    return AuthContext(
        user=user,
        workspace=workspace,
        membership=membership,
        role=role,
        permissions=perms,
        session=session_row,
    )


def require_auth_context(db: Session, request: Request) -> AuthContext:
    raw_session_id = request.cookies.get(SESSION_COOKIE_NAME)
    ctx = resolve_auth_context(db, raw_session_id=raw_session_id)
    if not ctx:
        raise HTTPException(status_code=401, detail="Authentication required")
    return ctx


def verify_csrf(db: Session, request: Request) -> None:
    raw_session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if not raw_session_id:
        return
    ctx = resolve_auth_context(db, raw_session_id=raw_session_id)
    if not ctx:
        raise HTTPException(status_code=401, detail="Authentication required")
    header_token = (request.headers.get(CSRF_HEADER_NAME) or "").strip()
    if not header_token:
        raise HTTPException(status_code=403, detail="Missing CSRF token")
    if _hash_token(header_token) != ctx.session.csrf_token:
        raise HTTPException(status_code=403, detail="Invalid CSRF token")


def issue_csrf_token(db: Session, raw_session_id: Optional[str]) -> Optional[str]:
    """Issue and persist a new CSRF token for an existing active session."""
    if not raw_session_id:
        return None
    sid = _hash_token(raw_session_id)
    session_row = db.get(AuthSession, sid)
    if not session_row:
        return None
    now = _now()
    if session_row.revoked_at is not None or session_row.expires_at <= now:
        return None
    raw_csrf = _token(48)
    session_row.csrf_token = _hash_token(raw_csrf)
    session_row.last_seen_at = now
    db.add(session_row)
    db.commit()
    return raw_csrf
