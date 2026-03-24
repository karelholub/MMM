from dataclasses import dataclass
from typing import Dict, List, Optional

from fastapi import Depends, Header, HTTPException, Request

from app.db import get_db
from app.services_access_control import DEFAULT_WORKSPACE_ID
from app.services_access_control import PERMISSIONS as RBAC_PERMISSIONS
from app.services_auth import SESSION_COOKIE_NAME, resolve_auth_context


@dataclass
class PermissionContext:
    user_id: str
    workspace_id: str
    permissions: set[str]
    source: str  # session | legacy_header


_ALL_PERMISSION_KEYS = {p["key"] for p in RBAC_PERMISSIONS}
_VIEW_PERMISSIONS = {p for p in _ALL_PERMISSION_KEYS if p.endswith(".view")}
_EDITOR_PERMISSIONS = _ALL_PERMISSION_KEYS - {"users.manage", "roles.manage", "audit.view"}
_LEGACY_ROLE_PERMISSION_MAP: Dict[str, set[str]] = {
    "viewer": set(_VIEW_PERMISSIONS),
    "analyst": set(_EDITOR_PERMISSIONS),
    "editor": set(_EDITOR_PERMISSIONS),
    "power_user": set(_ALL_PERMISSION_KEYS),
    "admin": set(_ALL_PERMISSION_KEYS),
}
_LEGACY_HEADERLESS_VIEWER_PREFIXES = (
    "/api/settings",
    "/api/taxonomy",
)


def get_permission_context(
    request: Request,
    db=Depends(get_db),
    x_user_id: Optional[str] = Header(None, alias="X-User-Id"),
    x_user_role: Optional[str] = Header(None, alias="X-User-Role"),
) -> PermissionContext:
    raw_session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if raw_session_id:
        ctx = resolve_auth_context(db, raw_session_id=raw_session_id)
        if not ctx:
            raise HTTPException(
                status_code=401,
                detail={"code": "auth_required", "message": "Authentication required"},
            )
        return PermissionContext(
            user_id=ctx.user.id,
            workspace_id=ctx.workspace.id,
            permissions=set(ctx.permissions),
            source="session",
        )

    # Legacy compatibility is intentionally narrow: only keep headerless
    # viewer access for settings/taxonomy read surfaces that still rely on it.
    if x_user_role is None and x_user_id is None:
        role = "viewer" if request.url.path.startswith(_LEGACY_HEADERLESS_VIEWER_PREFIXES) else ""
    else:
        role = (x_user_role or "viewer").strip().lower()
    perms = _LEGACY_ROLE_PERMISSION_MAP.get(role, set())
    return PermissionContext(
        user_id=(x_user_id or "system"),
        workspace_id=DEFAULT_WORKSPACE_ID,
        permissions=set(perms),
        source="legacy_header",
    )


def has_permission(ctx: PermissionContext, permission_key: str) -> bool:
    return permission_key in ctx.permissions


def require_permission(permission_key: str):
    def _dep(ctx: PermissionContext = Depends(get_permission_context)) -> PermissionContext:
        if not has_permission(ctx, permission_key):
            raise HTTPException(
                status_code=403,
                detail={
                    "code": "permission_denied",
                    "message": f"Missing permission: {permission_key}",
                    "permission": permission_key,
                },
            )
        return ctx

    return _dep


def require_any_permission(permission_keys: List[str]):
    keys = tuple(dict.fromkeys(permission_keys))

    def _dep(ctx: PermissionContext = Depends(get_permission_context)) -> PermissionContext:
        for key in keys:
            if key in ctx.permissions:
                return ctx
        raise HTTPException(
            status_code=403,
            detail={
                "code": "forbidden",
                "message": "Missing required permission",
                "permission_any_of": list(keys),
            },
        )

    return _dep


def workspace_scope_or_403(ctx: PermissionContext, workspace_id: Optional[str]) -> str:
    target = (workspace_id or ctx.workspace_id or DEFAULT_WORKSPACE_ID).strip() or DEFAULT_WORKSPACE_ID
    if ctx.source == "session" and target != ctx.workspace_id:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "workspace_scope_denied",
                "message": "Requested workspace is outside current session scope",
                "workspace_id": target,
            },
        )
    return target
