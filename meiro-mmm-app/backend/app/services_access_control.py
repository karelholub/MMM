"""Access-control schema seeds and helpers."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy.orm import Session

from .models_config_dq import Permission, Role, RolePermission, Workspace


DEFAULT_WORKSPACE_ID = "default"


def _new_id() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.utcnow()


PERMISSIONS: List[Dict[str, str]] = [
    {"key": "journeys.view", "description": "View journeys analytics and paths", "category": "analytics"},
    {"key": "journeys.manage", "description": "Manage journey definitions and settings", "category": "analytics"},
    {"key": "funnels.view", "description": "View funnel analytics", "category": "analytics"},
    {"key": "funnels.manage", "description": "Manage funnels and diagnostics", "category": "analytics"},
    {"key": "attribution.view", "description": "View attribution dashboards", "category": "analytics"},
    {"key": "attribution.manage", "description": "Manage attribution configurations", "category": "analytics"},
    {"key": "alerts.view", "description": "View alerts", "category": "analytics"},
    {"key": "alerts.manage", "description": "Manage alert rules and states", "category": "analytics"},
    {"key": "audiences.manage", "description": "Manage audience activation", "category": "activation"},
    {"key": "exports.manage", "description": "Manage exports and downstream delivery", "category": "activation"},
    {"key": "settings.view", "description": "View settings", "category": "admin"},
    {"key": "settings.manage", "description": "Manage workspace settings", "category": "admin"},
    {"key": "users.manage", "description": "Manage users and memberships", "category": "admin"},
    {"key": "roles.manage", "description": "Manage custom roles and role assignments", "category": "admin"},
    {"key": "audit.view", "description": "View security/admin audit logs", "category": "admin"},
]


SYSTEM_ROLE_PERMISSIONS: Dict[str, List[str]] = {
    "Admin": [p["key"] for p in PERMISSIONS],
    "Analyst": [
        "journeys.view",
        "journeys.manage",
        "funnels.view",
        "funnels.manage",
        "attribution.view",
        "attribution.manage",
        "alerts.view",
        "alerts.manage",
        "settings.view",
    ],
    "Viewer": [
        "journeys.view",
        "funnels.view",
        "attribution.view",
        "alerts.view",
        "settings.view",
    ],
}


def ensure_access_control_seed_data(db: Session) -> Dict[str, Any]:
    inserted_permissions = 0
    inserted_roles = 0
    inserted_role_permissions = 0
    inserted_workspaces = 0

    ws = db.get(Workspace, DEFAULT_WORKSPACE_ID)
    if ws is None:
        db.add(
            Workspace(
                id=DEFAULT_WORKSPACE_ID,
                name="Default Workspace",
                slug="default",
                created_at=_now(),
                updated_at=_now(),
            )
        )
        inserted_workspaces += 1
        db.flush()

    for perm in PERMISSIONS:
        existing = db.get(Permission, perm["key"])
        if existing:
            if (
                existing.description != perm["description"]
                or existing.category != perm["category"]
            ):
                existing.description = perm["description"]
                existing.category = perm["category"]
                db.add(existing)
            continue
        db.add(
            Permission(
                key=perm["key"],
                description=perm["description"],
                category=perm["category"],
                created_at=_now(),
            )
        )
        inserted_permissions += 1
    db.flush()

    role_by_name: Dict[str, Role] = {}
    for role_name, perms in SYSTEM_ROLE_PERMISSIONS.items():
        role = (
            db.query(Role)
            .filter(Role.workspace_id.is_(None), Role.name == role_name, Role.is_system == True)  # noqa: E712
            .first()
        )
        if not role:
            role = Role(
                id=_new_id(),
                workspace_id=None,
                name=role_name,
                description=f"System {role_name} role",
                is_system=True,
                created_at=_now(),
                updated_at=_now(),
            )
            db.add(role)
            db.flush()
            inserted_roles += 1
        role_by_name[role_name] = role

        existing_perm_keys = {
            rp.permission_key
            for rp in db.query(RolePermission).filter(RolePermission.role_id == role.id).all()
        }
        for perm_key in perms:
            if perm_key in existing_perm_keys:
                continue
            db.add(
                RolePermission(
                    role_id=role.id,
                    permission_key=perm_key,
                    created_at=_now(),
                )
            )
            inserted_role_permissions += 1

    db.commit()
    return {
        "permissions_inserted": inserted_permissions,
        "roles_inserted": inserted_roles,
        "role_permissions_inserted": inserted_role_permissions,
        "workspaces_inserted": inserted_workspaces,
        "system_roles": sorted(role_by_name.keys()),
    }

