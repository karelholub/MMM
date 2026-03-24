from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import func

from app.models_config_dq import (
    Invitation as ORMInvitation,
    Permission as ORMPermission,
    Role as ORMRole,
    RolePermission as ORMRolePermission,
    SecurityAuditLog as ORMSecurityAuditLog,
    User as ORMUser,
    WorkspaceMembership,
)
from app.modules.admin_access.schemas import (
    AdminInvitationCreatePayload,
    AdminMembershipUpdatePayload,
    AdminRoleCreatePayload,
    AdminRoleUpdatePayload,
    AdminUserUpdatePayload,
    InvitationAcceptPayload,
)
from app.services_auth import revoke_all_user_sessions


def _parse_admin_audit_datetime(value: Optional[str], *, field_name: str) -> Optional[datetime]:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}; expected ISO-8601 datetime")
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    require_permission_dependency: Callable[[str], Callable[..., Any]],
    require_any_permission_dependency: Callable[[list[str]], Callable[..., Any]],
    workspace_scope_or_403_fn: Callable[..., str],
    get_settings_obj: Callable[[], Any],
    write_security_audit_fn: Callable[..., None],
    invite_token_fn: Callable[[], str],
    hash_invite_token_fn: Callable[[str], str],
) -> APIRouter:
    router = APIRouter(tags=["admin_access"])

    @router.get("/api/admin/permissions")
    def admin_list_permissions(
        category: Optional[str] = Query(None),
        _ctx=Depends(require_permission_dependency("roles.manage")),
        db=Depends(get_db_dependency),
    ):
        q = db.query(ORMPermission)
        if category:
            q = q.filter(ORMPermission.category == category)
        rows = q.order_by(ORMPermission.category.asc(), ORMPermission.key.asc()).all()
        return [{"key": r.key, "description": r.description, "category": r.category} for r in rows]

    @router.get("/api/admin/audit-log")
    def admin_list_audit_log(
        workspaceId: Optional[str] = Query(None),
        action: Optional[str] = Query(None),
        actor: Optional[str] = Query(None, description="Actor name/email contains"),
        date_from: Optional[str] = Query(None, description="ISO datetime"),
        date_to: Optional[str] = Query(None, description="ISO datetime"),
        page: int = Query(1, ge=1),
        page_size: int = Query(50, ge=1, le=200),
        _ctx=Depends(require_any_permission_dependency(["audit.view", "settings.manage"])),
        db=Depends(get_db_dependency),
    ):
        if not getattr(get_settings_obj().feature_flags, "audit_log_enabled", False):
            raise HTTPException(status_code=404, detail="audit_log_enabled flag is off")
        workspace_id = workspace_scope_or_403_fn(_ctx, workspaceId)
        from_dt = _parse_admin_audit_datetime(date_from, field_name="date_from")
        to_dt = _parse_admin_audit_datetime(date_to, field_name="date_to")
        if from_dt and to_dt and from_dt > to_dt:
            raise HTTPException(status_code=400, detail="date_from must be <= date_to")

        q = (
            db.query(ORMSecurityAuditLog, ORMUser.name.label("actor_name"), ORMUser.email.label("actor_email"))
            .outerjoin(ORMUser, ORMUser.id == ORMSecurityAuditLog.actor_user_id)
            .filter(ORMSecurityAuditLog.workspace_id == workspace_id)
        )
        if action and action.strip():
            q = q.filter(ORMSecurityAuditLog.action_key.ilike(f"%{action.strip()}%"))
        if actor and actor.strip():
            actor_like = actor.strip()
            q = q.filter((ORMUser.name.ilike(f"%{actor_like}%")) | (ORMUser.email.ilike(f"%{actor_like}%")))
        if from_dt:
            q = q.filter(ORMSecurityAuditLog.created_at >= from_dt)
        if to_dt:
            q = q.filter(ORMSecurityAuditLog.created_at <= to_dt)

        total = int(q.count() or 0)
        rows = (
            q.order_by(ORMSecurityAuditLog.created_at.desc(), ORMSecurityAuditLog.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
            .all()
        )
        return {
            "items": [
                {
                    "id": row.id,
                    "workspace_id": row.workspace_id,
                    "actor_user_id": row.actor_user_id,
                    "actor_name": actor_name,
                    "actor_email": actor_email,
                    "action_key": row.action_key,
                    "target_type": row.target_type,
                    "target_id": row.target_id,
                    "metadata_json": row.metadata_json or {},
                    "ip": row.ip,
                    "user_agent": row.user_agent,
                    "created_at": row.created_at,
                }
                for row, actor_name, actor_email in rows
            ],
            "page": page,
            "page_size": page_size,
            "total": total,
        }

    @router.get("/api/admin/roles")
    def admin_list_roles(
        workspaceId: Optional[str] = Query(None),
        _ctx=Depends(require_permission_dependency("roles.manage")),
        db=Depends(get_db_dependency),
    ):
        workspace_id = workspace_scope_or_403_fn(_ctx, workspaceId)
        roles = (
            db.query(ORMRole)
            .filter((ORMRole.workspace_id == workspace_id) | (ORMRole.workspace_id.is_(None)))
            .order_by(ORMRole.is_system.desc(), ORMRole.name.asc())
            .all()
        )
        out = []
        for role in roles:
            member_count = (
                db.query(func.count(WorkspaceMembership.id))
                .filter(
                    WorkspaceMembership.workspace_id == workspace_id,
                    WorkspaceMembership.role_id == role.id,
                    WorkspaceMembership.status == "active",
                )
                .scalar()
                or 0
            )
            perm_keys = [
                row[0]
                for row in db.query(ORMRolePermission.permission_key).filter(ORMRolePermission.role_id == role.id).all()
            ]
            out.append(
                {
                    "id": role.id,
                    "workspace_id": role.workspace_id,
                    "name": role.name,
                    "description": role.description,
                    "is_system": bool(role.is_system),
                    "member_count": int(member_count),
                    "permission_keys": sorted(perm_keys),
                    "created_at": role.created_at,
                    "updated_at": role.updated_at,
                }
            )
        return {"items": out}

    @router.post("/api/admin/roles")
    def admin_create_role(
        body: AdminRoleCreatePayload,
        request: Request,
        _ctx=Depends(require_permission_dependency("roles.manage")),
        db=Depends(get_db_dependency),
    ):
        if not get_settings_obj().feature_flags.custom_roles_enabled:
            raise HTTPException(status_code=403, detail="custom_roles_enabled flag is off")
        workspace_id = workspace_scope_or_403_fn(_ctx, body.workspace_id)
        name = body.name.strip()
        if not name:
            raise HTTPException(status_code=400, detail="name is required")
        exists = db.query(ORMRole).filter(ORMRole.workspace_id == workspace_id, ORMRole.name == name).first()
        if exists:
            raise HTTPException(status_code=409, detail="Role name already exists in workspace")
        known_permissions = {p.key for p in db.query(ORMPermission).all()}
        unknown = [k for k in (body.permission_keys or []) if k not in known_permissions]
        if unknown:
            raise HTTPException(status_code=400, detail=f"Unknown permission keys: {', '.join(sorted(unknown))}")
        role = ORMRole(
            id=str(uuid.uuid4()),
            workspace_id=workspace_id,
            name=name,
            description=body.description,
            is_system=False,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(role)
        db.flush()
        for key in sorted(set(body.permission_keys or [])):
            db.add(ORMRolePermission(role_id=role.id, permission_key=key, created_at=datetime.utcnow()))
        db.commit()
        write_security_audit_fn(
            db,
            actor_user_id=_ctx.user_id,
            workspace_id=workspace_id,
            action_key="role.created",
            target_type="role",
            target_id=role.id,
            metadata={"name": role.name, "permission_keys": sorted(set(body.permission_keys or []))},
            request=request,
        )
        return {"id": role.id, "name": role.name, "workspace_id": role.workspace_id}

    @router.put("/api/admin/roles/{role_id}")
    def admin_update_role(
        role_id: str,
        body: AdminRoleUpdatePayload,
        request: Request,
        _ctx=Depends(require_permission_dependency("roles.manage")),
        db=Depends(get_db_dependency),
    ):
        if not get_settings_obj().feature_flags.custom_roles_enabled:
            raise HTTPException(status_code=403, detail="custom_roles_enabled flag is off")
        role = db.get(ORMRole, role_id)
        if not role:
            raise HTTPException(status_code=404, detail="Role not found")
        workspace_id = workspace_scope_or_403_fn(_ctx, role.workspace_id or _ctx.workspace_id)
        if role.is_system:
            raise HTTPException(status_code=400, detail="System roles are read-only")
        if body.name is not None:
            next_name = body.name.strip()
            if not next_name:
                raise HTTPException(status_code=400, detail="name is required")
            dupe = (
                db.query(ORMRole)
                .filter(ORMRole.workspace_id == workspace_id, ORMRole.name == next_name, ORMRole.id != role.id)
                .first()
            )
            if dupe:
                raise HTTPException(status_code=409, detail="Role name already exists in workspace")
            role.name = next_name
        if body.description is not None:
            role.description = body.description
        if body.permission_keys is not None:
            known_permissions = {p.key for p in db.query(ORMPermission).all()}
            unknown = [k for k in body.permission_keys if k not in known_permissions]
            if unknown:
                raise HTTPException(status_code=400, detail=f"Unknown permission keys: {', '.join(sorted(unknown))}")
            db.query(ORMRolePermission).filter(ORMRolePermission.role_id == role.id).delete(synchronize_session=False)
            for key in sorted(set(body.permission_keys)):
                db.add(ORMRolePermission(role_id=role.id, permission_key=key, created_at=datetime.utcnow()))
        role.updated_at = datetime.utcnow()
        db.add(role)
        db.commit()
        write_security_audit_fn(
            db,
            actor_user_id=_ctx.user_id,
            workspace_id=workspace_id,
            action_key="role.updated",
            target_type="role",
            target_id=role.id,
            metadata={"name": role.name},
            request=request,
        )
        return {"id": role.id, "name": role.name}

    @router.delete("/api/admin/roles/{role_id}")
    def admin_delete_role(
        role_id: str,
        request: Request,
        _ctx=Depends(require_permission_dependency("roles.manage")),
        db=Depends(get_db_dependency),
    ):
        if not get_settings_obj().feature_flags.custom_roles_enabled:
            raise HTTPException(status_code=403, detail="custom_roles_enabled flag is off")
        role = db.get(ORMRole, role_id)
        if not role:
            raise HTTPException(status_code=404, detail="Role not found")
        workspace_id = workspace_scope_or_403_fn(_ctx, role.workspace_id or _ctx.workspace_id)
        if role.is_system:
            raise HTTPException(status_code=400, detail="System roles cannot be deleted")
        active_members = (
            db.query(func.count(WorkspaceMembership.id))
            .filter(
                WorkspaceMembership.workspace_id == workspace_id,
                WorkspaceMembership.role_id == role.id,
                WorkspaceMembership.status == "active",
            )
            .scalar()
            or 0
        )
        if int(active_members) > 0:
            raise HTTPException(status_code=400, detail="Role has active members; reassign them first")
        db.query(ORMRolePermission).filter(ORMRolePermission.role_id == role.id).delete(synchronize_session=False)
        db.delete(role)
        db.commit()
        write_security_audit_fn(
            db,
            actor_user_id=_ctx.user_id,
            workspace_id=workspace_id,
            action_key="role.deleted",
            target_type="role",
            target_id=role_id,
            metadata={},
            request=request,
        )
        return {"id": role_id, "deleted": True}

    @router.get("/api/admin/users")
    def admin_list_users(
        search: Optional[str] = Query(None),
        status: Optional[str] = Query(None),
        workspaceId: Optional[str] = Query(None),
        _ctx=Depends(require_permission_dependency("users.manage")),
        db=Depends(get_db_dependency),
    ):
        workspace_id = workspace_scope_or_403_fn(_ctx, workspaceId)
        q = (
            db.query(ORMUser, WorkspaceMembership, ORMRole)
            .join(
                WorkspaceMembership,
                (WorkspaceMembership.user_id == ORMUser.id)
                & (WorkspaceMembership.workspace_id == workspace_id),
            )
            .outerjoin(ORMRole, ORMRole.id == WorkspaceMembership.role_id)
        )
        if status:
            q = q.filter(ORMUser.status == status)
        if search:
            term = f"%{search.strip().lower()}%"
            q = q.filter(func.lower(ORMUser.email).like(term) | func.lower(func.coalesce(ORMUser.name, "")).like(term))
        rows = q.order_by(ORMUser.email.asc()).all()
        return {
            "items": [
                {
                    "id": user.id,
                    "email": user.email,
                    "name": user.name,
                    "status": user.status,
                    "last_login_at": user.last_login_at,
                    "membership_id": membership.id,
                    "membership_status": membership.status,
                    "role_id": membership.role_id,
                    "role_name": role.name if role else None,
                }
                for user, membership, role in rows
            ]
        }

    @router.patch("/api/admin/users/{user_id}")
    def admin_update_user(
        user_id: str,
        body: AdminUserUpdatePayload,
        request: Request,
        _ctx=Depends(require_permission_dependency("users.manage")),
        db=Depends(get_db_dependency),
    ):
        user = db.get(ORMUser, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        membership = (
            db.query(WorkspaceMembership)
            .filter(WorkspaceMembership.workspace_id == _ctx.workspace_id, WorkspaceMembership.user_id == user.id)
            .first()
        )
        if not membership:
            raise HTTPException(status_code=404, detail="User is not a member of this workspace")
        user.status = body.status
        user.updated_at = datetime.utcnow()
        db.add(user)
        db.commit()
        if body.status == "disabled":
            revoke_all_user_sessions(db, user.id)
        write_security_audit_fn(
            db,
            actor_user_id=_ctx.user_id,
            workspace_id=_ctx.workspace_id,
            action_key="user.status_updated",
            target_type="user",
            target_id=user.id,
            metadata={"status": body.status},
            request=request,
        )
        return {"id": user.id, "status": user.status}

    @router.post("/api/admin/users/{user_id}/reset-sessions")
    def admin_reset_user_sessions(
        user_id: str,
        request: Request,
        _ctx=Depends(require_permission_dependency("users.manage")),
        db=Depends(get_db_dependency),
    ):
        user = db.get(ORMUser, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        membership = (
            db.query(WorkspaceMembership)
            .filter(WorkspaceMembership.workspace_id == _ctx.workspace_id, WorkspaceMembership.user_id == user.id)
            .first()
        )
        if not membership:
            raise HTTPException(status_code=404, detail="User is not a member of this workspace")
        revoked = revoke_all_user_sessions(db, user.id)
        write_security_audit_fn(
            db,
            actor_user_id=_ctx.user_id,
            workspace_id=_ctx.workspace_id,
            action_key="user.sessions_reset",
            target_type="user",
            target_id=user.id,
            metadata={"sessions_revoked": revoked},
            request=request,
        )
        return {"id": user.id, "sessions_revoked": revoked}

    @router.get("/api/admin/memberships")
    def admin_list_memberships(
        workspaceId: Optional[str] = Query(None),
        _ctx=Depends(require_permission_dependency("users.manage")),
        db=Depends(get_db_dependency),
    ):
        workspace_id = workspace_scope_or_403_fn(_ctx, workspaceId)
        rows = (
            db.query(WorkspaceMembership, ORMUser, ORMRole)
            .join(ORMUser, ORMUser.id == WorkspaceMembership.user_id)
            .outerjoin(ORMRole, ORMRole.id == WorkspaceMembership.role_id)
            .filter(WorkspaceMembership.workspace_id == workspace_id)
            .order_by(WorkspaceMembership.created_at.desc())
            .all()
        )
        return {
            "items": [
                {
                    "id": m.id,
                    "workspace_id": m.workspace_id,
                    "user_id": m.user_id,
                    "email": u.email,
                    "name": u.name,
                    "status": m.status,
                    "role_id": m.role_id,
                    "role_name": r.name if r else None,
                    "created_at": m.created_at,
                    "updated_at": m.updated_at,
                }
                for m, u, r in rows
            ]
        }

    @router.patch("/api/admin/memberships/{membership_id}")
    def admin_update_membership(
        membership_id: str,
        body: AdminMembershipUpdatePayload,
        request: Request,
        _ctx=Depends(require_permission_dependency("users.manage")),
        db=Depends(get_db_dependency),
    ):
        membership = db.get(WorkspaceMembership, membership_id)
        if not membership:
            raise HTTPException(status_code=404, detail="Membership not found")
        workspace_id = workspace_scope_or_403_fn(_ctx, membership.workspace_id)
        role = db.get(ORMRole, body.role_id)
        if not role:
            raise HTTPException(status_code=404, detail="Role not found")
        if role.workspace_id not in (None, workspace_id):
            raise HTTPException(status_code=400, detail="Role is not available in this workspace")
        membership.role_id = role.id
        membership.updated_at = datetime.utcnow()
        db.add(membership)
        db.commit()
        write_security_audit_fn(
            db,
            actor_user_id=_ctx.user_id,
            workspace_id=workspace_id,
            action_key="membership.role_changed",
            target_type="membership",
            target_id=membership.id,
            metadata={"role_id": role.id, "role_name": role.name},
            request=request,
        )
        return {"id": membership.id, "role_id": membership.role_id}

    @router.delete("/api/admin/memberships/{membership_id}")
    def admin_remove_membership(
        membership_id: str,
        request: Request,
        _ctx=Depends(require_permission_dependency("users.manage")),
        db=Depends(get_db_dependency),
    ):
        membership = db.get(WorkspaceMembership, membership_id)
        if not membership:
            raise HTTPException(status_code=404, detail="Membership not found")
        workspace_id = workspace_scope_or_403_fn(_ctx, membership.workspace_id)
        membership.status = "removed"
        membership.role_id = None
        membership.updated_at = datetime.utcnow()
        db.add(membership)
        db.commit()
        write_security_audit_fn(
            db,
            actor_user_id=_ctx.user_id,
            workspace_id=workspace_id,
            action_key="membership.removed",
            target_type="membership",
            target_id=membership.id,
            metadata={"user_id": membership.user_id},
            request=request,
        )
        return {"id": membership.id, "status": membership.status}

    @router.post("/api/admin/invitations")
    def admin_create_invitation(
        body: AdminInvitationCreatePayload,
        request: Request,
        _ctx=Depends(require_permission_dependency("users.manage")),
        db=Depends(get_db_dependency),
    ):
        workspace_id = workspace_scope_or_403_fn(_ctx, body.workspace_id)
        email = (body.email or "").strip().lower()
        if not email:
            raise HTTPException(status_code=400, detail="email is required")
        role = db.get(ORMRole, body.role_id)
        if not role:
            raise HTTPException(status_code=404, detail="Role not found")
        if role.workspace_id not in (None, workspace_id):
            raise HTTPException(status_code=400, detail="Role is not available in this workspace")
        raw_token = invite_token_fn()
        inv = ORMInvitation(
            id=str(uuid.uuid4()),
            workspace_id=workspace_id,
            email=email,
            role_id=role.id,
            token_hash=hash_invite_token_fn(raw_token),
            expires_at=datetime.utcnow() + timedelta(days=body.expires_in_days),
            invited_by_user_id=_ctx.user_id,
            accepted_at=None,
            created_at=datetime.utcnow(),
        )
        db.add(inv)
        db.commit()
        write_security_audit_fn(
            db,
            actor_user_id=_ctx.user_id,
            workspace_id=workspace_id,
            action_key="invitation.created",
            target_type="invitation",
            target_id=inv.id,
            metadata={"email": inv.email, "role_id": inv.role_id},
            request=request,
        )
        return {
            "id": inv.id,
            "workspace_id": inv.workspace_id,
            "email": inv.email,
            "role_id": inv.role_id,
            "expires_at": inv.expires_at,
            "created_at": inv.created_at,
            "token": raw_token,
        }

    @router.get("/api/admin/invitations")
    def admin_list_invitations(
        workspaceId: Optional[str] = Query(None),
        _ctx=Depends(require_permission_dependency("users.manage")),
        db=Depends(get_db_dependency),
    ):
        workspace_id = workspace_scope_or_403_fn(_ctx, workspaceId)
        rows = (
            db.query(ORMInvitation, ORMRole, ORMUser)
            .outerjoin(ORMRole, ORMRole.id == ORMInvitation.role_id)
            .outerjoin(ORMUser, ORMUser.id == ORMInvitation.invited_by_user_id)
            .filter(ORMInvitation.workspace_id == workspace_id)
            .order_by(ORMInvitation.created_at.desc())
            .all()
        )
        return {
            "items": [
                {
                    "id": inv.id,
                    "workspace_id": inv.workspace_id,
                    "email": inv.email,
                    "role_id": inv.role_id,
                    "role_name": role.name if role else None,
                    "expires_at": inv.expires_at,
                    "accepted_at": inv.accepted_at,
                    "created_at": inv.created_at,
                    "invited_by_user_id": inv.invited_by_user_id,
                    "invited_by_name": user.name if user else None,
                }
                for inv, role, user in rows
            ]
        }

    @router.post("/api/admin/invitations/{invitation_id}/resend")
    def admin_resend_invitation(
        invitation_id: str,
        request: Request,
        _ctx=Depends(require_permission_dependency("users.manage")),
        db=Depends(get_db_dependency),
    ):
        inv = db.get(ORMInvitation, invitation_id)
        if not inv:
            raise HTTPException(status_code=404, detail="Invitation not found")
        workspace_id = workspace_scope_or_403_fn(_ctx, inv.workspace_id)
        raw_token = invite_token_fn()
        inv.token_hash = hash_invite_token_fn(raw_token)
        inv.expires_at = datetime.utcnow() + timedelta(days=7)
        inv.created_at = datetime.utcnow()
        db.add(inv)
        db.commit()
        write_security_audit_fn(
            db,
            actor_user_id=_ctx.user_id,
            workspace_id=workspace_id,
            action_key="invitation.resent",
            target_type="invitation",
            target_id=inv.id,
            metadata={"email": inv.email},
            request=request,
        )
        return {"id": inv.id, "expires_at": inv.expires_at, "token": raw_token}

    @router.delete("/api/admin/invitations/{invitation_id}/revoke")
    def admin_revoke_invitation(
        invitation_id: str,
        request: Request,
        _ctx=Depends(require_permission_dependency("users.manage")),
        db=Depends(get_db_dependency),
    ):
        inv = db.get(ORMInvitation, invitation_id)
        if not inv:
            raise HTTPException(status_code=404, detail="Invitation not found")
        workspace_id = workspace_scope_or_403_fn(_ctx, inv.workspace_id)
        db.delete(inv)
        db.commit()
        write_security_audit_fn(
            db,
            actor_user_id=_ctx.user_id,
            workspace_id=workspace_id,
            action_key="invitation.revoked",
            target_type="invitation",
            target_id=invitation_id,
            metadata={},
            request=request,
        )
        return {"id": invitation_id, "revoked": True}

    @router.post("/api/invitations/accept")
    def accept_invitation(
        body: InvitationAcceptPayload,
        request: Request,
        db=Depends(get_db_dependency),
    ):
        token = (body.token or "").strip()
        if not token:
            raise HTTPException(status_code=400, detail="token is required")
        token_hash = hash_invite_token_fn(token)
        inv = db.query(ORMInvitation).filter(ORMInvitation.token_hash == token_hash).first()
        if not inv:
            raise HTTPException(status_code=404, detail="Invitation not found")
        if inv.accepted_at is not None:
            raise HTTPException(status_code=400, detail="Invitation already accepted")
        if inv.expires_at < datetime.utcnow():
            raise HTTPException(status_code=400, detail="Invitation expired")

        email = inv.email.strip().lower()
        user = db.query(ORMUser).filter(ORMUser.email == email).first()
        if not user:
            user = ORMUser(
                id=str(uuid.uuid4()),
                email=email,
                name=(body.name or email.split("@")[0]),
                status="active",
                auth_provider="local",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.add(user)
            db.flush()
        membership = (
            db.query(WorkspaceMembership)
            .filter(WorkspaceMembership.workspace_id == inv.workspace_id, WorkspaceMembership.user_id == user.id)
            .first()
        )
        if not membership:
            membership = WorkspaceMembership(
                id=str(uuid.uuid4()),
                workspace_id=inv.workspace_id,
                user_id=user.id,
                role_id=inv.role_id,
                status="active",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.add(membership)
        else:
            membership.status = "active"
            membership.role_id = inv.role_id or membership.role_id
            membership.updated_at = datetime.utcnow()
            db.add(membership)
        inv.accepted_at = datetime.utcnow()
        db.add(inv)
        db.commit()
        write_security_audit_fn(
            db,
            actor_user_id=user.id,
            workspace_id=inv.workspace_id,
            action_key="invitation.accepted",
            target_type="invitation",
            target_id=inv.id,
            metadata={"membership_id": membership.id},
            request=request,
        )
        return {"ok": True, "workspace_id": inv.workspace_id, "user_id": user.id, "membership_id": membership.id}

    return router
