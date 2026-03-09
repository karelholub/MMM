from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.connectors.ads_ops.registry import get_ads_adapter
from app.models_config_dq import AdsAuditLog, AdsChangeRequest, AdsEntityMap, DataSource, JourneyPathDaily
from app.services_oauth_connections import get_access_token_for_provider


PROVIDER_KEYS = {"google_ads", "meta_ads", "linkedin_ads"}
ENTITY_TYPES = {"campaign", "adset", "adgroup"}
ACTION_TYPES = {"pause", "enable", "update_budget"}
STATUS_VALUES = {"draft", "pending_approval", "approved", "rejected", "applied", "failed", "cancelled"}
PROVIDER_FROM_CHANNEL = {
    "google_ads": "google_ads",
    "meta_ads": "meta_ads",
    "linkedin_ads": "linkedin_ads",
}


def utcnow() -> datetime:
    return datetime.utcnow()


def _new_id() -> str:
    return str(uuid.uuid4())


def _default_account_for_provider(db: Session, *, workspace_id: str, provider: str) -> str:
    row = (
        db.query(DataSource)
        .filter(
            DataSource.workspace_id == workspace_id,
            DataSource.category == "ad_platform",
            DataSource.type == provider,
        )
        .first()
    )
    if not row:
        return "default"
    cfg = row.config_json or {}
    selected = cfg.get("selected_accounts") or []
    if selected:
        return str(selected[0])
    available = cfg.get("available_accounts") or []
    if available and isinstance(available[0], dict):
        first_id = available[0].get("id")
        if first_id:
            return str(first_id)
    return "default"


def _write_ads_audit(
    db: Session,
    *,
    workspace_id: str,
    actor_user_id: str,
    provider: str,
    account_id: str,
    entity_type: str,
    entity_id: str,
    event_type: str,
    event_payload_json: Dict[str, Any],
) -> None:
    db.add(
        AdsAuditLog(
            id=_new_id(),
            workspace_id=workspace_id,
            actor_user_id=actor_user_id,
            provider=provider,
            account_id=account_id,
            entity_type=entity_type,
            entity_id=entity_id,
            event_type=event_type,
            event_payload_json=event_payload_json or {},
            created_at=utcnow(),
        )
    )
    db.commit()


def _validate_provider_entity(provider: str, entity_type: str) -> None:
    if provider not in PROVIDER_KEYS:
        raise ValueError("Unsupported provider")
    if entity_type not in ENTITY_TYPES:
        raise ValueError("Unsupported entity_type")


def _hydrate_entity_map_from_paths(db: Session, *, workspace_id: str) -> int:
    cutoff = (utcnow() - timedelta(days=120)).date()
    rows = (
        db.query(JourneyPathDaily.channel_group, JourneyPathDaily.campaign_id)
        .filter(
            JourneyPathDaily.date >= cutoff,
            JourneyPathDaily.campaign_id.isnot(None),
            JourneyPathDaily.campaign_id != "",
        )
        .distinct()
        .all()
    )
    upserts = 0
    now = utcnow()
    for channel_group, campaign_id in rows:
        provider = PROVIDER_FROM_CHANNEL.get(str(channel_group or "").strip().lower())
        if not provider:
            continue
        entity_id = str(campaign_id).strip()
        if not entity_id:
            continue
        account_id = _default_account_for_provider(db, workspace_id=workspace_id, provider=provider)
        existing = (
            db.query(AdsEntityMap)
            .filter(
                AdsEntityMap.workspace_id == workspace_id,
                AdsEntityMap.provider == provider,
                AdsEntityMap.entity_type == "campaign",
                AdsEntityMap.entity_id == entity_id,
            )
            .first()
        )
        if existing:
            existing.last_seen_ts = now
            existing.account_id = existing.account_id or account_id
            db.add(existing)
        else:
            db.add(
                AdsEntityMap(
                    id=_new_id(),
                    workspace_id=workspace_id,
                    provider=provider,
                    account_id=account_id,
                    entity_type="campaign",
                    entity_id=entity_id,
                    entity_name=entity_id,
                    last_seen_ts=now,
                    created_at=now,
                    updated_at=now,
                )
            )
            upserts += 1
    db.commit()
    return upserts


def list_ads_entities(
    db: Session,
    *,
    workspace_id: str,
    provider: Optional[str] = None,
    entity_type: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 100,
) -> Dict[str, Any]:
    _hydrate_entity_map_from_paths(db, workspace_id=workspace_id)
    q = db.query(AdsEntityMap).filter(AdsEntityMap.workspace_id == workspace_id)
    if provider:
        q = q.filter(AdsEntityMap.provider == provider)
    if entity_type:
        q = q.filter(AdsEntityMap.entity_type == entity_type)
    if search:
        like = f"%{search.strip()}%"
        q = q.filter(
            or_(
                AdsEntityMap.entity_id.ilike(like),
                AdsEntityMap.entity_name.ilike(like),
                AdsEntityMap.account_id.ilike(like),
            )
        )
    rows = q.order_by(AdsEntityMap.updated_at.desc()).limit(max(1, min(limit, 500))).all()
    out = []
    for row in rows:
        adapter = get_ads_adapter(row.provider)
        out.append(
            {
                "id": row.id,
                "provider": row.provider,
                "account_id": row.account_id,
                "entity_type": row.entity_type,
                "entity_id": row.entity_id,
                "entity_name": row.entity_name,
                "deep_link": adapter.build_deep_link(
                    account_id=row.account_id,
                    entity_type=row.entity_type,
                    entity_id=row.entity_id,
                ),
                "last_seen_ts": row.last_seen_ts.isoformat() if row.last_seen_ts else None,
            }
        )
    return {"items": out, "total": len(out)}


def get_ads_deep_link(
    db: Session,
    *,
    workspace_id: str,
    provider: str,
    account_id: Optional[str],
    entity_type: str,
    entity_id: str,
) -> str:
    _validate_provider_entity(provider, entity_type)
    acc = (account_id or "").strip() or _default_account_for_provider(db, workspace_id=workspace_id, provider=provider)
    adapter = get_ads_adapter(provider)
    return adapter.build_deep_link(account_id=acc, entity_type=entity_type, entity_id=entity_id)


def fetch_ads_state(
    db: Session,
    *,
    workspace_id: str,
    provider: str,
    account_id: str,
    entity_type: str,
    entity_id: str,
) -> Dict[str, Any]:
    _validate_provider_entity(provider, entity_type)
    token = get_access_token_for_provider(db, workspace_id=workspace_id, provider_key=provider)
    deep_link = get_ads_deep_link(
        db,
        workspace_id=workspace_id,
        provider=provider,
        account_id=account_id,
        entity_type=entity_type,
        entity_id=entity_id,
    )
    if not token:
        return {
            "provider": provider,
            "account_id": account_id,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "deep_link": deep_link,
            "status": "unknown",
            "budget": None,
            "currency": None,
            "name": entity_id,
            "needs_reauth": True,
            "error": {"code": "missing_connection", "message": "Provider connection missing or expired"},
        }
    adapter = get_ads_adapter(provider)
    state = adapter.fetch_entity_state(
        access_token=token,
        account_id=account_id,
        entity_type=entity_type,
        entity_id=entity_id,
    )
    return {
        "provider": provider,
        "account_id": account_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "deep_link": deep_link,
        **(state or {}),
    }


def create_change_request(
    db: Session,
    *,
    workspace_id: str,
    requested_by_user_id: str,
    provider: str,
    account_id: str,
    entity_type: str,
    entity_id: str,
    action_type: str,
    action_payload: Dict[str, Any],
    approval_required: bool,
) -> Dict[str, Any]:
    _validate_provider_entity(provider, entity_type)
    if action_type not in ACTION_TYPES:
        raise ValueError("Unsupported action_type")
    adapter = get_ads_adapter(provider)
    if not adapter.supports(action_type, entity_type):
        raise ValueError(f"{provider} does not support action '{action_type}' for '{entity_type}'")
    row = AdsChangeRequest(
        id=_new_id(),
        workspace_id=workspace_id,
        requested_by_user_id=requested_by_user_id,
        provider=provider,
        account_id=account_id,
        entity_type=entity_type,
        entity_id=entity_id,
        action_type=action_type,
        action_payload_json=action_payload or {},
        status="pending_approval" if approval_required else "approved",
        approval_required=bool(approval_required),
        idempotency_key=f"adscr-{uuid.uuid4().hex[:18]}",
        created_at=utcnow(),
        updated_at=utcnow(),
    )
    db.add(row)
    db.commit()
    _write_ads_audit(
        db,
        workspace_id=workspace_id,
        actor_user_id=requested_by_user_id,
        provider=provider,
        account_id=account_id,
        entity_type=entity_type,
        entity_id=entity_id,
        event_type="proposal_created",
        event_payload_json={"change_request_id": row.id, "action_type": action_type, "approval_required": approval_required},
    )
    db.refresh(row)
    return serialize_change_request(row)


def serialize_change_request(row: AdsChangeRequest) -> Dict[str, Any]:
    return {
        "id": row.id,
        "workspace_id": row.workspace_id,
        "requested_by_user_id": row.requested_by_user_id,
        "provider": row.provider,
        "account_id": row.account_id,
        "entity_type": row.entity_type,
        "entity_id": row.entity_id,
        "action_type": row.action_type,
        "action_payload": row.action_payload_json or {},
        "status": row.status,
        "approval_required": bool(row.approval_required),
        "approved_by_user_id": row.approved_by_user_id,
        "approved_at": row.approved_at.isoformat() if row.approved_at else None,
        "applied_at": row.applied_at.isoformat() if row.applied_at else None,
        "error_message": row.error_message,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def get_change_request_or_404(db: Session, *, workspace_id: str, request_id: str) -> AdsChangeRequest:
    row = db.get(AdsChangeRequest, request_id)
    if not row or row.workspace_id != workspace_id:
        raise ValueError("Change request not found")
    return row


def approve_change_request(db: Session, *, workspace_id: str, request_id: str, actor_user_id: str) -> Dict[str, Any]:
    row = get_change_request_or_404(db, workspace_id=workspace_id, request_id=request_id)
    if row.status not in {"pending_approval", "draft"}:
        raise ValueError("Only pending/draft requests can be approved")
    row.status = "approved"
    row.approved_by_user_id = actor_user_id
    row.approved_at = utcnow()
    row.updated_at = utcnow()
    db.add(row)
    db.commit()
    _write_ads_audit(
        db,
        workspace_id=workspace_id,
        actor_user_id=actor_user_id,
        provider=row.provider,
        account_id=row.account_id,
        entity_type=row.entity_type,
        entity_id=row.entity_id,
        event_type="approved",
        event_payload_json={"change_request_id": row.id},
    )
    db.refresh(row)
    return serialize_change_request(row)


def reject_change_request(db: Session, *, workspace_id: str, request_id: str, actor_user_id: str, reason: Optional[str]) -> Dict[str, Any]:
    row = get_change_request_or_404(db, workspace_id=workspace_id, request_id=request_id)
    if row.status in {"applied", "failed", "cancelled"}:
        raise ValueError("Request already finalized")
    row.status = "rejected"
    row.error_message = (reason or "").strip() or "Rejected"
    row.updated_at = utcnow()
    db.add(row)
    db.commit()
    _write_ads_audit(
        db,
        workspace_id=workspace_id,
        actor_user_id=actor_user_id,
        provider=row.provider,
        account_id=row.account_id,
        entity_type=row.entity_type,
        entity_id=row.entity_id,
        event_type="rejected",
        event_payload_json={"change_request_id": row.id, "reason": row.error_message},
    )
    db.refresh(row)
    return serialize_change_request(row)


def apply_change_request(
    db: Session,
    *,
    workspace_id: str,
    request_id: str,
    actor_user_id: str,
    require_approval: bool,
    budget_change_limit_pct: float,
    admin_override: bool,
) -> Dict[str, Any]:
    row = get_change_request_or_404(db, workspace_id=workspace_id, request_id=request_id)
    if row.status == "applied":
        return serialize_change_request(row)
    if row.status in {"rejected", "cancelled"}:
        raise ValueError(f"Cannot apply request with status '{row.status}'")
    if require_approval and row.approval_required and row.status != "approved":
        raise ValueError("Approval required before applying this request")

    token = get_access_token_for_provider(db, workspace_id=workspace_id, provider_key=row.provider)
    if not token:
        row.status = "failed"
        row.error_message = "Connection missing or needs re-auth"
        row.updated_at = utcnow()
        db.add(row)
        db.commit()
        _write_ads_audit(
            db,
            workspace_id=workspace_id,
            actor_user_id=actor_user_id,
            provider=row.provider,
            account_id=row.account_id,
            entity_type=row.entity_type,
            entity_id=row.entity_id,
            event_type="credentials_missing",
            event_payload_json={"change_request_id": row.id},
        )
        return serialize_change_request(row)

    adapter = get_ads_adapter(row.provider)
    state = adapter.fetch_entity_state(
        access_token=token,
        account_id=row.account_id,
        entity_type=row.entity_type,
        entity_id=row.entity_id,
    )
    payload = row.action_payload_json or {}
    if row.action_type == "update_budget" and not admin_override:
        current_budget = state.get("budget")
        proposed_budget = float(payload.get("daily_budget") or 0)
        if current_budget and current_budget > 0 and proposed_budget > 0:
            pct = abs((proposed_budget - float(current_budget)) / float(current_budget) * 100.0)
            if pct > float(budget_change_limit_pct):
                raise ValueError(f"Budget change exceeds configured limit ({budget_change_limit_pct:.0f}%)")

    try:
        if row.action_type == "pause":
            result = adapter.pause_entity(
                access_token=token,
                account_id=row.account_id,
                entity_type=row.entity_type,
                entity_id=row.entity_id,
                idempotency_key=row.idempotency_key or row.id,
            )
        elif row.action_type == "enable":
            result = adapter.enable_entity(
                access_token=token,
                account_id=row.account_id,
                entity_type=row.entity_type,
                entity_id=row.entity_id,
                idempotency_key=row.idempotency_key or row.id,
            )
        else:
            result = adapter.update_budget(
                access_token=token,
                account_id=row.account_id,
                entity_type=row.entity_type,
                entity_id=row.entity_id,
                daily_budget=float(payload.get("daily_budget") or 0),
                currency=payload.get("currency"),
                idempotency_key=row.idempotency_key or row.id,
            )
        if not result.ok:
            raise RuntimeError(result.message or "Provider returned non-ok result")
        row.status = "applied"
        row.applied_at = utcnow()
        row.error_message = None
        row.updated_at = utcnow()
        db.add(row)
        db.commit()
        _write_ads_audit(
            db,
            workspace_id=workspace_id,
            actor_user_id=actor_user_id,
            provider=row.provider,
            account_id=row.account_id,
            entity_type=row.entity_type,
            entity_id=row.entity_id,
            event_type="applied",
            event_payload_json={"change_request_id": row.id, "provider_request_id": result.provider_request_id, "meta": result.meta or {}},
        )
    except Exception as exc:
        err = adapter.normalize_error(exc)
        row.status = "failed"
        row.error_message = err.message
        row.updated_at = utcnow()
        db.add(row)
        db.commit()
        _write_ads_audit(
            db,
            workspace_id=workspace_id,
            actor_user_id=actor_user_id,
            provider=row.provider,
            account_id=row.account_id,
            entity_type=row.entity_type,
            entity_id=row.entity_id,
            event_type="apply_failed",
            event_payload_json={"change_request_id": row.id, "code": err.code, "message": err.message, "retryable": err.retryable, "needs_reauth": err.needs_reauth},
        )
    db.refresh(row)
    return serialize_change_request(row)


def list_change_requests(
    db: Session,
    *,
    workspace_id: str,
    status: Optional[str] = None,
    provider: Optional[str] = None,
    limit: int = 200,
) -> Dict[str, Any]:
    q = db.query(AdsChangeRequest).filter(AdsChangeRequest.workspace_id == workspace_id)
    if status:
        q = q.filter(AdsChangeRequest.status == status)
    if provider:
        q = q.filter(AdsChangeRequest.provider == provider)
    rows = q.order_by(AdsChangeRequest.created_at.desc()).limit(max(1, min(limit, 1000))).all()
    return {"items": [serialize_change_request(r) for r in rows], "total": len(rows)}


def list_ads_audit(
    db: Session,
    *,
    workspace_id: str,
    provider: Optional[str] = None,
    entity_id: Optional[str] = None,
    limit: int = 200,
) -> Dict[str, Any]:
    q = db.query(AdsAuditLog).filter(AdsAuditLog.workspace_id == workspace_id)
    if provider:
        q = q.filter(AdsAuditLog.provider == provider)
    if entity_id:
        q = q.filter(AdsAuditLog.entity_id == entity_id)
    rows = q.order_by(AdsAuditLog.created_at.desc()).limit(max(1, min(limit, 1000))).all()
    out = [
        {
            "id": r.id,
            "workspace_id": r.workspace_id,
            "actor_user_id": r.actor_user_id,
            "provider": r.provider,
            "account_id": r.account_id,
            "entity_type": r.entity_type,
            "entity_id": r.entity_id,
            "event_type": r.event_type,
            "event_payload": r.event_payload_json or {},
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]
    return {"items": out, "total": len(out)}
