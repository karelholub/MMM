"""CRUD/test services for data sources inventory and connector framework."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional
import json
import uuid

from sqlalchemy.orm import Session

from app.connectors.data_sources.registry import get_connector
from app.models_config_dq import DataSource, SecretStore
from app.utils.encrypt import decrypt, encrypt


ALLOWED_CATEGORIES = {"warehouse", "ad_platform", "cdp"}
ALLOWED_STATUSES = {"connected", "error", "needs_reauth", "disabled"}


def _safe_config_for_type(source_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
    cfg = dict(config or {})
    cfg.pop("service_account_json", None)
    cfg.pop("private_key_pem", None)
    cfg.pop("passphrase", None)
    cfg.pop("api_key", None)
    if source_type == "bigquery":
        return {
            "project_id": cfg.get("project_id"),
            "default_dataset": cfg.get("default_dataset"),
            "service_account_email": cfg.get("service_account_email"),
        }
    if source_type == "snowflake":
        return {
            "account": cfg.get("account"),
            "username": cfg.get("username"),
            "warehouse": cfg.get("warehouse"),
            "role": cfg.get("role"),
            "default_database": cfg.get("default_database"),
            "default_schema": cfg.get("default_schema"),
        }
    return cfg


def _serialize_data_source(item: DataSource) -> Dict[str, Any]:
    return {
        "id": item.id,
        "workspace_id": item.workspace_id,
        "category": item.category,
        "type": item.type,
        "name": item.name,
        "status": item.status,
        "config_json": item.config_json or {},
        "has_secret": bool(item.secret_ref),
        "secret_ref": item.secret_ref,
        "last_tested_at": item.last_tested_at.isoformat() if item.last_tested_at else None,
        "last_error": item.last_error,
        "created_at": item.created_at.isoformat() if item.created_at else None,
        "updated_at": item.updated_at.isoformat() if item.updated_at else None,
    }


def _upsert_secret(db: Session, *, workspace_id: str, kind: str, secret_payload: Dict[str, Any], secret_ref: Optional[str] = None) -> str:
    payload = json.dumps(secret_payload or {}, separators=(",", ":"), ensure_ascii=True)
    encrypted = encrypt(payload)
    now = datetime.utcnow()
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


def list_data_sources(db: Session, *, workspace_id: str = "default", category: Optional[str] = None) -> Dict[str, Any]:
    q = db.query(DataSource).filter(DataSource.workspace_id == workspace_id)
    if category:
        q = q.filter(DataSource.category == category)
    rows = q.order_by(DataSource.updated_at.desc()).all()
    return {"items": [_serialize_data_source(r) for r in rows], "total": len(rows)}


def create_data_source(
    db: Session,
    *,
    workspace_id: str,
    category: str,
    source_type: str,
    name: str,
    config_json: Dict[str, Any],
    secrets: Dict[str, Any],
) -> Dict[str, Any]:
    if category not in ALLOWED_CATEGORIES:
        raise ValueError("Invalid category")
    connector = get_connector(source_type)
    cfg = dict(config_json or {})
    cfg["name"] = name
    connector.validate_config(cfg, secrets or {})
    safe_cfg = _safe_config_for_type(source_type, cfg)
    if source_type == "bigquery":
        try:
            sa = json.loads(str((secrets or {}).get("service_account_json") or "{}"))
            if not safe_cfg.get("project_id"):
                safe_cfg["project_id"] = sa.get("project_id")
            safe_cfg["service_account_email"] = sa.get("client_email")
        except Exception:
            pass
    secret_ref = _upsert_secret(
        db,
        workspace_id=workspace_id,
        kind=source_type,
        secret_payload=secrets or {},
    )
    now = datetime.utcnow()
    item = DataSource(
        id=str(uuid.uuid4()),
        workspace_id=workspace_id,
        category=category,
        type=source_type,
        name=name.strip(),
        status="connected",
        config_json=safe_cfg,
        secret_ref=secret_ref,
        created_at=now,
        updated_at=now,
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return _serialize_data_source(item)


def update_data_source(
    db: Session,
    *,
    source_id: str,
    name: Optional[str] = None,
    status: Optional[str] = None,
    config_json: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    item = db.get(DataSource, source_id)
    if not item:
        return None
    if name is not None:
        item.name = name.strip()
    if status is not None:
        if status not in ALLOWED_STATUSES:
            raise ValueError("Invalid status")
        item.status = status
    if config_json is not None:
        item.config_json = _safe_config_for_type(item.type, config_json)
    item.updated_at = datetime.utcnow()
    db.add(item)
    db.commit()
    db.refresh(item)
    return _serialize_data_source(item)


def rotate_data_source_credentials(db: Session, *, source_id: str, secrets: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    item = db.get(DataSource, source_id)
    if not item:
        return None
    connector = get_connector(item.type)
    connector.validate_config({**(item.config_json or {}), "name": item.name}, secrets or {})
    item.secret_ref = _upsert_secret(
        db,
        workspace_id=item.workspace_id,
        kind=item.type,
        secret_payload=secrets or {},
        secret_ref=item.secret_ref,
    )
    item.updated_at = datetime.utcnow()
    db.add(item)
    db.commit()
    db.refresh(item)
    return _serialize_data_source(item)


def disable_data_source(db: Session, source_id: str) -> bool:
    item = db.get(DataSource, source_id)
    if not item:
        return False
    item.status = "disabled"
    item.updated_at = datetime.utcnow()
    db.add(item)
    db.commit()
    return True


def delete_data_source(db: Session, source_id: str) -> bool:
    item = db.get(DataSource, source_id)
    if not item:
        return False
    db.delete(item)
    db.commit()
    return True


def test_data_source_payload(
    db: Session,
    *,
    source_type: str,
    config_json: Dict[str, Any],
    secrets: Dict[str, Any],
) -> Dict[str, Any]:
    connector = get_connector(source_type)
    cfg = dict(config_json or {})
    if not cfg.get("name"):
        cfg["name"] = "temp-test"
    out = connector.test_connection(cfg, secrets or {})
    return {
        "ok": bool(out.get("ok", False)),
        "message": str(out.get("message", "")),
        "latency_ms": int(out.get("latency_ms", 0) or 0),
        "details": out.get("details") or {"sample_namespaces": [], "sample_schemas": [], "sample_tables": []},
    }


def test_saved_data_source(db: Session, *, source_id: str) -> Optional[Dict[str, Any]]:
    item = db.get(DataSource, source_id)
    if not item:
        return None
    secrets = _load_secret(db, item.secret_ref)
    out = test_data_source_payload(
        db,
        source_type=item.type,
        config_json={"name": item.name, **(item.config_json or {})},
        secrets=secrets,
    )
    item.last_tested_at = datetime.utcnow()
    item.status = "connected" if out.get("ok") else "error"
    item.last_error = None if out.get("ok") else out.get("message")
    item.updated_at = datetime.utcnow()
    db.add(item)
    db.commit()
    return out
