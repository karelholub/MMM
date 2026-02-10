"""Services for ModelConfig versioning and validation."""

from __future__ import annotations

import copy
import json
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.orm import Session

from .models_config_dq import (
    ModelConfig,
    ModelConfigAudit,
    ModelConfigStatus,
)


def _new_id() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.utcnow()


def _diff_configs(old: Optional[Dict[str, Any]], new: Dict[str, Any]) -> Dict[str, Any]:
    """Very simple diff: store old/new; callers can inspect fields as needed."""
    return {"old": old or {}, "new": new}


def create_draft_config(
    db: Session,
    name: str,
    config_json: Dict[str, Any],
    created_by: str,
    change_note: Optional[str] = None,
) -> ModelConfig:
    """Create a new draft ModelConfig (version starts at 1 for a name)."""
    max_version = (
        db.query(ModelConfig)
        .filter(ModelConfig.name == name)
        .with_entities(ModelConfig.version)
        .order_by(ModelConfig.version.desc())
        .first()
    )
    next_version = (max_version[0] + 1) if max_version else 1

    cfg = ModelConfig(
        id=_new_id(),
        name=name,
        status=ModelConfigStatus.DRAFT,
        version=next_version,
        parent_id=None,
        created_at=_now(),
        updated_at=_now(),
        created_by=created_by,
        change_note=change_note,
        config_json=config_json,
    )
    db.add(cfg)
    db.flush()
    audit = ModelConfigAudit(
        model_config_id=cfg.id,
        actor=created_by,
        action="create",
        diff_json=_diff_configs(None, config_json),
        created_at=_now(),
    )
    db.add(audit)
    db.commit()
    db.refresh(cfg)
    return cfg


def clone_config(db: Session, source_id: str, actor: str, change_note: Optional[str] = None) -> ModelConfig:
    src: ModelConfig = db.get(ModelConfig, source_id)
    if not src:
        raise ValueError("Source config not found")
    cfg_json = copy.deepcopy(src.config_json or {})
    new_cfg = create_draft_config(
        db=db,
        name=src.name,
        config_json=cfg_json,
        created_by=actor,
        change_note=change_note or f"Cloned from {src.id}",
    )
    new_cfg.parent_id = src.id
    db.add(new_cfg)
    db.commit()
    db.refresh(new_cfg)
    return new_cfg


def update_draft_config(
    db: Session,
    cfg_id: str,
    new_config_json: Dict[str, Any],
    actor: str,
    change_note: Optional[str] = None,
) -> ModelConfig:
    cfg: ModelConfig = db.get(ModelConfig, cfg_id)
    if not cfg:
        raise ValueError("Config not found")
    if cfg.status != ModelConfigStatus.DRAFT:
        raise ValueError("Only draft configs can be edited")
    old = copy.deepcopy(cfg.config_json or {})
    cfg.config_json = new_config_json
    cfg.updated_at = _now()
    if change_note:
        cfg.change_note = change_note
    audit = ModelConfigAudit(
        model_config_id=cfg.id,
        actor=actor,
        action="update",
        diff_json=_diff_configs(old, new_config_json),
        created_at=_now(),
    )
    db.add(audit)
    db.commit()
    db.refresh(cfg)
    return cfg


def _validate_windows(cfg_json: Dict[str, Any]) -> Tuple[bool, str]:
    windows = cfg_json.get("windows", {})
    click_lb = windows.get("click_lookback_days", 30)
    imp_lb = windows.get("impression_lookback_days", 7)
    sess = windows.get("session_timeout_minutes", 30)
    latency = windows.get("conversion_latency_days", 7)
    if not (0 < click_lb <= 90):
        return False, "click_lookback_days must be between 1 and 90"
    if not (0 <= imp_lb <= 90):
        return False, "impression_lookback_days must be between 0 and 90"
    if not (1 <= sess <= 1440):
        return False, "session_timeout_minutes must be between 1 and 1440"
    if not (0 <= latency <= 90):
        return False, "conversion_latency_days must be between 0 and 90"
    return True, ""


def _validate_conversions(cfg_json: Dict[str, Any]) -> Tuple[bool, str]:
    convs = cfg_json.get("conversions", {})
    defs = convs.get("conversion_definitions", []) or []
    keys = [d.get("key") for d in defs if d.get("key")]
    if len(keys) != len(set(keys)):
        return False, "conversion keys must be unique"
    # Primary conversion key should exist if provided
    primary = convs.get("primary_conversion_key")
    if primary and primary not in keys:
        return False, f"primary_conversion_key '{primary}' not found in conversion_definitions"
    return True, ""


def validate_model_config(cfg_json: Dict[str, Any]) -> Tuple[bool, str]:
    ok, msg = _validate_windows(cfg_json)
    if not ok:
        return ok, msg
    ok, msg = _validate_conversions(cfg_json)
    if not ok:
        return ok, msg
    # Event catalog validation is application-specific; we only warn at runtime.
    return True, ""


def activate_config(
    db: Session,
    cfg_id: str,
    actor: str,
    set_as_default: bool = True,
) -> ModelConfig:
    cfg: ModelConfig = db.get(ModelConfig, cfg_id)
    if not cfg:
        raise ValueError("Config not found")
    ok, msg = validate_model_config(cfg.config_json or {})
    if not ok:
        raise ValueError(f"Config validation failed: {msg}")

    # Only one active per name: demote others
    others = (
        db.query(ModelConfig)
        .filter(ModelConfig.name == cfg.name, ModelConfig.status == ModelConfigStatus.ACTIVE)
        .all()
    )
    for o in others:
        o.status = ModelConfigStatus.ARCHIVED
        o.updated_at = _now()

    cfg.status = ModelConfigStatus.ACTIVE
    cfg.activated_at = _now()
    cfg.updated_at = _now()
    db.add(cfg)
    db.flush()
    audit = ModelConfigAudit(
        model_config_id=cfg.id,
        actor=actor,
        action="activate",
        diff_json=None,
        created_at=_now(),
    )
    db.add(audit)
    db.commit()
    db.refresh(cfg)

    if set_as_default:
        _set_default_config_id(cfg.id)

    return cfg


def archive_config(db: Session, cfg_id: str, actor: str) -> ModelConfig:
    cfg: ModelConfig = db.get(ModelConfig, cfg_id)
    if not cfg:
        raise ValueError("Config not found")
    cfg.status = ModelConfigStatus.ARCHIVED
    cfg.updated_at = _now()
    db.add(cfg)
    audit = ModelConfigAudit(
        model_config_id=cfg.id,
        actor=actor,
        action="archive",
        diff_json=None,
        created_at=_now(),
    )
    db.add(audit)
    db.commit()
    db.refresh(cfg)
    return cfg


# ---------------------------------------------------------------------------
# Default active config persistence (file-based to avoid extra DB tables)
# ---------------------------------------------------------------------------

from pathlib import Path  # noqa: E402

# Resolve data directory relative to this file to avoid importing main.py
_DATA_DIR = Path(__file__).resolve().parent / "data"
_DATA_DIR.mkdir(parents=True, exist_ok=True)
_DEFAULT_CFG_PATH = _DATA_DIR / "model_config_settings.json"


def _set_default_config_id(cfg_id: str) -> None:
    payload = {"default_config_id": cfg_id, "updated_at": _now().isoformat()}
    _DEFAULT_CFG_PATH.write_text(json.dumps(payload, indent=2))


def get_default_config_id() -> Optional[str]:
    if not _DEFAULT_CFG_PATH.exists():
        return None
    try:
        data = json.loads(_DEFAULT_CFG_PATH.read_text())
        return data.get("default_config_id")
    except Exception:
        return None

