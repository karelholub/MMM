"""CRUD for notification channels and user notification preferences."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models_overview_alerts import NotificationChannel, UserNotificationPref
from app.utils.notification_secrets import get_webhook_url, set_webhook_url

WORKSPACE_SCOPE = "default"


# ---- Channels ----

def list_channels(db: Session) -> List[Dict[str, Any]]:
    """List all notification channels. Slack webhook URLs are never returned."""
    channels = db.query(NotificationChannel).order_by(NotificationChannel.id).all()
    out = []
    for ch in channels:
        payload = {
            "id": ch.id,
            "type": ch.type,
            "config": ch.config_json or {},
            "is_verified": ch.is_verified,
            "created_at": ch.created_at.isoformat() + "Z" if ch.created_at else None,
        }
        if ch.type == "slack_webhook":
            payload["config"] = {"configured": bool(get_webhook_url(ch.id))}
        out.append(payload)
    return out


def create_channel(
    db: Session,
    type_: str,
    config: Dict[str, Any],
    *,
    slack_webhook_url: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a notification channel. For slack_webhook, pass URL in slack_webhook_url; it is stored securely."""
    if type_ not in ("email", "slack_webhook"):
        raise ValueError("type must be 'email' or 'slack_webhook'")
    config_json: Dict[str, Any] = {}
    if type_ == "email":
        emails = config.get("emails")
        config_json = {"emails": [e.strip() for e in (emails or []) if isinstance(e, str) and e.strip()]}
    else:
        config_json = {"configured": True}
    ch = NotificationChannel(type=type_, config_json=config_json, is_verified=False)
    db.add(ch)
    db.commit()
    db.refresh(ch)
    if type_ == "slack_webhook" and slack_webhook_url and slack_webhook_url.strip():
        set_webhook_url(ch.id, slack_webhook_url.strip())
    return channel_to_response(ch)


def get_channel(db: Session, channel_id: int) -> Optional[Dict[str, Any]]:
    ch = db.query(NotificationChannel).filter(NotificationChannel.id == channel_id).first()
    if not ch:
        return None
    return channel_to_response(ch)


def update_channel(
    db: Session,
    channel_id: int,
    config: Optional[Dict[str, Any]] = None,
    *,
    slack_webhook_url: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    ch = db.query(NotificationChannel).filter(NotificationChannel.id == channel_id).first()
    if not ch:
        return None
    if config is not None:
        if ch.type == "email":
            emails = config.get("emails")
            ch.config_json = {"emails": [e.strip() for e in (emails or []) if isinstance(e, str) and e.strip()]}
        # slack_webhook: config stays {"configured": True}; URL updated via slack_webhook_url
    if ch.type == "slack_webhook":
        if slack_webhook_url is not None:
            set_webhook_url(ch.id, slack_webhook_url.strip() if slack_webhook_url else None)
        ch.config_json = {"configured": bool(get_webhook_url(ch.id))}
    db.commit()
    db.refresh(ch)
    return channel_to_response(ch)


def delete_channel(db: Session, channel_id: int) -> bool:
    ch = db.query(NotificationChannel).filter(NotificationChannel.id == channel_id).first()
    if not ch:
        return False
    if ch.type == "slack_webhook":
        set_webhook_url(ch.id, None)
    db.delete(ch)
    db.commit()
    return True


def channel_to_response(ch: NotificationChannel) -> Dict[str, Any]:
    out = {
        "id": ch.id,
        "type": ch.type,
        "config": ch.config_json or {},
        "is_verified": ch.is_verified,
        "created_at": ch.created_at.isoformat() + "Z" if ch.created_at else None,
    }
    if ch.type == "slack_webhook":
        out["config"] = {"configured": bool(get_webhook_url(ch.id))}
    return out


# ---- User preferences (defaults: notifications off) ----

def list_prefs(db: Session, user_id: str) -> List[Dict[str, Any]]:
    """List notification preferences for a user (one row per channel)."""
    prefs = (
        db.query(UserNotificationPref)
        .filter(UserNotificationPref.user_id == user_id)
        .order_by(UserNotificationPref.channel_id)
        .all()
    )
    return [pref_to_response(p) for p in prefs]


def get_pref(db: Session, pref_id: int) -> Optional[Dict[str, Any]]:
    p = db.query(UserNotificationPref).filter(UserNotificationPref.id == pref_id).first()
    return pref_to_response(p) if p else None


def upsert_pref(
    db: Session,
    user_id: str,
    channel_id: int,
    *,
    severities: Optional[List[str]] = None,
    digest_mode: str = "realtime",
    quiet_hours: Optional[Dict[str, Any]] = None,
    is_enabled: bool = False,
) -> Dict[str, Any]:
    """Create or update user pref for (user_id, channel_id). Default is_enabled=False (notifications off)."""
    p = (
        db.query(UserNotificationPref)
        .filter(
            UserNotificationPref.user_id == user_id,
            UserNotificationPref.channel_id == channel_id,
        )
        .first()
    )
    severities = severities if severities is not None else []
    if not isinstance(digest_mode, str) or digest_mode not in ("realtime", "daily"):
        digest_mode = "realtime"
    if p:
        p.severities_json = severities
        p.digest_mode = digest_mode
        p.quiet_hours_json = quiet_hours
        p.is_enabled = is_enabled
        p.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(p)
        return pref_to_response(p)
    p = UserNotificationPref(
        user_id=user_id,
        channel_id=channel_id,
        severities_json=severities,
        digest_mode=digest_mode,
        quiet_hours_json=quiet_hours,
        is_enabled=is_enabled,
    )
    db.add(p)
    db.commit()
    db.refresh(p)
    return pref_to_response(p)


def update_pref(
    db: Session,
    pref_id: int,
    *,
    severities: Optional[List[str]] = None,
    digest_mode: Optional[str] = None,
    quiet_hours: Optional[Dict[str, Any]] = None,
    is_enabled: Optional[bool] = None,
) -> Optional[Dict[str, Any]]:
    p = db.query(UserNotificationPref).filter(UserNotificationPref.id == pref_id).first()
    if not p:
        return None
    if severities is not None:
        p.severities_json = severities
    if digest_mode is not None and digest_mode in ("realtime", "daily"):
        p.digest_mode = digest_mode
    if quiet_hours is not None:
        p.quiet_hours_json = quiet_hours
    if is_enabled is not None:
        p.is_enabled = is_enabled
    p.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(p)
    return pref_to_response(p)


def delete_pref(db: Session, pref_id: int) -> bool:
    p = db.query(UserNotificationPref).filter(UserNotificationPref.id == pref_id).first()
    if not p:
        return False
    db.delete(p)
    db.commit()
    return True


def pref_to_response(p: UserNotificationPref) -> Dict[str, Any]:
    return {
        "id": p.id,
        "user_id": p.user_id,
        "channel_id": p.channel_id,
        "severities": p.severities_json or [],
        "digest_mode": p.digest_mode,
        "quiet_hours": p.quiet_hours_json,
        "is_enabled": p.is_enabled,
        "created_at": p.created_at.isoformat() + "Z" if p.created_at else None,
        "updated_at": p.updated_at.isoformat() + "Z" if p.updated_at else None,
    }
