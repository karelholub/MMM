"""
Notification delivery for alert_events.

- On new "open" alert_event: deliver by user_notification_pref (realtime → immediate;
  digest/daily → accumulate, send in daily job).
- Channels: email (simple template), slack_webhook (simple payload).
- Idempotency: (alert_event_id, channel_id); no duplicate sends.
- Failures: logged and retried with exponential backoff.
- No PII in message body (rule name, severity, message, link only).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
from sqlalchemy.orm import Session

from .models_overview_alerts import (
    AlertEvent,
    NotificationChannel,
    NotificationDelivery,
    UserNotificationPref,
)
from .utils.notification_secrets import get_webhook_url

logger = logging.getLogger(__name__)

# Max retries and backoff (seconds): 1, 2, 4
MAX_DELIVERY_RETRIES = 3
BACKOFF_BASE_SECONDS = 1.0

# Optional: inject email sender (e.g. SMTP); if None, delivery logs only (no PII in body).
_email_sender: Optional[Callable[[List[str], str, str], None]] = None


def set_email_sender(fn: Callable[[List[str], str, str], None]) -> None:
    """Set the email sender: fn(recipient_emails, subject, body_plain)."""
    global _email_sender
    _email_sender = fn


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def _get_or_create_delivery(
    db: Session, alert_event_id: int, channel_id: int
) -> Tuple[NotificationDelivery, bool]:
    """Get existing delivery record or create pending. Returns (record, created)."""
    rec = (
        db.query(NotificationDelivery)
        .filter(
            NotificationDelivery.alert_event_id == alert_event_id,
            NotificationDelivery.channel_id == channel_id,
        )
        .first()
    )
    if rec:
        return rec, False
    rec = NotificationDelivery(
        alert_event_id=alert_event_id,
        channel_id=channel_id,
        status="pending",
        retry_count=0,
    )
    db.add(rec)
    db.flush()
    return rec, True


def is_delivered(db: Session, alert_event_id: int, channel_id: int) -> bool:
    """True if this (event, channel) was already successfully sent."""
    rec = (
        db.query(NotificationDelivery)
        .filter(
            NotificationDelivery.alert_event_id == alert_event_id,
            NotificationDelivery.channel_id == channel_id,
        )
        .first()
    )
    return rec is not None and rec.status == "sent"


def mark_delivered(db: Session, delivery_id: int) -> None:
    rec = db.query(NotificationDelivery).filter(NotificationDelivery.id == delivery_id).first()
    if rec:
        rec.status = "sent"
        rec.delivered_at = datetime.utcnow()
        rec.last_error = None
        rec.updated_at = datetime.utcnow()


def mark_failed(db: Session, delivery_id: int, error: str) -> None:
    rec = db.query(NotificationDelivery).filter(NotificationDelivery.id == delivery_id).first()
    if rec:
        rec.status = "failed"
        rec.last_error = (error or "")[:2000]
        rec.retry_count += 1
        rec.updated_at = datetime.utcnow()


# ---------------------------------------------------------------------------
# Templates (no PII in content: rule name, severity, message, link only)
# ---------------------------------------------------------------------------


def _dashboard_link(alert_event_id: int, base_url: str = "") -> str:
    return f"{base_url.rstrip('/')}/dashboard/data-quality?alert_id={alert_event_id}"


def render_email_subject(event: AlertEvent, digest: bool = False) -> str:
    if digest:
        return "[MMM] Daily alert digest"
    return f"[MMM] {event.severity.upper()}: {event.title}"


def render_email_body(
    event: AlertEvent,
    base_url: str = "",
    digest: bool = False,
    events: Optional[List[AlertEvent]] = None,
) -> str:
    """Plain text body; no PII."""
    if digest and events:
        lines = ["Daily alert summary\n"]
        for e in events:
            link = _dashboard_link(e.id, base_url)
            lines.append(f"- [{e.severity}] {e.title}")
            lines.append(f"  {e.message}")
            lines.append(f"  View: {link}\n")
        return "\n".join(lines)
    link = _dashboard_link(event.id, base_url)
    return f"{event.title}\n\n{event.message}\n\nView: {link}"


def render_slack_payload(
    event: AlertEvent,
    base_url: str = "",
    digest: bool = False,
    events: Optional[List[AlertEvent]] = None,
) -> Dict[str, Any]:
    """Slack block kit compatible payload; no PII in text."""
    if digest and events:
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "*Daily alert digest*"}}]
        for e in events:
            link = _dashboard_link(e.id, base_url)
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*[{e.severity}]* {e.title}\n{e.message}\n<{link}|View>",
                },
            })
        return {"blocks": blocks}
    link = _dashboard_link(event.id, base_url)
    return {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*[{event.severity.upper()}]* {event.title}\n{event.message}\n<{link}|View>",
                },
            }
        ]
    }


# ---------------------------------------------------------------------------
# Senders (with retry and backoff)
# ---------------------------------------------------------------------------


def _send_slack(channel_id: int, payload: Dict[str, Any]) -> None:
    url = get_webhook_url(channel_id)
    if not url:
        raise ValueError("Slack webhook URL not configured for channel")
    r = requests.post(url, json=payload, timeout=10)
    r.raise_for_status()


def _send_email(channel: NotificationChannel, subject: str, body: str) -> None:
    emails = (channel.config_json or {}).get("emails") or []
    if not emails:
        raise ValueError("No email addresses configured for channel")
    if _email_sender:
        _email_sender(emails, subject, body)
    else:
        logger.info("Email (no sender configured): to=%s subject=%s", len(emails), subject[:50])


def _deliver_with_retry(
    db: Session,
    delivery_id: int,
    channel: NotificationChannel,
    event: AlertEvent,
    base_url: str,
    digest: bool = False,
    digest_events: Optional[List[AlertEvent]] = None,
) -> bool:
    """Perform one delivery attempt with up to MAX_DELIVERY_RETRIES and backoff. Returns True if sent."""
    last_error: Optional[str] = None
    for attempt in range(MAX_DELIVERY_RETRIES):
        try:
            if channel.type == "email":
                subject = render_email_subject(event, digest=digest)
                body = render_email_body(event, base_url, digest=digest, events=digest_events)
                _send_email(channel, subject, body)
            elif channel.type == "slack_webhook":
                payload = render_slack_payload(event, base_url, digest=digest, events=digest_events)
                _send_slack(channel.id, payload)
            else:
                last_error = f"Unsupported channel type: {channel.type}"
                break
            mark_delivered(db, delivery_id)
            db.commit()
            return True
        except Exception as e:
            last_error = str(e)
            logger.warning(
                "Delivery attempt %s/%s failed for delivery_id=%s: %s",
                attempt + 1,
                MAX_DELIVERY_RETRIES,
                delivery_id,
                last_error,
                exc_info=True,
            )
            if attempt < MAX_DELIVERY_RETRIES - 1:
                time.sleep(BACKOFF_BASE_SECONDS * (2 ** attempt))
    mark_failed(db, delivery_id, last_error or "Unknown error")
    db.commit()
    return False


# ---------------------------------------------------------------------------
# Preference filter and quiet hours
# ---------------------------------------------------------------------------


def _severity_matches(pref: UserNotificationPref, severity: str) -> bool:
    severities = pref.severities_json or []
    if not severities:
        return True
    return severity in severities


def _in_quiet_hours(now: datetime, quiet_hours_json: Optional[Dict[str, Any]]) -> bool:
    """True if now falls inside quiet hours. Expects {"start": "HH:MM", "end": "HH:MM"} (24h)."""
    if not quiet_hours_json or not isinstance(quiet_hours_json, dict):
        return False
    start_s = quiet_hours_json.get("start")
    end_s = quiet_hours_json.get("end")
    if not start_s or not end_s:
        return False
    try:
        start_parts = start_s.split(":")
        end_parts = end_s.split(":")
        start_min = int(start_parts[0]) * 60 + int(start_parts[1]) if len(start_parts) >= 2 else int(start_parts[0]) * 60
        end_min = int(end_parts[0]) * 60 + int(end_parts[1]) if len(end_parts) >= 2 else int(end_parts[0]) * 60
        now_min = now.hour * 60 + now.minute
        if start_min <= end_min:
            return start_min <= now_min < end_min
        return now_min >= start_min or now_min < end_min
    except (ValueError, TypeError):
        return False


def _get_prefs_for_realtime(db: Session, severity: str) -> List[Tuple[UserNotificationPref, NotificationChannel]]:
    """Enabled prefs that want realtime and match severity; channel loaded."""
    prefs = (
        db.query(UserNotificationPref)
        .filter(UserNotificationPref.is_enabled == True, UserNotificationPref.digest_mode == "realtime")
        .all()
    )
    out = []
    for p in prefs:
        if not _severity_matches(p, severity):
            continue
        ch = db.query(NotificationChannel).filter(NotificationChannel.id == p.channel_id).first()
        if ch:
            out.append((p, ch))
    return out


def _get_prefs_for_daily_digest(db: Session, now: datetime) -> List[Tuple[UserNotificationPref, NotificationChannel]]:
    """Enabled prefs with digest_mode=daily for which now is outside quiet hours."""
    prefs = (
        db.query(UserNotificationPref)
        .filter(UserNotificationPref.is_enabled == True, UserNotificationPref.digest_mode == "daily")
        .all()
    )
    out = []
    for p in prefs:
        if _in_quiet_hours(now, p.quiet_hours_json):
            continue
        ch = db.query(NotificationChannel).filter(NotificationChannel.id == p.channel_id).first()
        if ch:
            out.append((p, ch))
    return out


# ---------------------------------------------------------------------------
# Public API: trigger on new open event, and digest job
# ---------------------------------------------------------------------------


def trigger_realtime_for_new_open_events(
    db: Session,
    alert_event_ids: List[int],
    base_url: str = "",
) -> Dict[str, Any]:
    """
    For each new open alert_event, deliver immediately to prefs with digest_mode=realtime
    (idempotent per alert_event_id + channel_id).
    """
    if not alert_event_ids:
        return {"delivered": 0, "skipped": 0, "failed": 0}
    events = (
        db.query(AlertEvent)
        .filter(AlertEvent.id.in_(alert_event_ids), AlertEvent.status == "open")
        .all()
    )
    delivered, skipped, failed = 0, 0, 0
    for event in events:
        prefs_channels = _get_prefs_for_realtime(db, event.severity)
        for pref, channel in prefs_channels:
            if is_delivered(db, event.id, channel.id):
                skipped += 1
                continue
            rec, created = _get_or_create_delivery(db, event.id, channel.id)
            if not created and rec.status == "sent":
                skipped += 1
                continue
            db.commit()
            if _deliver_with_retry(db, rec.id, channel, event, base_url):
                delivered += 1
            else:
                failed += 1
    return {"delivered": delivered, "skipped": skipped, "failed": failed}


def run_daily_digest(
    db: Session,
    base_url: str = "",
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Send daily digest to prefs with digest_mode=daily, respecting quiet hours.
    Collects open alert_events from the last 24h that have not been delivered to each channel.
    """
    now = now or datetime.utcnow()
    prefs_channels = _get_prefs_for_daily_digest(db, now)
    if not prefs_channels:
        return {"digests_sent": 0, "failed": 0}
    since = datetime(now.year, now.month, now.day, 0, 0, 0) - timedelta(days=1)
    open_events = (
        db.query(AlertEvent)
        .filter(
            AlertEvent.status == "open",
            AlertEvent.ts_detected >= since,
        )
        .order_by(AlertEvent.ts_detected.desc())
        .all()
    )
    digests_sent, failed = 0, 0
    for pref, channel in prefs_channels:
        # Events not yet delivered to this channel
        to_send = []
        for ev in open_events:
            if not is_delivered(db, ev.id, channel.id) and _severity_matches(pref, ev.severity):
                to_send.append(ev)
        if not to_send:
            continue
        # One digest per channel: create one pending delivery per event for idempotency
        first_ev = to_send[0]
        rec, _ = _get_or_create_delivery(db, first_ev.id, channel.id)
        if rec.status == "sent":
            continue
        db.commit()
        if _deliver_with_retry(
            db, rec.id, channel, first_ev, base_url, digest=True, digest_events=to_send
        ):
            digests_sent += 1
            for ev in to_send:
                if ev.id != first_ev.id:
                    rec2, created = _get_or_create_delivery(db, ev.id, channel.id)
                    if created:
                        mark_delivered(db, rec2.id)
            db.commit()
        else:
            failed += 1
    return {"digests_sent": digests_sent, "failed": failed}
