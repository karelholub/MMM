"""
Integration tests for alert notification delivery: idempotency, templates, realtime trigger, digest.
"""

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_overview_alerts import (
    AlertEvent,
    AlertRule,
    NotificationChannel,
    NotificationDelivery,
    UserNotificationPref,
)
from app.services_delivery import (
    is_delivered,
    render_email_body,
    render_email_subject,
    render_slack_payload,
    run_daily_digest,
    trigger_realtime_for_new_open_events,
    _get_or_create_delivery,
    _in_quiet_hours,
    _severity_matches,
)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def alert_rule(db_session):
    r = AlertRule(
        name="Test rule",
        scope="default",
        severity="warn",
        rule_type="threshold",
        schedule="daily",
        created_by="test",
    )
    db_session.add(r)
    db_session.commit()
    db_session.refresh(r)
    return r


@pytest.fixture
def open_alert_event(db_session, alert_rule):
    ev = AlertEvent(
        rule_id=alert_rule.id,
        ts_detected=datetime.utcnow(),
        severity="warn",
        title="Test alert",
        message="KPI exceeded threshold",
        status="open",
    )
    db_session.add(ev)
    db_session.commit()
    db_session.refresh(ev)
    return ev


@pytest.fixture
def email_channel(db_session):
    ch = NotificationChannel(type="email", config_json={"emails": ["alerts@example.com"]})
    db_session.add(ch)
    db_session.commit()
    db_session.refresh(ch)
    return ch


@pytest.fixture
def slack_channel(db_session, tmp_path):
    ch = NotificationChannel(type="slack_webhook", config_json={"configured": True})
    db_session.add(ch)
    db_session.commit()
    db_session.refresh(ch)
    return ch


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_get_or_create_delivery_creates_once(db_session, open_alert_event, email_channel):
    rec1, created1 = _get_or_create_delivery(db_session, open_alert_event.id, email_channel.id)
    db_session.commit()
    assert created1 is True
    assert rec1.status == "pending"
    assert rec1.alert_event_id == open_alert_event.id
    assert rec1.channel_id == email_channel.id

    rec2, created2 = _get_or_create_delivery(db_session, open_alert_event.id, email_channel.id)
    assert created2 is False
    assert rec2.id == rec1.id


def test_is_delivered_false_until_marked(db_session, open_alert_event, email_channel):
    assert is_delivered(db_session, open_alert_event.id, email_channel.id) is False
    rec, _ = _get_or_create_delivery(db_session, open_alert_event.id, email_channel.id)
    db_session.commit()
    assert is_delivered(db_session, open_alert_event.id, email_channel.id) is False
    rec.status = "sent"
    db_session.commit()
    assert is_delivered(db_session, open_alert_event.id, email_channel.id) is True


# ---------------------------------------------------------------------------
# Templates (no PII)
# ---------------------------------------------------------------------------


def test_render_email_subject():
    ev = MagicMockEvent(severity="critical", title="Data freshness")
    assert "CRITICAL" in render_email_subject(ev, digest=False)
    assert "Data freshness" in render_email_subject(ev, digest=False)
    assert "[MMM] Daily alert digest" == render_email_subject(ev, digest=True)


def test_render_email_body_no_pii(open_alert_event):
    body = render_email_body(open_alert_event, base_url="https://app.example.com")
    assert "Test alert" in body
    assert "KPI exceeded threshold" in body
    assert "alert_id=" in body
    # No user_id, email, or other PII in body
    assert "user" not in body.lower() or "user_id" not in body
    assert "alerts@example.com" not in body


def test_render_slack_payload_no_pii(open_alert_event):
    payload = render_slack_payload(open_alert_event, base_url="https://app.example.com")
    assert "blocks" in payload
    text = payload["blocks"][0]["text"]["text"]
    assert "WARN" in text or "warn" in text
    assert "Test alert" in text
    assert "alert_id=" in text


class MagicMockEvent:
    def __init__(self, severity="warn", title="Test", message="Msg", id=1):
        self.id = id
        self.severity = severity
        self.title = title
        self.message = message


def test_render_email_digest_body():
    ev1 = MagicMockEvent(title="A", message="M1", id=1)
    ev2 = MagicMockEvent(title="B", message="M2", id=2)
    body = render_email_body(ev1, base_url="", digest=True, events=[ev1, ev2])
    assert "Daily alert summary" in body
    assert "A" in body and "M1" in body
    assert "B" in body and "M2" in body


# ---------------------------------------------------------------------------
# Quiet hours and severity
# ---------------------------------------------------------------------------


def test_in_quiet_hours_empty():
    assert _in_quiet_hours(datetime(2024, 2, 15, 3, 0), None) is False
    assert _in_quiet_hours(datetime(2024, 2, 15, 3, 0), {}) is False


def test_in_quiet_hours_same_day_window():
    # 22:00 - 06:00 next day
    q = {"start": "22:00", "end": "06:00"}
    assert _in_quiet_hours(datetime(2024, 2, 15, 23, 0), q) is True
    assert _in_quiet_hours(datetime(2024, 2, 15, 2, 0), q) is True
    assert _in_quiet_hours(datetime(2024, 2, 15, 9, 0), q) is False
    assert _in_quiet_hours(datetime(2024, 2, 15, 21, 0), q) is False


def test_in_quiet_hours_respects_timezone_metadata():
    q = {"start": "22:00", "end": "06:00", "timezone": "America/Los_Angeles"}
    # 12:30 UTC == 04:30 in Los Angeles (inside quiet window)
    assert _in_quiet_hours(datetime(2024, 2, 15, 12, 30), q) is True
    # 18:00 UTC == 10:00 in Los Angeles (outside quiet window)
    assert _in_quiet_hours(datetime(2024, 2, 15, 18, 0), q) is False


def test_severity_matches_empty_means_all():
    pref = MagicMockPref(severities_json=[])
    assert _severity_matches(pref, "warn") is True
    assert _severity_matches(pref, "critical") is True


def test_severity_matches_filter():
    pref = MagicMockPref(severities_json=["warn", "critical"])
    assert _severity_matches(pref, "warn") is True
    assert _severity_matches(pref, "critical") is True
    assert _severity_matches(pref, "info") is False


class MagicMockPref:
    def __init__(self, severities_json=None):
        self.severities_json = severities_json or []


# ---------------------------------------------------------------------------
# Realtime trigger (mocked HTTP and email)
# ---------------------------------------------------------------------------


def test_trigger_realtime_empty_ids(db_session):
    out = trigger_realtime_for_new_open_events(db_session, [])
    assert out == {"delivered": 0, "skipped": 0, "failed": 0}


def test_trigger_realtime_delivers_to_realtime_pref(
    db_session, open_alert_event, email_channel
):
    pref = UserNotificationPref(
        user_id="user1",
        channel_id=email_channel.id,
        digest_mode="realtime",
        is_enabled=True,
        severities_json=["warn"],
    )
    db_session.add(pref)
    db_session.commit()

    emails_sent = []

    def capture_email(recipients, subject, body):
        emails_sent.append({"to": recipients, "subject": subject, "body": body})

    with patch("app.services_delivery._email_sender", capture_email):
        out = trigger_realtime_for_new_open_events(
            db_session, [open_alert_event.id], base_url="https://app.example.com"
        )
    assert out["delivered"] == 1
    assert len(emails_sent) == 1
    assert "Test alert" in emails_sent[0]["subject"]
    assert "alert_id=" in emails_sent[0]["body"]
    assert is_delivered(db_session, open_alert_event.id, email_channel.id) is True


def test_trigger_realtime_idempotent_skips_second_send(
    db_session, open_alert_event, email_channel
):
    pref = UserNotificationPref(
        user_id="user1",
        channel_id=email_channel.id,
        digest_mode="realtime",
        is_enabled=True,
        severities_json=[],
    )
    db_session.add(pref)
    db_session.commit()

    emails_sent = []

    def capture_email(recipients, subject, body):
        emails_sent.append(1)

    with patch("app.services_delivery._email_sender", capture_email):
        trigger_realtime_for_new_open_events(db_session, [open_alert_event.id])
        out2 = trigger_realtime_for_new_open_events(db_session, [open_alert_event.id])
    assert len(emails_sent) == 1
    assert out2["skipped"] == 1


def test_trigger_realtime_slack_mocked(db_session, open_alert_event, slack_channel):
    pref = UserNotificationPref(
        user_id="user1",
        channel_id=slack_channel.id,
        digest_mode="realtime",
        is_enabled=True,
        severities_json=["warn"],
    )
    db_session.add(pref)
    db_session.commit()

    with patch("app.services_delivery.get_webhook_url", return_value="https://hooks.slack.com/test"):
        with patch("app.services_delivery.requests.post") as mock_post:
            mock_post.return_value.raise_for_status = lambda: None
            out = trigger_realtime_for_new_open_events(db_session, [open_alert_event.id])
    assert out["delivered"] == 1
    assert mock_post.called
    payload = mock_post.call_args[1]["json"]
    assert "blocks" in payload
    assert "Test alert" in str(payload)


# ---------------------------------------------------------------------------
# Daily digest and quiet hours
# ---------------------------------------------------------------------------


def test_daily_digest_respects_quiet_hours(db_session, alert_rule, email_channel):
    # User with daily digest and quiet hours 22:00-06:00
    pref = UserNotificationPref(
        user_id="u1",
        channel_id=email_channel.id,
        digest_mode="daily",
        is_enabled=True,
        quiet_hours_json={"start": "22:00", "end": "06:00"},
        severities_json=["warn"],
    )
    db_session.add(pref)
    # Open event from today
    ev = AlertEvent(
        rule_id=alert_rule.id,
        ts_detected=datetime.utcnow(),
        severity="warn",
        title="Digest alert",
        message="For digest",
        status="open",
    )
    db_session.add(ev)
    db_session.commit()
    db_session.refresh(ev)

    # Run at 3 AM -> inside quiet hours -> no pref included -> digests_sent = 0
    with patch("app.services_delivery._email_sender", lambda *a: None):
        result = run_daily_digest(db_session, now=datetime(2024, 2, 15, 3, 0))
    assert result["digests_sent"] == 0

    # Run at 9 AM -> outside quiet hours -> digest sent
    with patch("app.services_delivery._email_sender", lambda *a: None):
        result = run_daily_digest(db_session, now=datetime(2024, 2, 15, 9, 0))
    assert result["digests_sent"] == 1


def test_daily_digest_only_undelivered_events(db_session, alert_rule, email_channel):
    pref = UserNotificationPref(
        user_id="u1",
        channel_id=email_channel.id,
        digest_mode="daily",
        is_enabled=True,
        severities_json=["warn"],
    )
    db_session.add(pref)
    ev = AlertEvent(
        rule_id=alert_rule.id,
        ts_detected=datetime.utcnow(),
        severity="warn",
        title="One",
        message="First",
        status="open",
    )
    db_session.add(ev)
    db_session.commit()
    db_session.refresh(ev)

    digests = []

    def capture(recipients, subject, body):
        digests.append({"subject": subject, "body": body})

    with patch("app.services_delivery._email_sender", capture):
        run_daily_digest(db_session, now=datetime(2024, 2, 15, 10, 0))
    assert len(digests) == 1
    assert "digest" in digests[0]["subject"].lower()
    assert "One" in digests[0]["body"]

    # Second run: already delivered for this channel -> no new digest
    with patch("app.services_delivery._email_sender", capture):
        result = run_daily_digest(db_session, now=datetime(2024, 2, 15, 10, 0))
    assert result["digests_sent"] == 0
    assert len(digests) == 1  # still 1, no second send
