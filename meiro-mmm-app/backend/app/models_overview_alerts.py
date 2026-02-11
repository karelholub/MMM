"""SQLAlchemy models for Overview snapshots and Alerts."""

from __future__ import annotations

from datetime import datetime
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
    JSON,
    ForeignKey,
)
from sqlalchemy.orm import relationship

from .db import Base


class MetricSnapshot(Base):
    """Hourly or daily KPI snapshots for Overview dashboards (optional for MVP)."""

    __tablename__ = "metric_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, nullable=False, index=True)
    scope = Column(String(64), nullable=False, index=True)  # account / workspace
    kpi_key = Column(String(128), nullable=False, index=True)
    kpi_value = Column(Float, nullable=False)
    dimensions_json = Column(JSON, nullable=True)
    computed_from = Column(String(32), nullable=False)  # raw | model
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class AlertRule(Base):
    """Alert rule definition: anomaly_kpi, threshold, data_freshness, pipeline_health."""

    __tablename__ = "alert_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    is_enabled = Column(Boolean, nullable=False, default=True)
    scope = Column(String(64), nullable=False, index=True)  # account / workspace
    severity = Column(String(32), nullable=False)  # info / warn / critical
    rule_type = Column(String(64), nullable=False)  # anomaly_kpi | threshold | data_freshness | pipeline_health
    kpi_key = Column(String(128), nullable=True, index=True)
    dimension_filters_json = Column(JSON, nullable=True)
    params_json = Column(JSON, nullable=True)  # zscore_threshold, lookback_days, min_volume, threshold_value, etc.
    schedule = Column(String(32), nullable=False)  # hourly / daily
    created_by = Column(String(255), nullable=False)
    updated_by = Column(String(255), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    events = relationship("AlertEvent", back_populates="rule", cascade="all, delete-orphan")


class AlertEvent(Base):
    """Fired alert event from a rule; lifecycle: open -> ack/snoozed/resolved."""

    __tablename__ = "alert_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(Integer, ForeignKey("alert_rules.id"), nullable=False, index=True)
    ts_detected = Column(DateTime, nullable=False, index=True)
    severity = Column(String(32), nullable=False)
    title = Column(String(512), nullable=False)
    message = Column(Text, nullable=False)
    context_json = Column(JSON, nullable=True)  # kpi_key, observed, expected, zscore, dims
    status = Column(String(32), nullable=False, default="open")  # open / ack / snoozed / resolved
    fingerprint = Column(String(64), nullable=True, index=True)  # hash(rule_id + dims + period) for de-duplication
    assignee_user_id = Column(String(128), nullable=True)
    related_entities_json = Column(JSON, nullable=True)  # campaign_id, channel, source, table, pipeline, etc.
    snooze_until = Column(DateTime, nullable=True)  # when snoozed status expires
    updated_by = Column(String(255), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    rule = relationship("AlertRule", back_populates="events")


class NotificationChannel(Base):
    """Delivery channel for alerts: email or slack_webhook."""

    __tablename__ = "notification_channels"

    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String(32), nullable=False)  # email | slack_webhook
    config_json = Column(JSON, nullable=False)  # webhook URL, email list ref, etc.
    is_verified = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    user_prefs = relationship("UserNotificationPref", back_populates="channel", cascade="all, delete-orphan")


class UserNotificationPref(Base):
    """Per-user, per-channel notification preferences."""

    __tablename__ = "user_notification_prefs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(128), nullable=False, index=True)
    channel_id = Column(Integer, ForeignKey("notification_channels.id"), nullable=False, index=True)
    severities_json = Column(JSON, nullable=True)  # list of severities to receive
    digest_mode = Column(String(32), nullable=False, default="realtime")  # realtime | daily
    quiet_hours_json = Column(JSON, nullable=True)
    is_enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (UniqueConstraint("user_id", "channel_id", name="uq_user_notification_pref_user_channel"),)

    channel = relationship("NotificationChannel", back_populates="user_prefs")


class NotificationDelivery(Base):
    """Idempotency record: one row per (alert_event_id, channel_id). Prevents duplicate delivery."""

    __tablename__ = "notification_deliveries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    alert_event_id = Column(Integer, ForeignKey("alert_events.id", ondelete="CASCADE"), nullable=False, index=True)
    channel_id = Column(Integer, ForeignKey("notification_channels.id", ondelete="CASCADE"), nullable=False, index=True)
    status = Column(String(32), nullable=False, default="pending")  # pending | sent | failed
    delivered_at = Column(DateTime, nullable=True)
    last_error = Column(Text, nullable=True)
    retry_count = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (UniqueConstraint("alert_event_id", "channel_id", name="uq_notification_delivery_event_channel"),)
