"""SQLAlchemy models for model config versioning and data quality."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    Float,
    Integer,
    String,
    Text,
    Boolean,
    JSON,
    ForeignKey,
)
from sqlalchemy.orm import relationship

from .db import Base


class ModelConfigStatus(str):
    DRAFT = "draft"
    ACTIVE = "active"
    ARCHIVED = "archived"


class ModelConfig(Base):
    __tablename__ = "model_configs"

    id = Column(String(36), primary_key=True)
    name = Column(String(255), nullable=False, index=True)
    status = Column(
        Enum(
            ModelConfigStatus.DRAFT,
            ModelConfigStatus.ACTIVE,
            ModelConfigStatus.ARCHIVED,
            name="model_config_status",
        ),
        nullable=False,
        default=ModelConfigStatus.DRAFT,
    )
    version = Column(Integer, nullable=False, default=1)
    parent_id = Column(String(36), ForeignKey("model_configs.id"), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_by = Column(String(255), nullable=False)
    change_note = Column(Text, nullable=True)
    config_json = Column(JSON, nullable=False)
    activated_at = Column(DateTime, nullable=True)

    parent = relationship("ModelConfig", remote_side=[id], backref="children")
    audits = relationship("ModelConfigAudit", back_populates="config", cascade="all, delete-orphan")


class ModelConfigAudit(Base):
    __tablename__ = "model_config_audit"

    id = Column(Integer, primary_key=True, autoincrement=True)
    model_config_id = Column(String(36), ForeignKey("model_configs.id"), nullable=False, index=True)
    actor = Column(String(255), nullable=False)
    action = Column(String(64), nullable=False)  # create / update / activate / archive
    diff_json = Column(JSON, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    config = relationship("ModelConfig", back_populates="audits")


class DQSnapshot(Base):
    __tablename__ = "dq_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_bucket = Column(DateTime, nullable=False, index=True)
    source = Column(String(64), nullable=False, index=True)
    metric_key = Column(String(128), nullable=False, index=True)
    metric_value = Column(Float, nullable=False)
    meta_json = Column(JSON, nullable=True)


class DQAlertRule(Base):
    __tablename__ = "dq_alert_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    metric_key = Column(String(128), nullable=False, index=True)
    source = Column(String(64), nullable=True, index=True)
    threshold_type = Column(String(32), nullable=False)  # gt / lt / abs_change / pct_change
    threshold_value = Column(Float, nullable=False)
    lookback_period_days = Column(Integer, nullable=False, default=7)
    severity = Column(String(32), nullable=False, default="warn")  # info / warn / critical
    is_enabled = Column(Boolean, nullable=False, default=True)


class DQAlert(Base):
    __tablename__ = "dq_alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(Integer, ForeignKey("dq_alert_rules.id"), nullable=False, index=True)
    triggered_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    ts_bucket = Column(DateTime, nullable=False)
    metric_value = Column(Float, nullable=False)
    baseline_value = Column(Float, nullable=True)
    status = Column(String(32), nullable=False, default="open")  # open/acked/resolved
    message = Column(Text, nullable=False)


class NotificationEndpoint(Base):
    __tablename__ = "notification_endpoints"

    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String(32), nullable=False)  # webhook/email/slack (placeholder)
    target = Column(String(512), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class AttributionQualitySnapshot(Base):
    __tablename__ = "attribution_quality_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_bucket = Column(DateTime, nullable=False, index=True)
    scope = Column(String(32), nullable=False, index=True)  # global/channel/campaign
    scope_id = Column(String(255), nullable=True, index=True)
    conversion_key = Column(String(64), nullable=True, index=True)
    confidence_score = Column(Float, nullable=False)
    confidence_label = Column(String(16), nullable=False)  # high/medium/low
    components_json = Column(JSON, nullable=False)


class Experiment(Base):
    __tablename__ = "experiments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    channel = Column(String(64), nullable=False)
    start_at = Column(DateTime, nullable=False)
    end_at = Column(DateTime, nullable=False)
    status = Column(String(32), nullable=False, default="draft")  # draft/running/completed
    config_id = Column(String(36), nullable=True)
    config_version = Column(Integer, nullable=True)
    conversion_key = Column(String(64), nullable=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class ExperimentAssignment(Base):
    __tablename__ = "experiment_assignments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    experiment_id = Column(Integer, ForeignKey("experiments.id"), nullable=False, index=True)
    profile_id = Column(String(128), nullable=False, index=True)
    group = Column(String(16), nullable=False)  # control / treatment
    assigned_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class ExperimentExposure(Base):
    __tablename__ = "experiment_exposures"

    id = Column(Integer, primary_key=True, autoincrement=True)
    experiment_id = Column(Integer, ForeignKey("experiments.id"), nullable=False, index=True)
    profile_id = Column(String(128), nullable=False, index=True)
    exposure_ts = Column(DateTime, nullable=False)
    campaign_id = Column(String(128), nullable=True)
    message_id = Column(String(128), nullable=True)


class ExperimentOutcome(Base):
    __tablename__ = "experiment_outcomes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    experiment_id = Column(Integer, ForeignKey("experiments.id"), nullable=False, index=True)
    profile_id = Column(String(128), nullable=False, index=True)
    conversion_ts = Column(DateTime, nullable=False)
    value = Column(Float, nullable=False, default=0.0)


class ExperimentResult(Base):
    __tablename__ = "experiment_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    experiment_id = Column(Integer, ForeignKey("experiments.id"), nullable=False, unique=True)
    computed_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    uplift_abs = Column(Float, nullable=False)
    uplift_rel = Column(Float, nullable=True)
    ci_low = Column(Float, nullable=True)
    ci_high = Column(Float, nullable=True)
    p_value = Column(Float, nullable=True)
    treatment_size = Column(Integer, nullable=False)
    control_size = Column(Integer, nullable=False)
    meta_json = Column(JSON, nullable=True)


class ConversionPath(Base):
    __tablename__ = "conversion_paths"

    id = Column(Integer, primary_key=True, autoincrement=True)
    conversion_id = Column(String(128), nullable=False, index=True)
    profile_id = Column(String(128), nullable=False, index=True)
    conversion_key = Column(String(64), nullable=True, index=True)
    conversion_ts = Column(DateTime, nullable=False, index=True)
    path_json = Column(JSON, nullable=False)
    path_hash = Column(String(64), nullable=False, index=True)
    length = Column(Integer, nullable=False)
    first_touch_ts = Column(DateTime, nullable=False)
    last_touch_ts = Column(DateTime, nullable=False)


class PathAggregate(Base):
    __tablename__ = "path_aggregates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, nullable=False, index=True)
    conversion_key = Column(String(64), nullable=True, index=True)
    path_hash = Column(String(64), nullable=False, index=True)
    count = Column(Integer, nullable=False)
    avg_time_to_convert = Column(Float, nullable=True)
    top_channels_json = Column(JSON, nullable=True)


class PathCluster(Base):
    __tablename__ = "path_clusters"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, nullable=False, index=True)
    conversion_key = Column(String(64), nullable=True, index=True)
    cluster_id = Column(Integer, nullable=False)
    cluster_name = Column(String(255), nullable=True)
    size = Column(Integer, nullable=False)
    centroid_json = Column(JSON, nullable=True)
    top_paths_json = Column(JSON, nullable=True)


class PathClusterMembership(Base):
    __tablename__ = "path_cluster_membership"

    id = Column(Integer, primary_key=True, autoincrement=True)
    path_hash = Column(String(64), nullable=False, index=True)
    conversion_key = Column(String(64), nullable=True, index=True)
    cluster_id = Column(Integer, nullable=False)
    date = Column(DateTime, nullable=False, index=True)


class PathAnomaly(Base):
    __tablename__ = "path_anomalies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, nullable=False, index=True)
    conversion_key = Column(String(64), nullable=True, index=True)
    anomaly_type = Column(String(64), nullable=False)
    metric_key = Column(String(128), nullable=False)
    metric_value = Column(Float, nullable=False)
    baseline_value = Column(Float, nullable=True)
    severity = Column(String(32), nullable=False, default="warn")
    details_json = Column(JSON, nullable=True)
    status = Column(String(32), nullable=False, default="open")  # open/acked/resolved


