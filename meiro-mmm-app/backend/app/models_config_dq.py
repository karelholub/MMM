"""SQLAlchemy models for model config versioning and data quality."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    Integer,
    Index,
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


class JourneySettingsStatus(str):
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


class JourneySettingsVersion(Base):
    __tablename__ = "journey_settings_versions"

    id = Column(String(36), primary_key=True)
    status = Column(
        Enum(
            JourneySettingsStatus.DRAFT,
            JourneySettingsStatus.ACTIVE,
            JourneySettingsStatus.ARCHIVED,
            name="journey_settings_status",
        ),
        nullable=False,
        default=JourneySettingsStatus.DRAFT,
        index=True,
    )
    version_label = Column(String(64), nullable=False, default="v1")
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_by = Column(String(255), nullable=False, default="system")
    activated_at = Column(DateTime, nullable=True)
    activated_by = Column(String(255), nullable=True)
    settings_json = Column(JSON, nullable=False)
    validation_json = Column(JSON, nullable=True)
    diff_json = Column(JSON, nullable=True)


class WorkspaceSettings(Base):
    __tablename__ = "workspace_settings"

    workspace_id = Column(String(128), primary_key=True, default="default")
    active_journey_settings_version_id = Column(
        String(36),
        ForeignKey("journey_settings_versions.id"),
        nullable=True,
        index=True,
    )
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_by = Column(String(255), nullable=True)


class User(Base):
    __tablename__ = "users"

    id = Column(String(36), primary_key=True)
    username = Column(String(64), nullable=True, unique=True, index=True)
    email = Column(String(255), nullable=False, unique=True, index=True)
    name = Column(String(255), nullable=True)
    status = Column(String(32), nullable=False, default="active", index=True)  # active/disabled
    auth_provider = Column(String(32), nullable=True)  # local/oidc
    password_hash = Column(String(255), nullable=True)
    password_updated_at = Column(DateTime, nullable=True)
    external_id = Column(String(255), nullable=True, index=True)
    last_login_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class AuthSession(Base):
    __tablename__ = "auth_sessions"

    id = Column(String(64), primary_key=True)
    user_id = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    workspace_id = Column(String(36), ForeignKey("workspaces.id", ondelete="CASCADE"), nullable=False, index=True)
    csrf_token = Column(String(64), nullable=False)
    expires_at = Column(DateTime, nullable=False, index=True)
    revoked_at = Column(DateTime, nullable=True, index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_seen_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    ip = Column(String(64), nullable=True)
    user_agent = Column(String(512), nullable=True)


class Workspace(Base):
    __tablename__ = "workspaces"

    id = Column(String(36), primary_key=True)
    name = Column(String(255), nullable=False)
    slug = Column(String(128), nullable=False, unique=True, index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class Role(Base):
    __tablename__ = "roles"

    id = Column(String(36), primary_key=True)
    workspace_id = Column(String(36), ForeignKey("workspaces.id", ondelete="CASCADE"), nullable=True, index=True)
    name = Column(String(128), nullable=False)
    description = Column(Text, nullable=True)
    is_system = Column(Boolean, nullable=False, default=False, index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_roles_workspace_name", "workspace_id", "name", unique=True),
    )


class Permission(Base):
    __tablename__ = "permissions"

    key = Column(String(128), primary_key=True)
    description = Column(Text, nullable=False)
    category = Column(String(64), nullable=False, index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class RolePermission(Base):
    __tablename__ = "role_permissions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    role_id = Column(String(36), ForeignKey("roles.id", ondelete="CASCADE"), nullable=False, index=True)
    permission_key = Column(String(128), ForeignKey("permissions.key", ondelete="CASCADE"), nullable=False, index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_role_permissions_role_perm", "role_id", "permission_key", unique=True),
    )


class WorkspaceMembership(Base):
    __tablename__ = "workspace_memberships"

    id = Column(String(36), primary_key=True)
    workspace_id = Column(String(36), ForeignKey("workspaces.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    role_id = Column(String(36), ForeignKey("roles.id", ondelete="SET NULL"), nullable=True, index=True)
    status = Column(String(32), nullable=False, default="active", index=True)  # active/invited/removed
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_workspace_memberships_workspace_user", "workspace_id", "user_id", unique=True),
    )


class Invitation(Base):
    __tablename__ = "invitations"

    id = Column(String(36), primary_key=True)
    workspace_id = Column(String(36), ForeignKey("workspaces.id", ondelete="CASCADE"), nullable=False, index=True)
    email = Column(String(255), nullable=False, index=True)
    role_id = Column(String(36), ForeignKey("roles.id", ondelete="SET NULL"), nullable=True, index=True)
    token_hash = Column(String(255), nullable=False, unique=True, index=True)
    expires_at = Column(DateTime, nullable=False)
    invited_by_user_id = Column(String(36), ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    accepted_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class SecurityAuditLog(Base):
    __tablename__ = "audit_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    workspace_id = Column(String(36), ForeignKey("workspaces.id", ondelete="SET NULL"), nullable=True, index=True)
    actor_user_id = Column(String(36), ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    action_key = Column(String(128), nullable=False, index=True)
    target_type = Column(String(64), nullable=False, index=True)
    target_id = Column(String(128), nullable=True, index=True)
    metadata_json = Column(JSON, nullable=True)
    ip = Column(String(64), nullable=True)
    user_agent = Column(String(512), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


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
    note = Column(Text, nullable=True)


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


class JourneyDefinitionMode(str):
    CONVERSION_ONLY = "conversion_only"
    ALL_JOURNEYS = "all_journeys"


class JourneyDefinition(Base):
    __tablename__ = "journey_definitions"

    id = Column(String(36), primary_key=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text, nullable=True)
    conversion_kpi_id = Column(String(64), nullable=True, index=True)
    lookback_window_days = Column(Integer, nullable=False, default=30)
    mode_default = Column(
        Enum(
            JourneyDefinitionMode.CONVERSION_ONLY,
            JourneyDefinitionMode.ALL_JOURNEYS,
            name="journey_definition_mode",
        ),
        nullable=False,
        default=JourneyDefinitionMode.CONVERSION_ONLY,
    )
    created_by = Column(String(255), nullable=False, default="system")
    updated_by = Column(String(255), nullable=True)
    is_archived = Column(Boolean, nullable=False, default=False)
    archived_at = Column(DateTime, nullable=True)
    archived_by = Column(String(255), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class JourneyPathDaily(Base):
    __tablename__ = "journey_paths_daily"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    journey_definition_id = Column(String(36), ForeignKey("journey_definitions.id", ondelete="CASCADE"), nullable=False)
    path_hash = Column(Text, nullable=False)
    path_steps = Column(JSON, nullable=False)
    path_length = Column(Integer, nullable=False)
    count_journeys = Column(Integer, nullable=False, default=0)
    count_conversions = Column(Integer, nullable=False, default=0)
    avg_time_to_convert_sec = Column(Float, nullable=True)
    p50_time_to_convert_sec = Column(Float, nullable=True)
    p90_time_to_convert_sec = Column(Float, nullable=True)
    channel_group = Column(String(128), nullable=True)
    campaign_id = Column(String(128), nullable=True)
    device = Column(String(64), nullable=True)
    country = Column(String(64), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_journey_paths_daily_def_date", "journey_definition_id", "date"),
        Index("ix_journey_paths_daily_def_date_hash", "journey_definition_id", "date", "path_hash"),
    )


class JourneyTransitionDaily(Base):
    __tablename__ = "journey_transitions_daily"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    journey_definition_id = Column(String(36), ForeignKey("journey_definitions.id", ondelete="CASCADE"), nullable=False)
    from_step = Column(Text, nullable=False)
    to_step = Column(Text, nullable=False)
    count_transitions = Column(Integer, nullable=False, default=0)
    count_profiles = Column(Integer, nullable=False, default=0)
    channel_group = Column(String(128), nullable=True)
    campaign_id = Column(String(128), nullable=True)
    device = Column(String(64), nullable=True)
    country = Column(String(64), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_journey_transitions_daily_def_date", "journey_definition_id", "date"),
        Index("ix_journey_transitions_daily_def_date_from", "journey_definition_id", "date", "from_step"),
    )


class FunnelDefinition(Base):
    __tablename__ = "funnels"

    id = Column(String(36), primary_key=True)
    journey_definition_id = Column(String(36), ForeignKey("journey_definitions.id", ondelete="CASCADE"), nullable=False, index=True)
    workspace_id = Column(String(128), nullable=False, default="default", index=True)
    user_id = Column(String(128), nullable=False, default="default", index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    steps_json = Column(JSON, nullable=False)
    counting_method = Column(String(32), nullable=False, default="ordered")
    window_days = Column(Integer, nullable=False, default=30)
    is_archived = Column(Boolean, nullable=False, default=False, index=True)
    created_by = Column(String(255), nullable=False, default="system")
    updated_by = Column(String(255), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class JourneyAlertDefinition(Base):
    __tablename__ = "journey_alert_definitions"

    id = Column(String(36), primary_key=True)
    name = Column(String(255), nullable=False)
    type = Column(String(64), nullable=False, index=True)  # path_cr_drop | path_volume_change | funnel_dropoff_spike | ttc_shift
    domain = Column(String(32), nullable=False, index=True, default="journeys")  # journeys | funnels
    scope_json = Column(JSON, nullable=False)  # journey_definition_id/path_hash/funnel_id/step_index/filters
    metric = Column(String(128), nullable=False)
    condition_json = Column(JSON, nullable=False)  # threshold/comparison mode/sensitivity/cooldown
    schedule_json = Column(JSON, nullable=True)  # {"cadence":"daily"}
    is_enabled = Column(Boolean, nullable=False, default=True, index=True)
    created_by = Column(String(255), nullable=False, default="system")
    updated_by = Column(String(255), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_journey_alert_definitions_domain_enabled", "domain", "is_enabled"),
    )


class JourneyAlertEvent(Base):
    __tablename__ = "journey_alert_events"

    id = Column(String(36), primary_key=True)
    alert_definition_id = Column(String(36), ForeignKey("journey_alert_definitions.id", ondelete="CASCADE"), nullable=False, index=True)
    domain = Column(String(32), nullable=False, index=True, default="journeys")
    triggered_at = Column(DateTime, nullable=False, index=True)
    severity = Column(String(32), nullable=False)  # info/warn/critical
    summary = Column(Text, nullable=False)
    details_json = Column(JSON, nullable=False)  # computed values + time window + filters + deep-link payload
    dedupe_key = Column(String(128), nullable=True, index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_journey_alert_events_definition_triggered", "alert_definition_id", "triggered_at"),
        Index("ix_journey_alert_events_domain_triggered", "domain", "triggered_at"),
    )


class SecretStore(Base):
    __tablename__ = "secret_store"

    id = Column(String(36), primary_key=True)
    workspace_id = Column(String(128), nullable=False, index=True, default="default")
    kind = Column(String(64), nullable=False, index=True)
    secret_encrypted = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class DataSource(Base):
    __tablename__ = "data_sources"

    id = Column(String(36), primary_key=True)
    workspace_id = Column(String(128), nullable=False, index=True, default="default")
    category = Column(String(32), nullable=False, index=True)  # warehouse | ad_platform | cdp
    type = Column(String(64), nullable=False, index=True)  # bigquery | snowflake | meta | meiro ...
    name = Column(String(255), nullable=False)
    status = Column(String(32), nullable=False, default="connected", index=True)  # connected | error | disabled
    config_json = Column(JSON, nullable=False, default={})
    secret_ref = Column(String(36), ForeignKey("secret_store.id", ondelete="SET NULL"), nullable=True)
    last_tested_at = Column(DateTime, nullable=True)
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_data_sources_workspace_category", "workspace_id", "category"),
        Index("ix_data_sources_workspace_type", "workspace_id", "type"),
    )


class OAuthSession(Base):
    __tablename__ = "oauth_sessions"

    id = Column(String(36), primary_key=True)
    workspace_id = Column(String(128), nullable=False, index=True, default="default")
    user_id = Column(String(128), nullable=False, index=True)
    provider_key = Column(String(64), nullable=False, index=True)
    state = Column(String(128), nullable=False, unique=True, index=True)
    pkce_verifier_encrypted = Column(Text, nullable=False)
    return_url = Column(String(1024), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False, index=True)
    consumed_at = Column(DateTime, nullable=True, index=True)

    __table_args__ = (
        Index("ix_oauth_sessions_workspace_provider", "workspace_id", "provider_key"),
    )
