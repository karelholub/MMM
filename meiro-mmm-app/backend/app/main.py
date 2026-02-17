from fastapi import FastAPI, BackgroundTasks, UploadFile, File, Form, HTTPException, Query, Body, Request, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, Response, JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Tuple
import pandas as pd
from pathlib import Path
import io
import json
import os
import re
import time
import uuid
import hashlib
import secrets
import requests
import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from sqlalchemy import func

logger = logging.getLogger(__name__)
from app.utils.token_store import save_token, get_token, delete_token, get_all_connected_platforms
from app.utils.encrypt import encrypt, decrypt
from app.utils import datasource_config as ds_config
from app.utils.taxonomy import load_taxonomy, save_taxonomy, Taxonomy, MatchExpression, ChannelRule
from app.utils.kpi_config import load_kpi_config, save_kpi_config, KpiConfig, KpiDefinition
from app.utils.api_params import clamp_int, resolve_per_page, resolve_sort_dir
from app.db import Base, engine, get_db, SessionLocal
from app.models_config_dq import (
    ModelConfig as ORMModelConfig,
    ModelConfigAudit,
    ModelConfigStatus,
    JourneySettingsVersion as ORMJourneySettingsVersion,
    JourneySettingsStatus,
    AuthSession as ORMAuthSession,
    User as ORMUser,
    Workspace as ORMWorkspace,
    WorkspaceMembership,
    Role as ORMRole,
    Permission as ORMPermission,
    RolePermission as ORMRolePermission,
    Invitation as ORMInvitation,
    SecurityAuditLog as ORMSecurityAuditLog,
    DQSnapshot,
    DQAlertRule,
    DQAlert,
    Experiment,
    ExperimentAssignment,
    ExperimentExposure,
    ExperimentOutcome,
    ExperimentResult,
)
from app.services_model_config import (
    create_draft_config,
    clone_config,
    update_draft_config,
    activate_config,
    archive_config,
    get_default_config_id,
)
from app.services_journey_settings import (
    activate_journey_settings_version,
    archive_journey_settings_version,
    build_journey_settings_impact_preview,
    create_journey_settings_draft,
    ensure_active_journey_settings,
    get_active_journey_settings,
    get_journey_settings_version,
    invalidate_active_journey_settings_cache,
    list_journey_settings_versions,
    update_journey_settings_draft,
    validate_journey_settings,
)
from app.services_access_control import ensure_access_control_seed_data, DEFAULT_WORKSPACE_ID
from app.services_access_control import PERMISSIONS as RBAC_PERMISSIONS
from app.services_auth import (
    CSRF_HEADER_NAME,
    SESSION_COOKIE_NAME,
    authenticate_local_user,
    create_session,
    ensure_local_password_seed_users,
    ensure_user_and_membership,
    issue_csrf_token,
    require_auth_context,
    resolve_auth_context,
    revoke_all_user_sessions,
    revoke_session,
    verify_csrf,
)
from dataclasses import dataclass
from app.services_data_quality import compute_dq_snapshots, evaluate_alert_rules
from app.services_conversions import (
    apply_model_config_to_journeys,
    load_journeys_from_db,
    persist_journeys_as_conversion_paths,
)
from app.services_journey_ingestion import validate_and_normalize
from app.services_import_runs import (
    create_run as create_import_run,
    get_runs as get_import_runs,
    get_run as get_import_run,
    get_last_successful_run,
)
from app.services_journey_definitions import (
    ensure_default_journey_definition,
    list_journey_definitions,
    create_journey_definition,
    update_journey_definition,
    archive_journey_definition,
    serialize_journey_definition,
    get_journey_definition,
)
from app.services_journey_paths import list_paths_for_journey_definition
from app.services_journey_transitions import list_transitions_for_journey_definition
from app.services_journey_aggregates import run_daily_journey_aggregates
from app.services_funnels import (
    create_funnel,
    get_funnel,
    get_funnel_diagnostics as compute_funnel_diagnostics,
    get_funnel_results as compute_funnel_results,
    list_funnels,
)
from app.services_journey_attribution import build_journey_attribution_summary
from app.services_data_sources import (
    create_data_source,
    delete_data_source,
    disable_data_source,
    list_data_sources,
    rotate_data_source_credentials,
    test_data_source_payload,
    test_saved_data_source,
    update_data_source,
)
from app.services_oauth_connections import (
    OAUTH_PROVIDER_LABELS,
    build_authorization_url,
    complete_oauth_callback,
    create_oauth_session,
    disconnect_connection,
    list_oauth_connections,
    list_provider_accounts,
    normalize_provider_key,
    select_accounts,
    test_connection_health,
    get_access_token_for_provider,
)
from app.services_journey_alerts import (
    ALERT_DOMAINS as JOURNEY_ALERT_DOMAINS,
    ALERT_TYPES as JOURNEY_ALERT_TYPES,
    create_alert_definition as create_journey_alert_definition,
    list_alert_definitions as list_journey_alert_definitions,
    list_alert_events as list_journey_alert_events,
    preview_alert as preview_journey_alert,
    update_alert_definition as update_journey_alert_definition,
)
from app.models_overview_alerts import AlertEvent, AlertRule, NotificationChannel, UserNotificationPref
from app.services_alerts import (
    list_alerts,
    get_alert_by_id,
    ack_alert,
    snooze_alert,
    resolve_alert,
    list_alert_rules,
    get_alert_rule_by_id,
    create_alert_rule,
    update_alert_rule,
    delete_alert_rule,
)
from app.services_overview import (
    get_overview_summary,
    get_overview_drivers,
    get_overview_alerts,
)
from app.services_performance_trends import (
    build_channel_trend_response,
    build_campaign_trend_response,
    build_channel_summary_response,
    build_campaign_summary_response,
    resolve_period_windows,
)
from app.services_quality import (
    load_config_and_meta,
    get_latest_quality_for_scope,
    summarize_config_changes,
    compute_overall_quality_from_dq,
)
from app.services_paths import compute_path_archetypes, compute_path_anomalies
from app.services_revenue_config import normalize_revenue_config
from app.services_conversion_paths_adapter import (
    build_conversion_paths_analysis_from_daily,
    build_conversion_path_details_from_daily,
)
from app.services_mmm_platform import build_mmm_dataset_from_platform
from app.services_mmm_mapping import build_smart_suggestions, validate_mapping
from app.services_incrementality import (
    assign_profiles_deterministic,
    record_exposure,
    record_exposures_batch,
    record_outcome,
    record_outcomes_batch,
    compute_experiment_results,
    estimate_sample_size,
    run_nightly_report,
    get_experiment_time_series,
    auto_assign_from_conversion_paths,
    compute_experiment_health,
)
from app.mmm_engine import fit_model as mmm_fit_model, engine_info
from app.connectors import meiro_cdp
from app.utils.meiro_config import (
    append_webhook_event,
    get_last_test_at,
    get_webhook_events,
    get_webhook_last_received_at,
    get_webhook_received_count,
    get_webhook_secret,
    get_mapping,
    save_mapping,
    get_pull_config,
    save_pull_config,
    rotate_webhook_secret,
    set_webhook_received,
)
from app.attribution_engine import (
    run_attribution,
    run_all_models as run_all_attribution,
    run_attribution_campaign,
    compute_channel_performance,
    parse_conversion_paths,
    analyze_paths,
    compute_next_best_action,
    has_any_campaign,
    ATTRIBUTION_MODELS,
    _step_string,
)

# Create DB tables if a real database is configured. This is idempotent and cheap
# for SQLite; in PostgreSQL you should run proper migrations from backend/migrations.
Base.metadata.create_all(bind=engine)

try:
    _seed_db = SessionLocal()
    try:
        ensure_access_control_seed_data(_seed_db)
    finally:
        _seed_db.close()
except Exception as e:
    logger.warning("Access control seed initialization failed: %s", e)

# Ensure alert_events / alert_rules have columns from migrations 005/006 (SQLite doesn't run migrations).
# If the DB predates these, SELECTs would raise OperationalError; we add columns if missing.
def _ensure_alert_tables_columns():
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            for stmt in (
                "ALTER TABLE alert_events ADD COLUMN fingerprint VARCHAR(64)",
                "ALTER TABLE alert_events ADD COLUMN snooze_until TIMESTAMP",
                "ALTER TABLE alert_events ADD COLUMN updated_by VARCHAR(255)",
                "ALTER TABLE alert_rules ADD COLUMN updated_by VARCHAR(255)",
            ):
                try:
                    conn.execute(text(stmt))
                    conn.commit()
                except Exception:
                    conn.rollback()
    except Exception as e:
        logger.debug("Alert table column check skipped (e.g. not SQLite or tables missing): %s", e)


def _ensure_journey_definition_columns():
    """Ensure post-v1 journey_definition columns exist for local SQLite dev DBs."""
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            for stmt in (
                "ALTER TABLE journey_definitions ADD COLUMN updated_by VARCHAR(255)",
                "ALTER TABLE journey_definitions ADD COLUMN is_archived BOOLEAN NOT NULL DEFAULT 0",
                "ALTER TABLE journey_definitions ADD COLUMN archived_at TIMESTAMP",
                "ALTER TABLE journey_definitions ADD COLUMN archived_by VARCHAR(255)",
            ):
                try:
                    conn.execute(text(stmt))
                    conn.commit()
                except Exception:
                    conn.rollback()
    except Exception as e:
        logger.debug("Journey definition column check skipped: %s", e)


def _ensure_journey_paths_columns():
    """Ensure new columns on journey_paths_daily exist for local SQLite dev DBs."""
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            for stmt in (
                "ALTER TABLE journey_paths_daily ADD COLUMN campaign_id VARCHAR(128)",
                "ALTER TABLE journey_transitions_daily ADD COLUMN campaign_id VARCHAR(128)",
                "ALTER TABLE journey_transitions_daily ADD COLUMN country VARCHAR(64)",
            ):
                try:
                    conn.execute(text(stmt))
                    conn.commit()
                except Exception:
                    conn.rollback()
    except Exception as e:
        logger.debug("Journey paths column check skipped: %s", e)


def _ensure_user_auth_columns():
    """Ensure local credential auth columns exist for SQLite/dev databases."""
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            for stmt in (
                "ALTER TABLE users ADD COLUMN username VARCHAR(64)",
                "ALTER TABLE users ADD COLUMN password_hash VARCHAR(255)",
                "ALTER TABLE users ADD COLUMN password_updated_at TIMESTAMP",
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_users_username ON users (username)",
            ):
                try:
                    conn.execute(text(stmt))
                    conn.commit()
                except Exception:
                    conn.rollback()
    except Exception as e:
        logger.debug("User auth column check skipped: %s", e)


if "sqlite" in (os.getenv("DATABASE_URL") or "").lower() or not os.getenv("DATABASE_URL"):
    try:
        _ensure_alert_tables_columns()
        _ensure_journey_definition_columns()
        _ensure_journey_paths_columns()
        _ensure_user_auth_columns()
    except Exception:
        pass


def _is_dev_environment() -> bool:
    """Best-effort dev/local detection for optional seeds."""
    explicit = (os.getenv("APP_ENV") or os.getenv("ENV") or os.getenv("MEIRO_ENV") or os.getenv("ENVIRONMENT") or "").strip().lower()
    if explicit in {"prod", "production"}:
        return False
    for key in ("APP_ENV", "ENV", "MEIRO_ENV", "ENVIRONMENT"):
        val = (os.getenv(key) or "").strip().lower()
        if val in {"dev", "development", "local"}:
            return True
    db_url = (os.getenv("DATABASE_URL") or "").strip().lower()
    if not db_url or "sqlite" in db_url:
        return True
    return os.getenv("DEBUG") == "1"


def _maybe_seed_default_journey_definition() -> None:
    """Create one default journey definition in dev if none exist."""
    if not _is_dev_environment():
        return
    db = SessionLocal()
    try:
        ensure_default_journey_definition(db, created_by="dev-seed")
    except Exception as e:
        logger.debug("Default journey definition seed skipped: %s", e)
    finally:
        db.close()


def _maybe_seed_local_auth_users() -> None:
    """Seed minimal local credential users for dev/demo login."""
    if not _is_dev_environment():
        return
    db = SessionLocal()
    try:
        ensure_local_password_seed_users(db, workspace_id=DEFAULT_WORKSPACE_ID)
    except Exception as e:
        logger.debug("Local auth seed skipped: %s", e)
    finally:
        db.close()


try:
    _maybe_seed_default_journey_definition()
except Exception:
    pass

try:
    _maybe_seed_local_auth_users()
except Exception:
    pass

app = FastAPI(title="Meiro Attribution Dashboard API", version="0.3.0")

SESSION_COOKIE_SECURE = (os.getenv("SESSION_COOKIE_SECURE", "1").strip() != "0")
SESSION_COOKIE_SAMESITE = (os.getenv("SESSION_COOKIE_SAMESITE", "lax").strip().lower() or "lax")
SESSION_COOKIE_MAX_AGE = max(3600, int(os.getenv("SESSION_COOKIE_MAX_AGE_SECONDS", str(60 * 60 * 24 * 7))))
CSRF_EXEMPT_PATHS = {
    "/api/auth/login",
    "/api/auth/status",
}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def csrf_protection_middleware(request: Request, call_next):
    # Enforce CSRF for cookie-authenticated unsafe methods. Legacy header-based
    # callers without session cookies remain unaffected until RBAC rollout.
    if (
        request.method in {"POST", "PUT", "PATCH", "DELETE"}
        and request.url.path.startswith("/api/")
        and request.url.path not in CSRF_EXEMPT_PATHS
        and request.cookies.get(SESSION_COOKIE_NAME)
    ):
        db = SessionLocal()
        try:
            verify_csrf(db, request)
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
        finally:
            db.close()
    return await call_next(request)


@dataclass
class PermissionContext:
    user_id: str
    workspace_id: str
    permissions: set[str]
    source: str  # session | legacy_header


_ALL_PERMISSION_KEYS = {p["key"] for p in RBAC_PERMISSIONS}
_VIEW_PERMISSIONS = {p for p in _ALL_PERMISSION_KEYS if p.endswith(".view")}
_EDITOR_PERMISSIONS = _ALL_PERMISSION_KEYS - {"users.manage", "roles.manage", "audit.view"}
_LEGACY_ROLE_PERMISSION_MAP: Dict[str, set[str]] = {
    "viewer": set(_VIEW_PERMISSIONS),
    "analyst": set(_EDITOR_PERMISSIONS),
    "editor": set(_EDITOR_PERMISSIONS),
    "power_user": set(_ALL_PERMISSION_KEYS),
    "admin": set(_ALL_PERMISSION_KEYS),
}


def get_permission_context(
    request: Request,
    db=Depends(get_db),
    x_user_id: Optional[str] = Header(None, alias="X-User-Id"),
    x_user_role: Optional[str] = Header(None, alias="X-User-Role"),
) -> PermissionContext:
    raw_session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if raw_session_id:
        ctx = resolve_auth_context(db, raw_session_id=raw_session_id)
        if not ctx:
            raise HTTPException(status_code=401, detail={"code": "auth_required", "message": "Authentication required"})
        return PermissionContext(
            user_id=ctx.user.id,
            workspace_id=ctx.workspace.id,
            permissions=set(ctx.permissions),
            source="session",
        )
    # Legacy compatibility: treat headerless callers as viewer so read-only
    # settings/taxonomy pages keep working without explicit auth headers.
    role = (x_user_role or "viewer").strip().lower()
    perms = _LEGACY_ROLE_PERMISSION_MAP.get(role, set())
    return PermissionContext(
        user_id=(x_user_id or "system"),
        workspace_id=DEFAULT_WORKSPACE_ID,
        permissions=set(perms),
        source="legacy_header",
    )


def has_permission(ctx: PermissionContext, permission_key: str) -> bool:
    return permission_key in ctx.permissions


def require_permission(permission_key: str):
    def _dep(ctx: PermissionContext = Depends(get_permission_context)) -> PermissionContext:
        if not has_permission(ctx, permission_key):
            raise HTTPException(
                status_code=403,
                detail={
                    "code": "permission_denied",
                    "message": f"Missing permission: {permission_key}",
                    "permission": permission_key,
                },
            )
        return ctx

    return _dep


def require_any_permission(permission_keys: List[str]):
    keys = tuple(dict.fromkeys(permission_keys))

    def _dep(ctx: PermissionContext = Depends(get_permission_context)) -> PermissionContext:
        for key in keys:
            if key in ctx.permissions:
                return ctx
        raise HTTPException(
            status_code=403,
            detail={
                "code": "forbidden",
                "message": "Missing required permission",
                "permission_any_of": list(keys),
            },
        )

    return _dep


def _workspace_scope_or_403(ctx: PermissionContext, workspace_id: Optional[str]) -> str:
    target = (workspace_id or ctx.workspace_id or DEFAULT_WORKSPACE_ID).strip() or DEFAULT_WORKSPACE_ID
    if ctx.source == "session" and target != ctx.workspace_id:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "workspace_scope_denied",
                "message": "Requested workspace is outside current session scope",
                "workspace_id": target,
            },
        )
    return target

# ==================== Pydantic Models ====================

class ModelConfig(BaseModel):
    dataset_id: str
    frequency: str = "W"
    kpi_mode: str = "conversions"
    kpi: Optional[str] = None
    spend_channels: List[str]
    covariates: Optional[List[str]] = []
    priors: Optional[Dict[str, Any]] = None
    mcmc: Optional[Dict[str, Any]] = None
    use_adstock: bool = True
    use_saturation: bool = True
    holdout_weeks: Optional[int] = 8
    random_seed: Optional[int] = None
    channel_display_names: Optional[Dict[str, str]] = None  # channel id -> human-readable name for MMM outputs

    def __init__(self, **data):
        if 'priors' not in data or data['priors'] is None:
            data['priors'] = {
                "adstock": {"alpha_mean": 0.5, "alpha_sd": 0.2},
                "saturation": {"lam_mean": 0.001, "lam_sd": 0.0005}
            }
        if 'mcmc' not in data or data['mcmc'] is None:
            data['mcmc'] = {"draws": 1000, "tune": 1000, "chains": 4, "target_accept": 0.9}
        if 'kpi' not in data or data['kpi'] is None:
            if 'kpi_mode' in data:
                data['kpi'] = {'conversions': 'conversions', 'aov': 'aov', 'profit': 'profit'}.get(data['kpi_mode'], 'conversions')
        super().__init__(**data)

class ChannelConstraint(BaseModel):
    """Optional per-channel constraints for automatic optimization.

    The optimizer works with *relative multipliers* around the current allocation,
    not absolute currency units. A value of 1.0 means "keep at baseline", 2.0
    means "double relative weight", etc.
    """

    min: float | None = None
    max: float | None = None
    locked: bool = False


class OptimizeRequest(BaseModel):
    """Global + per-channel configuration for automatic budget optimization.

    total_budget:
        Interpreted as the *average* multiplier across channels. With N channels,
        the optimizer enforces sum(x_i) = total_budget * N, where x_i are the
        per-channel multipliers around the current mix.
        A value of 1.0 keeps the overall level at baseline.

    min_spend / max_spend:
        Global lower/upper bounds for each channel's multiplier, used as
        defaults when no per-channel constraint is provided.

    channel_constraints:
        Optional fine-grained constraints per channel. For a given channel:
        - if locked=True, the multiplier is fixed at 1.0 (baseline)
        - otherwise, min/max override the global bounds if provided.
    """

    total_budget: float = 1.0
    min_spend: float = 0.5
    max_spend: float = 2.0
    channel_constraints: Dict[str, ChannelConstraint] | None = None

class MeiroCDPConnectRequest(BaseModel):
    api_base_url: str
    api_key: str

class MeiroCDPTestRequest(BaseModel):
    api_base_url: Optional[str] = None
    api_key: Optional[str] = None
    save_on_success: bool = False

class MeiroCDPExportRequest(BaseModel):
    since: str
    until: str
    event_types: Optional[List[str]] = None
    attributes: Optional[List[str]] = None
    segment_id: Optional[str] = None

class ExpenseEntry(BaseModel):
    # Core classification
    channel: str
    cost_type: str = "Media Spend"  # Media Spend, Platform Fees, etc.

    # Amounts & currencies
    amount: float  # original amount
    currency: str = "USD"  # original currency code
    reporting_currency: str = "USD"
    converted_amount: Optional[float] = None
    fx_rate: Optional[float] = None
    fx_date: Optional[str] = None  # label only, e.g. "2024-01-15"

    # Periods
    period: Optional[str] = None  # legacy field kept for compatibility (YYYY-MM)
    service_period_start: Optional[str] = None  # ISO date
    service_period_end: Optional[str] = None  # ISO date
    entry_date: Optional[str] = None  # invoice/entry date (ISO date)

    # References & notes
    invoice_ref: Optional[str] = None
    external_link: Optional[str] = None
    notes: Optional[str] = None

    # Provenance & status
    source_type: str = "manual"  # manual | import
    source_name: Optional[str] = None  # e.g. "google_ads", "meta_ads"
    status: str = "active"  # active | deleted

    # Audit trail (shallow, no user system)
    created_at: Optional[str] = None  # ISO timestamp
    updated_at: Optional[str] = None  # ISO timestamp
    deleted_at: Optional[str] = None  # ISO timestamp
    actor_type: str = "manual"  # manual | system | import
    change_note: Optional[str] = None


class ExpenseChangeEvent(BaseModel):
    expense_id: str
    timestamp: str
    event_type: str  # created | updated | deleted | restored
    actor_type: str  # manual | system | import
    note: Optional[str] = None

class AttributionMappingConfig(BaseModel):
    touchpoint_attr: str = "touchpoints"
    value_attr: str = "conversion_value"
    id_attr: str = "customer_id"
    channel_field: str = "channel"
    timestamp_field: str = "timestamp"


class FromCDPRequest(BaseModel):
    mapping: Optional[AttributionMappingConfig] = None
    config_id: Optional[str] = None
    import_note: Optional[str] = None


class BuildFromPlatformRequest(BaseModel):
    """Request to build an MMM weekly dataset from platform journeys and expenses."""
    date_start: str  # ISO date
    date_end: str    # ISO date
    kpi_target: str = "sales"  # sales | attribution (marketing-driven conversions)
    spend_channels: List[str]  # e.g. ["google_ads", "meta_ads", "linkedin_ads"]
    covariates: Optional[List[str]] = None
    currency: str = "USD"
    attribution_model: Optional[str] = None  # e.g. "linear"; when kpi_target=attribution, which model was used
    attribution_config_id: Optional[str] = None  # optional model config id


class ValidateMappingRequest(BaseModel):
    """Request to validate an MMM column mapping before running the model."""
    date_column: str
    kpi: str
    spend_channels: List[str]
    covariates: Optional[List[str]] = None
    kpi_target: Optional[str] = None  # sales | attribution; used for suggestion context only

# ==================== In-memory State ====================

RUNS: Dict[str, Any] = {}
DATASETS: Dict[str, Dict[str, Any]] = {}
EXPENSES: Dict[str, ExpenseEntry] = {}  # key: arbitrary unique id
EXPENSE_AUDIT_LOG: List[ExpenseChangeEvent] = []

# Import health & reconciliation (per-source sync state)
IMPORT_SYNC_STATE: Dict[str, Dict[str, Any]] = {}  # source -> { last_success_at, last_attempt_at, status, last_error, action_hint, records_imported, platform_total, period_start, period_end }
SYNC_IN_PROGRESS: set = set()  # source names currently syncing

JOURNEYS: List[Dict] = []  # Current loaded journeys
ATTRIBUTION_RESULTS: Dict[str, Any] = {}  # Cached attribution results
# Lightweight in-memory cache for path archetypes, keyed by view state.
PATH_ARCHETYPES_CACHE: Dict[tuple, Dict[str, Any]] = {}

SAMPLE_DIR = Path(__file__).parent / "sample_data"
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MMM_PLATFORM_DIR = DATA_DIR / "mmm_platform"
MMM_PLATFORM_DIR.mkdir(parents=True, exist_ok=True)
RUNS_FILE = DATA_DIR / "mmm_runs.json"
IMPORT_RUNS_FILE = DATA_DIR / "import_runs.json"
LAST_IMPORT_RESULT_FILE = DATA_DIR / "last_import_result.json"


def _load_runs():
    """Load run registry from disk (idempotent)."""
    global RUNS
    if RUNS_FILE.exists():
        try:
            raw = RUNS_FILE.read_text(encoding="utf-8")
            RUNS.update(json.loads(raw))
        except Exception:
            pass


def _save_runs():
    """Persist run registry. Only serializable fields (no Path, etc.)."""
    try:
        out = {}
        for rid, r in RUNS.items():
            out[rid] = {k: v for k, v in r.items() if k in ("status", "config", "kpi_mode", "created_at", "updated_at", "dataset_id", "r2", "contrib", "roi", "engine", "detail", "uplift", "campaigns", "channel_summary", "adstock_params", "saturation_params", "diagnostics", "attribution_model", "attribution_config_id")}
        RUNS_FILE.parent.mkdir(parents=True, exist_ok=True)
        RUNS_FILE.write_text(json.dumps(out, indent=0), encoding="utf-8")
    except Exception:
        pass


def _refresh_journey_aggregates_after_import(db, *, reprocess_days: Optional[int] = None) -> None:
    """Best-effort aggregate refresh so Journeys UI has path/transition data right after import."""
    try:
        active = get_active_journey_settings(db, use_cache=True)
        default_reprocess = (
            ((active.get("settings_json") or {}).get("performance_guardrails") or {}).get(
                "aggregation_reprocess_window_days",
                3,
            )
        )
        effective_reprocess = max(1, int(reprocess_days or default_reprocess or 3))
        metrics = run_daily_journey_aggregates(db, reprocess_days=effective_reprocess)
        logger.info(
            "Post-import journey aggregates refreshed: definitions=%s days_processed=%s source_rows=%s reprocess_days=%s",
            metrics.get("definitions", 0),
            metrics.get("days_processed", 0),
            metrics.get("source_rows_processed", 0),
            effective_reprocess,
        )
    except Exception as e:
        logger.warning("Post-import journey aggregates refresh failed: %s", e, exc_info=True)


_load_runs()


def _append_import_run(
    source: str,
    count: int,
    status: str = "success",
    error: Optional[str] = None,
    total: int = 0,
    valid: int = 0,
    invalid: int = 0,
    converted: int = 0,
    channels_detected: Optional[List[str]] = None,
    validation_summary: Optional[Dict[str, Any]] = None,
    config_snapshot: Optional[Dict[str, Any]] = None,
    preview_rows: Optional[List[Dict[str, Any]]] = None,
    initiated_by: Optional[str] = None,
    import_note: Optional[str] = None,
) -> None:
    """Append an import run. Delegates to services_import_runs with full schema."""
    create_import_run(
        source=source,
        status=status,
        total=total if total is not None and total > 0 else count,
        valid=valid if valid is not None and valid >= 0 else (count if status == "success" else 0),
        invalid=invalid,
        converted=converted,
        channels_detected=channels_detected,
        validation_summary=validation_summary,
        config_snapshot=config_snapshot,
        error=error,
        preview_rows=preview_rows,
        initiated_by=initiated_by,
        import_note=import_note,
    )


def _save_last_import_result(result: Dict[str, Any]) -> None:
    """Persist last import result for preview drawer and validation report."""
    try:
        # Serialize for JSON (convert datetime etc.)
        out = {
            "import_summary": result.get("import_summary", {}),
            "validation_items": result.get("validation_items", []),
            "items_detail": result.get("items_detail", []),
        }
        LAST_IMPORT_RESULT_FILE.parent.mkdir(parents=True, exist_ok=True)
        LAST_IMPORT_RESULT_FILE.write_text(json.dumps(out, indent=0, default=str), encoding="utf-8")
    except Exception:
        pass


def _load_last_import_result() -> Dict[str, Any]:
    """Load last import result for UI."""
    if not LAST_IMPORT_RESULT_FILE.exists():
        return {"import_summary": {}, "validation_items": [], "items_detail": []}
    try:
        return json.loads(LAST_IMPORT_RESULT_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"import_summary": {}, "validation_items": [], "items_detail": []}


# ==================== Settings ====================


class AttributionSettings(BaseModel):
    """Configurable knobs for attribution models and path analysis."""

    lookback_window_days: int = 30
    use_converted_flag: bool = True
    min_conversion_value: float = 0.0

    time_decay_half_life_days: float = 7.0
    position_first_pct: float = 0.4
    position_last_pct: float = 0.4
    markov_min_paths: int = 5


class MMMSettings(BaseModel):
    """High-level MMM configuration (used as defaults when starting new runs)."""

    frequency: str = "W"  # "W" or "M"


class NBASettings(BaseModel):
    """Next-best-action configuration."""

    min_prefix_support: int = 5
    min_conversion_rate: float = 0.01  # 1%
    max_prefix_depth: int = 5
    min_next_support: int = 5
    max_suggestions_per_prefix: int = 3
    min_uplift_pct: Optional[float] = None  # expressed as decimal (e.g., 0.1 = 10%)
    excluded_channels: List[str] = Field(default_factory=lambda: ["direct"])


class FeatureFlags(BaseModel):
    """Workspace feature switches."""

    journeys_enabled: bool = False
    journey_examples_enabled: bool = False
    funnel_builder_enabled: bool = False
    funnel_diagnostics_enabled: bool = False
    access_control_enabled: bool = False
    custom_roles_enabled: bool = False
    audit_log_enabled: bool = False
    scim_enabled: bool = False
    sso_enabled: bool = False


class RevenueConfig(BaseModel):
    conversion_names: List[str] = Field(default_factory=lambda: ["purchase"])
    value_field_path: str = "value"
    currency_field_path: str = "currency"
    dedup_key: str = "conversion_id"
    base_currency: str = "EUR"
    fx_enabled: bool = False
    fx_mode: str = "none"
    fx_rates_json: Dict[str, float] = Field(default_factory=dict)
    source_type: str = "conversion_event"


class Settings(BaseModel):
    attribution: AttributionSettings = AttributionSettings()
    mmm: MMMSettings = MMMSettings()
    nba: NBASettings = NBASettings()
    feature_flags: FeatureFlags = FeatureFlags()
    revenue_config: RevenueConfig = RevenueConfig()


class AttributionPreviewPayload(BaseModel):
    settings: AttributionSettings


class AttributionPreviewResponse(BaseModel):
    previewAvailable: bool
    totalJourneys: int
    windowImpactCount: int
    windowDirection: str
    useConvertedFlagImpact: int
    useConvertedFlagDirection: str
    reason: Optional[str] = None


class NBAPreviewPayload(BaseModel):
    settings: NBASettings
    level: Optional[str] = "channel"


class NBAPreviewResponse(BaseModel):
    previewAvailable: bool
    datasetJourneys: int
    totalPrefixes: int
    prefixesEligible: int
    totalRecommendations: int
    averageRecommendationsPerPrefix: float
    filteredBySupportPct: float
    filteredByConversionPct: float
    reason: Optional[str] = None


class NBATestPayload(BaseModel):
    settings: NBASettings
    path_prefix: str
    level: Optional[str] = "channel"


class NBATestRecommendation(BaseModel):
    step: str
    channel: str
    campaign: Optional[str] = None
    count: int
    conversions: int
    conversion_rate: float
    avg_value: float
    avg_value_converted: float
    uplift_pct: Optional[float] = None


class NBATestResponse(BaseModel):
    previewAvailable: bool
    prefix: str
    level: str
    totalPrefixSupport: int
    baselineConversionRate: float
    recommendations: List[NBATestRecommendation]
    reason: Optional[str] = None


SETTINGS_PATH = DATA_DIR / "settings.json"


def _load_settings() -> Settings:
    if SETTINGS_PATH.exists():
        try:
            data = json.loads(SETTINGS_PATH.read_text())
            return Settings(**data)
        except Exception:
            # Fallback to defaults if file is corrupted
            pass
    settings = Settings()
    SETTINGS_PATH.write_text(settings.model_dump_json(indent=2))
    return settings


SETTINGS: Settings = _load_settings()


def _save_settings() -> None:
    SETTINGS_PATH.write_text(SETTINGS.model_dump_json(indent=2))


def _filter_nba_recommendations(
    nba_raw: Dict[str, List[Dict[str, Any]]],
    settings: NBASettings,
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    """Apply NBA settings thresholds to raw recommendations and collect stats."""

    stats: Dict[str, Any] = {
        "total_before": 0,
        "total_after": 0,
        "filtered_support": 0,
        "filtered_conversion": 0,
        "filtered_uplift": 0,
        "filtered_excluded": 0,
        "filtered_depth": 0,
        "trimmed_cap": 0,
        "prefixes_considered": 0,
        "prefixes_retained": 0,
    }

    min_prefix_support = max(1, settings.min_prefix_support)
    min_conversion_rate = max(0.0, settings.min_conversion_rate)
    max_prefix_depth = max(0, settings.max_prefix_depth)
    min_next_support = max(1, settings.min_next_support or settings.min_prefix_support)
    max_suggestions = max(1, settings.max_suggestions_per_prefix)
    min_uplift_pct = settings.min_uplift_pct
    excluded_channels = {
        ch.strip().lower() for ch in (settings.excluded_channels or []) if ch
    }

    filtered: Dict[str, List[Dict[str, Any]]] = {}

    for prefix, recs in nba_raw.items():
        stats["prefixes_considered"] += 1
        stats["total_before"] += len(recs)

        depth = len([step for step in prefix.split(" > ") if step])
        if depth > max_prefix_depth:
            stats["filtered_depth"] += len(recs)
            continue

        prefix_support = sum(int(r.get("count", 0)) for r in recs)
        if prefix_support < min_prefix_support:
            stats["filtered_support"] += len(recs)
            continue

        total_conversions = sum(int(r.get("conversions", 0)) for r in recs)
        baseline_rate = (
            total_conversions / prefix_support if prefix_support > 0 else 0.0
        )

        kept: List[Dict[str, Any]] = []
        for rec in recs:
            count = int(rec.get("count", 0))
            conv_rate = float(rec.get("conversion_rate", 0.0))
            channel = str(rec.get("channel", "")).lower()

            if count < min_next_support:
                stats["filtered_support"] += 1
                continue
            if conv_rate < min_conversion_rate:
                stats["filtered_conversion"] += 1
                continue
            if excluded_channels and channel in excluded_channels:
                stats["filtered_excluded"] += 1
                continue
            if (
                min_uplift_pct is not None
                and min_uplift_pct > 0
                and baseline_rate > 0
            ):
                uplift = (conv_rate - baseline_rate) / baseline_rate
                if uplift < min_uplift_pct:
                    stats["filtered_uplift"] += 1
                    continue

            kept.append(rec)

        if kept:
            if len(kept) > max_suggestions:
                stats["trimmed_cap"] += len(kept) - max_suggestions
                kept = kept[:max_suggestions]
            filtered[prefix] = kept
            stats["prefixes_retained"] += 1
            stats["total_after"] += len(kept)

    return filtered, stats


# KPI configuration (separate JSON file)
KPI_CONFIG: KpiConfig = load_kpi_config()


def compute_campaign_uplift(journeys: List[Dict]) -> Dict[str, Dict[str, Any]]:
    """
    For each campaign step (channel:campaign or channel), estimate uplift by comparing
    journeys that include the campaign vs. journeys that do not.

    Uplift is purely observational here (not a causal experiment):
      - treatment group: journeys that touched the campaign at least once
      - holdout group: journeys that never touched the campaign
    """
    if not journeys:
        return {}

    total_n = len(journeys)
    total_conv = 0

    treat_stats: Dict[str, Dict[str, Any]] = {}

    for j in journeys:
        converted = j.get("converted", True)
        if converted:
            total_conv += 1
        # Build set of campaign steps in this journey
        steps = set()
        for tp in j.get("touchpoints", []):
            channel = tp.get("channel", "unknown")
            campaign = tp.get("campaign")
            step = f"{channel}:{campaign}" if campaign else channel
            steps.add(step)
        for step in steps:
            st = treat_stats.setdefault(step, {"n": 0, "conv": 0})
            st["n"] += 1
            if converted:
                st["conv"] += 1

    uplift: Dict[str, Dict[str, Any]] = {}
    for step, st in treat_stats.items():
        treat_n = st["n"]
        treat_conv = st["conv"]
        control_n = total_n - treat_n
        control_conv = total_conv - treat_conv

        treat_rate = treat_conv / treat_n if treat_n > 0 else 0.0
        control_rate = control_conv / control_n if control_n > 0 else 0.0
        abs_uplift = treat_rate - control_rate
        rel_uplift = abs_uplift / control_rate if control_rate > 0 else None

        uplift[step] = {
            "treatment_n": treat_n,
            "treatment_conversions": treat_conv,
            "treatment_rate": treat_rate,
            "holdout_n": control_n,
            "holdout_conversions": control_conv,
            "holdout_rate": control_rate,
            "uplift_abs": abs_uplift,
            "uplift_rel": rel_uplift,
        }

    return uplift


def compute_campaign_trends(journeys: List[Dict]) -> Dict[str, Any]:
    """
    Build simple time series per campaign step (channel:campaign or channel):
      - transactions: number of converted journeys attributed to that campaign (last touch)
      - revenue: sum of conversion_value for those journeys

    Conversion date is taken as the timestamp of the last touchpoint.
    """
    if not journeys:
        return {"campaigns": [], "dates": [], "series": {}}

    series: Dict[str, Dict[str, Dict[str, float]]] = {}
    all_dates: set[str] = set()

    for j in journeys:
        if not j.get("converted", True):
            continue
        tps = j.get("touchpoints", [])
        if not tps:
            continue
        last_tp = tps[-1]
        ts = last_tp.get("timestamp")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts)
            date_key = dt.date().isoformat()
        except Exception:
            date_key = str(ts)

        channel = last_tp.get("channel", "unknown")
        campaign = last_tp.get("campaign")
        step = f"{channel}:{campaign}" if campaign else channel

        all_dates.add(date_key)
        step_series = series.setdefault(step, {})
        entry = step_series.setdefault(date_key, {"transactions": 0.0, "revenue": 0.0})
        entry["transactions"] += 1.0
        entry["revenue"] += float(j.get("conversion_value", 0.0) or 0.0)

    sorted_dates = sorted(all_dates)
    campaigns = sorted(series.keys())

    out_series: Dict[str, List[Dict[str, Any]]] = {}
    for step, date_map in series.items():
        points = []
        for d in sorted_dates:
            val = date_map.get(d, {"transactions": 0.0, "revenue": 0.0})
            points.append({"date": d, "transactions": int(val["transactions"]), "revenue": val["revenue"]})
        out_series[step] = points

    return {"campaigns": campaigns, "dates": sorted_dates, "series": out_series}


# ==================== Model Config Versioning API ====================


class ModelConfigPayload(BaseModel):
    """Payload for creating/updating a versioned attribution ModelConfig."""

    name: str
    config_json: Dict[str, Any]
    change_note: Optional[str] = None
    created_by: str = "system"


class ModelConfigUpdatePayload(BaseModel):
    config_json: Dict[str, Any]
    change_note: Optional[str] = None
    actor: str = "system"


class ModelConfigActivatePayload(BaseModel):
    actor: str = "system"
    set_as_default: bool = True
    activation_note: Optional[str] = None


class ModelConfigValidatePayload(BaseModel):
    config_json: Optional[Dict[str, Any]] = None


class ModelConfigPreviewPayload(BaseModel):
    config_json: Optional[Dict[str, Any]] = None


class JourneySettingsVersionCreatePayload(BaseModel):
    version_label: Optional[str] = None
    description: Optional[str] = None
    settings_json: Optional[Dict[str, Any]] = None
    created_by: str = "system"


class JourneySettingsVersionUpdatePayload(BaseModel):
    settings_json: Dict[str, Any]
    description: Optional[str] = None
    actor: str = "system"


class JourneySettingsValidatePayload(BaseModel):
    settings_json: Optional[Dict[str, Any]] = None
    version_id: Optional[str] = None


class JourneySettingsPreviewPayload(BaseModel):
    settings_json: Optional[Dict[str, Any]] = None
    version_id: Optional[str] = None


class JourneySettingsActivatePayload(BaseModel):
    version_id: str
    actor: str = "system"
    activation_note: Optional[str] = None
    confirm: bool = False


def _serialize_journey_settings_version(item: ORMJourneySettingsVersion) -> Dict[str, Any]:
    return {
        "id": item.id,
        "status": item.status,
        "version_label": item.version_label,
        "description": item.description,
        "created_at": item.created_at,
        "updated_at": item.updated_at,
        "created_by": item.created_by,
        "activated_at": item.activated_at,
        "activated_by": item.activated_by,
        "settings_json": item.settings_json,
        "validation_json": item.validation_json,
        "diff_json": item.diff_json,
    }


@app.get("/api/model-configs")
def list_model_configs(db=Depends(get_db)):
    """List all model configs (draft/active/archived)."""
    rows = (
        db.query(ORMModelConfig)
        .order_by(ORMModelConfig.name.asc(), ORMModelConfig.version.desc())
        .all()
    )
    return [
        {
            "id": r.id,
            "name": r.name,
            "status": r.status,
            "version": r.version,
            "parent_id": r.parent_id,
            "created_at": r.created_at,
            "updated_at": r.updated_at,
            "created_by": r.created_by,
            "change_note": r.change_note,
            "activated_at": r.activated_at,
        }
        for r in rows
    ]


@app.post("/api/model-configs")
def create_model_config(payload: ModelConfigPayload, db=Depends(get_db)):
    """Create a new draft ModelConfig."""
    cfg = create_draft_config(
        db=db,
        name=payload.name,
        config_json=payload.config_json,
        created_by=payload.created_by,
        change_note=payload.change_note,
    )
    return {"id": cfg.id, "status": cfg.status, "version": cfg.version}


@app.post("/api/model-configs/{cfg_id}/clone")
def clone_model_config(cfg_id: str, actor: str = "system", db=Depends(get_db)):
    """Clone an existing ModelConfig into a new draft."""
    try:
        cfg = clone_config(db=db, source_id=cfg_id, actor=actor)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"id": cfg.id, "status": cfg.status, "version": cfg.version, "parent_id": cfg.parent_id}


@app.patch("/api/model-configs/{cfg_id}")
def edit_model_config(cfg_id: str, payload: ModelConfigUpdatePayload, db=Depends(get_db)):
    """Edit a draft ModelConfig."""
    try:
        cfg = update_draft_config(
            db=db,
            cfg_id=cfg_id,
            new_config_json=payload.config_json,
            actor=payload.actor,
            change_note=payload.change_note,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"id": cfg.id, "status": cfg.status, "version": cfg.version}


@app.post("/api/model-configs/{cfg_id}/activate")
def activate_model_config(cfg_id: str, payload: ModelConfigActivatePayload, db=Depends(get_db)):
    """Activate a ModelConfig and (optionally) set as default for reports."""
    try:
        cfg = activate_config(
            db=db,
            cfg_id=cfg_id,
            actor=payload.actor,
            set_as_default=payload.set_as_default,
            activation_note=payload.activation_note,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"id": cfg.id, "status": cfg.status, "version": cfg.version, "activated_at": cfg.activated_at}


@app.post("/api/model-configs/{cfg_id}/archive")
def archive_model_config(cfg_id: str, actor: str = "system", db=Depends(get_db)):
    """Archive a ModelConfig (cannot be activated again)."""
    try:
        cfg = archive_config(db=db, cfg_id=cfg_id, actor=actor)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"id": cfg.id, "status": cfg.status, "version": cfg.version}


def _compute_journey_metrics(journeys: List[Dict[str, Any]]) -> Dict[str, Any]:
    touchpoints = sum(len(j.get("touchpoints") or []) for j in journeys)
    conversions = sum(1 for j in journeys if j.get("converted", True))
    return {
        "journeys": len(journeys),
        "touchpoints": touchpoints,
        "conversions": conversions,
    }


def _changed_top_level_keys(
    old_cfg: Optional[Dict[str, Any]],
    new_cfg: Dict[str, Any],
) -> List[str]:
    old_map = old_cfg if isinstance(old_cfg, dict) else {}
    keys = set(old_map.keys()) | set(new_cfg.keys())
    changed: List[str] = []
    for key in sorted(keys):
        if old_map.get(key) != new_cfg.get(key):
            changed.append(key)
    return changed


@app.post("/api/model-configs/{cfg_id}/validate")
def validate_model_config_route(
    cfg_id: str,
    payload: ModelConfigValidatePayload,
    db=Depends(get_db),
):
    cfg = db.get(ORMModelConfig, cfg_id)
    if not cfg:
        raise HTTPException(status_code=404, detail="Config not found")

    cfg_json: Any = payload.config_json or cfg.config_json or {}
    schema_errors: List[str] = []
    errors: List[str] = []
    warnings: List[str] = []
    missing_conversions: List[str] = []

    if not isinstance(cfg_json, dict):
        schema_errors.append("Config JSON must be a top-level object.")
    else:
        ok, msg = validate_model_config(cfg_json)
        if not ok:
            errors.append(msg)

        conv_section = cfg_json.get("conversions") or {}
        conversion_defs = conv_section.get("conversion_definitions") or []
        available_keys = {d.id for d in KPI_CONFIG.definitions}
        missing_conversions = sorted(
            {
                str(defn.get("key"))
                for defn in conversion_defs
                if defn.get("key") and defn.get("key") not in available_keys
            }
        )
        if missing_conversions:
            errors.append(
                "Conversion definitions reference unknown KPI keys: "
                + ", ".join(missing_conversions)
            )

        required_sections = ["eligible_touchpoints", "windows", "conversions"]
        for section in required_sections:
            if section not in cfg_json:
                errors.append(f"Missing top-level section '{section}'")

        touchpoints = cfg_json.get("eligible_touchpoints") or {}
        for field in [
            "include_channels",
            "exclude_channels",
            "include_event_types",
            "exclude_event_types",
        ]:
            value = touchpoints.get(field)
            if value is not None and not isinstance(value, list):
                errors.append(f"'{field}' must be an array of strings")

    valid = not errors and not schema_errors
    return {
        "valid": valid,
        "errors": errors,
        "warnings": warnings,
        "missing_conversions": missing_conversions,
        "schema_errors": schema_errors,
    }


@app.post("/api/model-configs/{cfg_id}/preview")
def preview_model_config(
    cfg_id: str,
    payload: ModelConfigPreviewPayload,
    db=Depends(get_db),
):
    cfg = db.get(ORMModelConfig, cfg_id)
    if not cfg:
        raise HTTPException(status_code=404, detail="Config not found")

    cfg_json: Any = payload.config_json or cfg.config_json or {}
    if not isinstance(cfg_json, dict):
        return {"preview_available": False, "reason": "Config JSON must be an object"}

    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        return {"preview_available": False, "reason": "No journeys loaded"}

    baseline_cfg: Optional[ORMModelConfig] = (
        db.query(ORMModelConfig)
        .filter(
            ORMModelConfig.name == cfg.name,
            ORMModelConfig.status == ModelConfigStatus.ACTIVE,
        )
        .order_by(ORMModelConfig.version.desc())
        .first()
    )
    baseline_json = baseline_cfg.config_json if baseline_cfg else {}

    baseline_journeys = apply_model_config_to_journeys(
        JOURNEYS,
        baseline_json or {},
    )
    draft_journeys = apply_model_config_to_journeys(JOURNEYS, cfg_json)

    baseline_metrics = _compute_journey_metrics(baseline_journeys)
    draft_metrics = _compute_journey_metrics(draft_journeys)

    deltas: Dict[str, float] = {
        key: float(draft_metrics.get(key, 0) - baseline_metrics.get(key, 0))
        for key in draft_metrics.keys()
    }
    deltas_pct: Dict[str, Optional[float]] = {}
    for key, delta in deltas.items():
        baseline_value = baseline_metrics.get(key, 0)
        deltas_pct[key] = (
            round((delta / baseline_value) * 100.0, 2) if baseline_value else None
        )

    warnings: List[str] = []
    coverage_warning = False
    baseline_conversions = baseline_metrics.get("conversions", 0)
    draft_conversions = draft_metrics.get("conversions", 0)
    if baseline_conversions and draft_conversions / baseline_conversions < 0.9:
        coverage_warning = True
        warnings.append(
            "Projected attributable conversions decrease by more than 10% versus the active config."
        )

    changed_keys = _changed_top_level_keys(baseline_json or {}, cfg_json)

    return {
        "preview_available": True,
        "baseline": baseline_metrics,
        "draft": draft_metrics,
        "deltas": deltas,
        "deltas_pct": deltas_pct,
        "warnings": warnings,
        "coverage_warning": coverage_warning,
        "changed_keys": changed_keys,
        "active_config_id": baseline_cfg.id if baseline_cfg else None,
        "active_version": baseline_cfg.version if baseline_cfg else None,
        "reason": None,
    }


@app.get("/api/model-configs/{cfg_id}")
def get_model_config_detail(cfg_id: str, db=Depends(get_db)):
    cfg = db.get(ORMModelConfig, cfg_id)
    if not cfg:
        raise HTTPException(status_code=404, detail="Config not found")
    return {
        "id": cfg.id,
        "name": cfg.name,
        "status": cfg.status,
        "version": cfg.version,
        "parent_id": cfg.parent_id,
        "created_at": cfg.created_at,
        "updated_at": cfg.updated_at,
        "created_by": cfg.created_by,
        "change_note": cfg.change_note,
        "activated_at": cfg.activated_at,
        "config_json": cfg.config_json,
    }


@app.get("/api/model-configs/{cfg_id}/audit")
def get_model_config_audit(cfg_id: str, db=Depends(get_db)):
    cfg = db.get(ORMModelConfig, cfg_id)
    if not cfg:
        raise HTTPException(status_code=404, detail="Config not found")
    audits = (
        db.query(ModelConfigAudit)
        .filter(ModelConfigAudit.model_config_id == cfg_id)
        .order_by(ModelConfigAudit.created_at.desc())
        .all()
    )
    return [
        {
            "id": a.id,
            "actor": a.actor,
            "action": a.action,
            "diff_json": a.diff_json,
            "created_at": a.created_at,
        }
        for a in audits
    ]


@app.get("/api/settings/journeys/versions")
def list_journeys_settings_versions_route(
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.view")),
):
    rows = list_journey_settings_versions(db)
    return [_serialize_journey_settings_version(r) for r in rows]


@app.post("/api/settings/journeys/versions")
def create_journeys_settings_version_route(
    payload: JourneySettingsVersionCreatePayload,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    item = create_journey_settings_draft(
        db,
        created_by=payload.created_by,
        version_label=payload.version_label,
        description=payload.description,
        settings_json=payload.settings_json,
    )
    return _serialize_journey_settings_version(item)


@app.get("/api/settings/journeys/versions/{version_id}")
def get_journeys_settings_version_route(
    version_id: str,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.view")),
):
    item = get_journey_settings_version(db, version_id)
    if not item:
        raise HTTPException(status_code=404, detail="Journey settings version not found")
    return _serialize_journey_settings_version(item)


@app.patch("/api/settings/journeys/versions/{version_id}")
def update_journeys_settings_version_route(
    version_id: str,
    payload: JourneySettingsVersionUpdatePayload,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    try:
        item = update_journey_settings_draft(
            db,
            version_id=version_id,
            actor=payload.actor,
            settings_json=payload.settings_json,
            description=payload.description,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _serialize_journey_settings_version(item)


@app.post("/api/settings/journeys/versions/{version_id}/archive")
def archive_journeys_settings_version_route(
    version_id: str,
    actor: str = "system",
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    try:
        item = archive_journey_settings_version(db, version_id=version_id, actor=actor)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _serialize_journey_settings_version(item)


@app.get("/api/settings/journeys/active")
def get_active_journeys_settings_route(
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.view")),
):
    active = ensure_active_journey_settings(db, actor="system")
    return _serialize_journey_settings_version(active)


@app.post("/api/settings/journeys/validate")
def validate_journeys_settings_route(
    payload: JourneySettingsValidatePayload,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.view")),
):
    if payload.version_id:
        item = get_journey_settings_version(db, payload.version_id)
        if not item:
            raise HTTPException(status_code=404, detail="Journey settings version not found")
        target = item.settings_json or {}
    else:
        target = payload.settings_json or {}
    result = validate_journey_settings(target)
    return {
        "valid": result["valid"],
        "errors": result["errors"],
        "warnings": result["warnings"],
    }


@app.post("/api/settings/journeys/preview")
def preview_journeys_settings_route(
    payload: JourneySettingsPreviewPayload,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.view")),
):
    target: Dict[str, Any]
    if payload.version_id:
        item = get_journey_settings_version(db, payload.version_id)
        if not item:
            raise HTTPException(status_code=404, detail="Journey settings version not found")
        target = item.settings_json or {}
    else:
        target = payload.settings_json or {}
    return build_journey_settings_impact_preview(db, draft_settings_json=target)


@app.post("/api/settings/journeys/activate")
def activate_journeys_settings_route(
    payload: JourneySettingsActivatePayload,
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
    db=Depends(get_db),
):
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="Activation requires explicit confirm=true")
    item = get_journey_settings_version(db, payload.version_id)
    if not item:
        raise HTTPException(status_code=404, detail="Journey settings version not found")
    preview = build_journey_settings_impact_preview(
        db,
        draft_settings_json=item.settings_json or {},
    )
    if not preview.get("preview_available"):
        raise HTTPException(status_code=400, detail="Impact preview unavailable; resolve validation before activation")
    try:
        item = activate_journey_settings_version(
            db,
            version_id=payload.version_id,
            actor=payload.actor,
            activation_note=payload.activation_note,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {
        **_serialize_journey_settings_version(item),
        "impact_preview": preview,
    }

# Initialize sample datasets
DATASETS["sample-weekly-01"] = {"path": SAMPLE_DIR / "sample-weekly-01.csv", "type": "sales"}
DATASETS["sample-weekly-realistic"] = {"path": SAMPLE_DIR / "sample-weekly-realistic.csv", "type": "sales"}
DATASETS["sample-attribution-weekly"] = {"path": SAMPLE_DIR / "sample-attribution-weekly.csv", "type": "attribution"}
DATASETS["sample-weekly-campaigns"] = {"path": SAMPLE_DIR / "sample-weekly-campaigns.csv", "type": "sales"}

def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _default_reporting_currency() -> str:
    # For now, we treat USD as the default reporting currency.
    # This could later be made configurable without changing the API shape.
    return "USD"


def _with_converted_amount(entry: ExpenseEntry) -> ExpenseEntry:
    """
    Ensure converted_amount is populated.
    For now we assume reporting_currency == currency and fx_rate == 1.0 by default.
    This keeps behaviour predictable while surfacing FX metadata in the UI.
    """
    if entry.reporting_currency is None:
        entry.reporting_currency = _default_reporting_currency()
    if entry.converted_amount is None:
        # If no explicit FX information, assume 1:1 conversion for now.
        entry.fx_rate = entry.fx_rate or 1.0
        entry.converted_amount = entry.amount * (entry.fx_rate or 1.0)
    return entry


# Load sample expenses
EXPENSES = {
    "google_ads_2024-01": _with_converted_amount(
        ExpenseEntry(
            channel="google_ads",
            cost_type="Media Spend",
            amount=12500.00,
            currency="USD",
            reporting_currency=_default_reporting_currency(),
            period="2024-01",
            service_period_start="2024-01-01",
            service_period_end="2024-01-31",
            notes="Google Ads Jan 2024",
            source_type="import",
            source_name="google_ads",
            actor_type="import",
            created_at=_now_iso(),
        )
    ),
    "meta_ads_2024-01": _with_converted_amount(
        ExpenseEntry(
            channel="meta_ads",
            cost_type="Media Spend",
            amount=8200.00,
            currency="USD",
            reporting_currency=_default_reporting_currency(),
            period="2024-01",
            service_period_start="2024-01-01",
            service_period_end="2024-01-31",
            notes="Meta Ads Jan 2024",
            source_type="import",
            source_name="meta_ads",
            actor_type="import",
            created_at=_now_iso(),
        )
    ),
    "linkedin_ads_2024-01": _with_converted_amount(
        ExpenseEntry(
            channel="linkedin_ads",
            cost_type="Media Spend",
            amount=3500.00,
            currency="USD",
            reporting_currency=_default_reporting_currency(),
            period="2024-01",
            service_period_start="2024-01-01",
            service_period_end="2024-01-31",
            notes="LinkedIn Ads Jan 2024",
            source_type="import",
            source_name="linkedin_ads",
            actor_type="import",
            created_at=_now_iso(),
        )
    ),
    "email_2024-01": _with_converted_amount(
        ExpenseEntry(
            channel="email",
            cost_type="Tools/Software",
            amount=450.00,
            currency="USD",
            reporting_currency=_default_reporting_currency(),
            period="2024-01",
            service_period_start="2024-01-01",
            service_period_end="2024-01-31",
            notes="Email platform cost Jan 2024",
            source_type="manual",
            source_name=None,
            actor_type="manual",
            created_at=_now_iso(),
        )
    ),
    "whatsapp_2024-01": _with_converted_amount(
        ExpenseEntry(
            channel="whatsapp",
            cost_type="Tools/Software",
            amount=280.00,
            currency="USD",
            reporting_currency=_default_reporting_currency(),
            period="2024-01",
            service_period_start="2024-01-01",
            service_period_end="2024-01-31",
            notes="WhatsApp Business Jan 2024",
            source_type="manual",
            source_name=None,
            actor_type="manual",
            created_at=_now_iso(),
        )
    ),
}

# Load sample journeys at startup
_sample_paths_file = SAMPLE_DIR / "sample-conversion-paths.json"
if _sample_paths_file.exists():
    try:
        with open(_sample_paths_file) as f:
            JOURNEYS = json.load(f)
    except Exception:
        JOURNEYS = []

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173")


class AuthLoginPayload(BaseModel):
    email: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    provider: str = "bootstrap"
    name: Optional[str] = None
    workspace_id: str = "default"


class AuthWorkspaceSwitchPayload(BaseModel):
    workspace_id: str


def _set_session_cookie(response: Response, session_id: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_id,
        max_age=SESSION_COOKIE_MAX_AGE,
        httponly=True,
        secure=SESSION_COOKIE_SECURE,
        samesite=SESSION_COOKIE_SAMESITE,
        path="/",
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path="/",
        secure=SESSION_COOKIE_SECURE,
        samesite=SESSION_COOKIE_SAMESITE,
    )


def _admin_role_id_for_workspace(db, workspace_id: str) -> Optional[str]:
    role = (
        db.query(ORMRole)
        .filter(
            ORMRole.name == "Admin",
            ORMRole.is_system == True,  # noqa: E712
        )
        .first()
    )
    return role.id if role else None


def _write_security_audit(
    db,
    *,
    actor_user_id: Optional[str],
    workspace_id: Optional[str],
    action_key: str,
    target_type: str,
    target_id: Optional[str],
    metadata: Optional[Dict[str, Any]] = None,
    request: Optional[Request] = None,
) -> None:
    try:
        if not getattr(SETTINGS.feature_flags, "audit_log_enabled", False):
            return
        row = ORMSecurityAuditLog(
            workspace_id=workspace_id,
            actor_user_id=actor_user_id,
            action_key=action_key,
            target_type=target_type,
            target_id=target_id,
            metadata_json=metadata or {},
            ip=(request.client.host if request and request.client else None),
            user_agent=(request.headers.get("user-agent")[:512] if request else None),
            created_at=datetime.utcnow(),
        )
        db.add(row)
        db.commit()
    except Exception as e:
        logger.warning("Failed to write security audit log (%s): %s", action_key, e)


def _invite_token() -> str:
    return secrets.token_urlsafe(32)


def _hash_invite_token(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ==================== Settings API ====================


@app.get("/api/settings")
def get_settings(_ctx: PermissionContext = Depends(require_permission("settings.view"))):
    """Return current configuration for attribution, MMM, and NBA."""
    return SETTINGS


@app.post("/api/settings")
def update_settings(
    new_settings: Settings,
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    """Update global configuration. Overwrites previous settings."""
    global SETTINGS, JOURNEYS
    SETTINGS = new_settings
    JOURNEYS = []
    _save_settings()
    return SETTINGS


@app.get("/api/settings/revenue-config")
def get_revenue_config_settings(_ctx: PermissionContext = Depends(require_permission("settings.view"))):
    return normalize_revenue_config(getattr(SETTINGS, "revenue_config", None).model_dump() if getattr(SETTINGS, "revenue_config", None) else None)


@app.put("/api/settings/revenue-config")
def update_revenue_config_settings(
    payload: RevenueConfig,
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    global SETTINGS, JOURNEYS
    SETTINGS = Settings(
        attribution=SETTINGS.attribution,
        mmm=SETTINGS.mmm,
        nba=SETTINGS.nba,
        feature_flags=SETTINGS.feature_flags,
        revenue_config=RevenueConfig(**normalize_revenue_config(payload.model_dump())),
    )
    JOURNEYS = []
    _save_settings()
    return normalize_revenue_config(SETTINGS.revenue_config.model_dump())


# ---- Notification settings (channels + user prefs) ----
from app.services_notifications import (
    list_channels as list_notification_channels,
    create_channel as create_notification_channel,
    get_channel as get_notification_channel,
    update_channel as update_notification_channel,
    delete_channel as delete_notification_channel,
    list_prefs as list_notification_prefs,
    get_pref as get_notification_pref,
    upsert_pref as upsert_notification_pref,
    update_pref as update_notification_pref,
    delete_pref as delete_notification_pref,
)


def _current_user_id(request: Request) -> str:
    """Resolve current user for notification prefs; session first, then legacy fallback."""
    raw_session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if raw_session_id:
        db = SessionLocal()
        try:
            ctx = resolve_auth_context(db, raw_session_id=raw_session_id)
            if ctx:
                return ctx.user.id
        finally:
            db.close()
    return request.headers.get("X-User-Id") or request.query_params.get("user_id") or "default"


class NotificationChannelCreate(BaseModel):
    type: str  # email | slack_webhook
    config: Dict[str, Any] = Field(default_factory=dict)
    slack_webhook_url: Optional[str] = None  # only for type=slack_webhook; stored securely


class NotificationChannelUpdate(BaseModel):
    config: Optional[Dict[str, Any]] = None
    slack_webhook_url: Optional[str] = None


class NotificationPrefUpdate(BaseModel):
    severities: Optional[List[str]] = None
    digest_mode: Optional[str] = None  # realtime | daily
    quiet_hours: Optional[Dict[str, Any]] = None  # { start, end, timezone }
    is_enabled: Optional[bool] = None


class NotificationPrefUpsert(BaseModel):
    channel_id: int
    severities: List[str] = Field(default_factory=list)
    digest_mode: str = "realtime"
    quiet_hours: Optional[Dict[str, Any]] = None
    is_enabled: bool = False


@app.get("/api/settings/notification-channels")
def api_list_notification_channels(
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.view")),
):
    """List notification channels. Slack webhook URLs are never returned."""
    return list_notification_channels(db)


@app.post("/api/settings/notification-channels")
def api_create_notification_channel(
    body: NotificationChannelCreate,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    """Create a channel. For slack_webhook, provide slack_webhook_url; it is stored securely."""
    try:
        return create_notification_channel(
            db,
            body.type,
            body.config,
            slack_webhook_url=body.slack_webhook_url if body.type == "slack_webhook" else None,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/settings/notification-channels/{channel_id}")
def api_get_notification_channel(
    channel_id: int,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.view")),
):
    out = get_notification_channel(db, channel_id)
    if out is None:
        raise HTTPException(status_code=404, detail="Channel not found")
    return out


@app.put("/api/settings/notification-channels/{channel_id}")
def api_update_notification_channel(
    channel_id: int,
    body: NotificationChannelUpdate,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    out = update_notification_channel(
        db,
        channel_id,
        config=body.config,
        slack_webhook_url=body.slack_webhook_url,
    )
    if out is None:
        raise HTTPException(status_code=404, detail="Channel not found")
    return out


@app.delete("/api/settings/notification-channels/{channel_id}")
def api_delete_notification_channel(
    channel_id: int,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    if not delete_notification_channel(db, channel_id):
        raise HTTPException(status_code=404, detail="Channel not found")
    return {"ok": True}


@app.get("/api/settings/notification-preferences")
def api_list_notification_preferences(
    request: Request,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.view")),
):
    user_id = _current_user_id(request)
    return list_notification_prefs(db, user_id)


@app.post("/api/settings/notification-preferences")
def api_upsert_notification_preference(
    body: NotificationPrefUpsert,
    request: Request,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    user_id = _current_user_id(request)
    return upsert_notification_pref(
        db,
        user_id,
        body.channel_id,
        severities=body.severities,
        digest_mode=body.digest_mode,
        quiet_hours=body.quiet_hours,
        is_enabled=body.is_enabled,
    )


@app.get("/api/settings/notification-preferences/{pref_id}")
def api_get_notification_preference(
    pref_id: int,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.view")),
):
    out = get_notification_pref(db, pref_id)
    if out is None:
        raise HTTPException(status_code=404, detail="Preference not found")
    return out


@app.put("/api/settings/notification-preferences/{pref_id}")
def api_update_notification_preference(
    pref_id: int,
    body: NotificationPrefUpdate,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    out = update_notification_pref(
        db,
        pref_id,
        severities=body.severities,
        digest_mode=body.digest_mode,
        quiet_hours=body.quiet_hours,
        is_enabled=body.is_enabled,
    )
    if out is None:
        raise HTTPException(status_code=404, detail="Preference not found")
    return out


@app.delete("/api/settings/notification-preferences/{pref_id}")
def api_delete_notification_preference(
    pref_id: int,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    if not delete_notification_pref(db, pref_id):
        raise HTTPException(status_code=404, detail="Preference not found")
    return {"ok": True}


@app.post(
    "/api/attribution/preview",
    response_model=AttributionPreviewResponse,
)
def attribution_preview(
    payload: AttributionPreviewPayload,
    db=Depends(get_db),
) -> AttributionPreviewResponse:
    """Return a lightweight preview of how attribution defaults would impact journeys."""
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db, limit=50000)

    if not JOURNEYS:
        return AttributionPreviewResponse(
            previewAvailable=False,
            totalJourneys=0,
            windowImpactCount=0,
            windowDirection="none",
            useConvertedFlagImpact=0,
            useConvertedFlagDirection="none",
            reason="Preview unavailable (no journeys loaded)",
        )

    baseline = SETTINGS.attribution
    proposed = payload.settings

    baseline_window = max(baseline.lookback_window_days, 1)
    proposed_window = max(proposed.lookback_window_days, 1)

    def _journey_duration_days(journey: Dict[str, Any]) -> Optional[int]:
        timestamps = []
        for tp in journey.get("touchpoints", []):
            ts = tp.get("timestamp")
            if not ts:
                continue
            try:
                timestamps.append(datetime.fromisoformat(ts))
            except ValueError:
                continue
        if not timestamps:
            return None
        timestamps.sort()
        delta = timestamps[-1] - timestamps[0]
        return max(delta.days, 0)

    window_direction = "none"
    if proposed_window < baseline_window:
        window_direction = "tighten"
    elif proposed_window > baseline_window:
        window_direction = "loosen"

    window_impact = 0
    for journey in JOURNEYS:
        if not journey.get("converted", True):
            continue
        duration = _journey_duration_days(journey)
        if duration is None:
            continue
        baseline_allowed = duration <= baseline_window
        proposed_allowed = duration <= proposed_window
        if baseline_allowed != proposed_allowed:
            window_impact += 1

    converted_direction = "none"
    converted_impact = 0
    converted_false_count = sum(1 for journey in JOURNEYS if not journey.get("converted", True))

    if baseline.use_converted_flag and not proposed.use_converted_flag:
        converted_direction = "more_included"
        converted_impact = converted_false_count
    elif not baseline.use_converted_flag and proposed.use_converted_flag:
        converted_direction = "fewer_included"
        converted_impact = converted_false_count

    return AttributionPreviewResponse(
        previewAvailable=True,
        totalJourneys=len(JOURNEYS),
        windowImpactCount=window_impact,
        windowDirection=window_direction,
        useConvertedFlagImpact=converted_impact,
        useConvertedFlagDirection=converted_direction,
    )


@app.post(
    "/api/nba/preview",
    response_model=NBAPreviewResponse,
)
def nba_preview(
    payload: NBAPreviewPayload,
    db=Depends(get_db),
) -> NBAPreviewResponse:
    """Return estimated impact metrics for proposed NBA thresholds."""
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db, limit=50000)

    if not JOURNEYS:
        return NBAPreviewResponse(
            previewAvailable=False,
            datasetJourneys=0,
            totalPrefixes=0,
            prefixesEligible=0,
            totalRecommendations=0,
            averageRecommendationsPerPrefix=0.0,
            filteredBySupportPct=0.0,
            filteredByConversionPct=0.0,
            reason="Preview unavailable (no journeys loaded)",
        )

    requested_level = (payload.level or "channel").lower()
    use_level = (
        "campaign"
        if requested_level == "campaign" and has_any_campaign(JOURNEYS)
        else "channel"
    )
    if requested_level == "campaign" and use_level != "campaign":
        reason = "Campaign-level preview unavailable (journeys lack campaign data)"
    else:
        reason = None

    nba_raw = compute_next_best_action(JOURNEYS, level=use_level)
    filtered, stats = _filter_nba_recommendations(nba_raw, payload.settings)

    total_before = stats.get("total_before", 0) or 0
    support_filtered = (
        stats.get("filtered_support", 0) + stats.get("filtered_depth", 0)
    )
    conversion_filtered = stats.get("filtered_conversion", 0)
    prefixes_eligible = stats.get("prefixes_retained", 0)
    total_after = stats.get("total_after", 0)
    avg_per_prefix = (
        total_after / prefixes_eligible if prefixes_eligible else 0.0
    )

    filtered_support_pct = (
        (support_filtered / total_before) * 100 if total_before else 0.0
    )
    filtered_conversion_pct = (
        (conversion_filtered / total_before) * 100 if total_before else 0.0
    )

    return NBAPreviewResponse(
        previewAvailable=True,
        datasetJourneys=len(JOURNEYS),
        totalPrefixes=stats.get("prefixes_considered", 0),
        prefixesEligible=prefixes_eligible,
        totalRecommendations=total_after,
        averageRecommendationsPerPrefix=round(avg_per_prefix, 2),
        filteredBySupportPct=round(filtered_support_pct, 2),
        filteredByConversionPct=round(filtered_conversion_pct, 2),
        reason=reason,
    )


@app.post(
    "/api/nba/test",
    response_model=NBATestResponse,
)
def nba_test(
    payload: NBATestPayload,
    db=Depends(get_db),
) -> NBATestResponse:
    """Return recommendations for a specific prefix using proposed settings."""
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db, limit=50000)

    if not JOURNEYS:
        return NBATestResponse(
            previewAvailable=False,
            prefix=payload.path_prefix or "",
            level=payload.level or "channel",
            totalPrefixSupport=0,
            baselineConversionRate=0.0,
            recommendations=[],
            reason="Recommendations unavailable (no journeys loaded)",
        )

    requested_level = (payload.level or "channel").lower()
    use_level = (
        "campaign"
        if requested_level == "campaign" and has_any_campaign(JOURNEYS)
        else "channel"
    )
    normalized_prefix = payload.path_prefix.strip()
    nba_raw = compute_next_best_action(JOURNEYS, level=use_level)

    if normalized_prefix not in nba_raw and normalized_prefix != "":
        # Allow simple comma-separated prefixes to be normalized via _step_string
        prefix_steps = [
            step.strip()
            for step in normalized_prefix.replace(",", " > ").split(" > ")
            if step.strip()
        ]
        normalized_prefix = " > ".join(prefix_steps)

    prefix_recs = {normalized_prefix: nba_raw.get(normalized_prefix, [])}
    filtered, stats = _filter_nba_recommendations(prefix_recs, payload.settings)
    kept = filtered.get(normalized_prefix, [])

    prefix_support = sum(int(r.get("count", 0)) for r in nba_raw.get(normalized_prefix, []))
    total_conversions = sum(
        int(r.get("conversions", 0)) for r in nba_raw.get(normalized_prefix, [])
    )
    baseline_rate = (
        total_conversions / prefix_support if prefix_support > 0 else 0.0
    )

    recommendations = [
        NBATestRecommendation(
            step=str(rec.get("step") or rec.get("channel")),
            channel=str(rec.get("channel")),
            campaign=rec.get("campaign"),
            count=int(rec.get("count", 0)),
            conversions=int(rec.get("conversions", 0)),
            conversion_rate=float(rec.get("conversion_rate", 0.0)),
            avg_value=float(rec.get("avg_value", 0.0)),
            avg_value_converted=float(rec.get("avg_value_converted", 0.0)),
            uplift_pct=(
                ((float(rec.get("conversion_rate", 0.0)) - baseline_rate) / baseline_rate)
                if baseline_rate > 0
                else None
            ),
        )
        for rec in kept
    ]

    if requested_level == "campaign" and use_level != "campaign":
        reason = "Campaign-level recommendations unavailable (journeys lack campaign data)"
    else:
        reason = None

    return NBATestResponse(
        previewAvailable=True,
        prefix=normalized_prefix or "(start)",
        level=use_level,
        totalPrefixSupport=prefix_support,
        baselineConversionRate=baseline_rate,
        recommendations=recommendations,
        reason=reason,
    )


# ==================== Taxonomy API ====================


@app.get("/api/taxonomy")
def get_taxonomy():
    """Return current channel/source/medium/campaign taxonomy rules."""
    tax = load_taxonomy()

    def _serialize_expression(expr):
        return {
            "operator": expr.normalize_operator(),
            "value": expr.value or "",
        }

    return {
        "channel_rules": [
            {
                "name": r.name,
                "channel": r.channel,
                "priority": r.priority,
                "enabled": r.enabled,
                "source": _serialize_expression(r.source),
                "medium": _serialize_expression(r.medium),
                "campaign": _serialize_expression(r.campaign),
            }
            for r in tax.channel_rules
        ],
        "source_aliases": tax.source_aliases,
        "medium_aliases": tax.medium_aliases,
    }


@app.post("/api/taxonomy")
def update_taxonomy(payload: Dict[str, Any]):
    """Replace taxonomy rules and aliases."""
    def _parse_expression(data: Optional[Dict[str, Any]], fallback_regex: Optional[str] = None):
        if isinstance(data, dict):
            return {
                "operator": data.get("operator", "any"),
                "value": data.get("value", ""),
            }
        if fallback_regex:
            return {"operator": "regex", "value": fallback_regex}
        return {"operator": "any", "value": ""}

    rules: List[ChannelRule] = []
    channel_rules_payload = payload.get("channel_rules", [])
    for idx, rule_payload in enumerate(channel_rules_payload):
        expr_source = _parse_expression(
            rule_payload.get("source"),
            rule_payload.get("source_regex"),
        )
        expr_medium = _parse_expression(
            rule_payload.get("medium"),
            rule_payload.get("medium_regex"),
        )
        expr_campaign = _parse_expression(rule_payload.get("campaign"))

        rules.append(
            ChannelRule(
                name=rule_payload.get("name", ""),
                channel=rule_payload.get("channel", ""),
                priority=int(rule_payload.get("priority", (idx + 1) * 10)),
                enabled=bool(rule_payload.get("enabled", True)),
                source=MatchExpression(**expr_source),
                medium=MatchExpression(**expr_medium),
                campaign=MatchExpression(**expr_campaign),
            )
        )

    rules.sort(key=lambda r: (r.priority, r.name))

    tax = Taxonomy(
        channel_rules=rules,
        source_aliases=payload.get("source_aliases", {}),
        medium_aliases=payload.get("medium_aliases", {}),
    )
    save_taxonomy(tax)
    return get_taxonomy()


@app.post("/api/taxonomy/validate-utm")
def validate_utm_endpoint(params: Dict[str, Any]):
    """
    Validate UTM parameters and return normalized values with warnings/errors.
    
    Example: {"utm_source": "google", "utm_medium": "cpc", "utm_campaign": "brand"}
    """
    from app.services_taxonomy import validate_utm_params
    
    result = validate_utm_params(params)
    return {
        "is_valid": result.is_valid,
        "warnings": result.warnings,
        "errors": result.errors,
        "normalized": result.normalized,
        "confidence": result.confidence,
    }


@app.post("/api/taxonomy/map-channel")
def map_channel_endpoint(body: Dict[str, Any]):
    """
    Map source/medium to channel with confidence score.
    
    Example: {"source": "google", "medium": "cpc"}
    """
    from app.services_taxonomy import map_to_channel
    
    source = body.get("source")
    medium = body.get("medium")
    campaign = body.get("campaign")
    
    mapping = map_to_channel(source, medium, campaign)
    return {
        "channel": mapping.channel,
        "matched_rule": mapping.matched_rule,
        "confidence": mapping.confidence,
        "source": mapping.source,
        "medium": mapping.medium,
        "fallback_reason": mapping.fallback_reason,
    }


@app.get("/api/taxonomy/unknown-share")
def get_unknown_share(limit: int = 20, db=Depends(get_db)):
    """
    Get unknown/unmapped traffic report.
    
    Returns unknown share, breakdowns by source/medium, and sample unmapped touchpoints.
    """
    from app.services_taxonomy import compute_unknown_share
    from app.services_conversions import load_journeys_from_db
    
    journeys = load_journeys_from_db(db, limit=10000)
    if not journeys:
        return {
            "total_touchpoints": 0,
            "unknown_count": 0,
            "unknown_share": 0.0,
            "by_source": {},
            "by_medium": {},
            "top_unmapped_patterns": [],
            "sample_unmapped": [],
        }
    
    report = compute_unknown_share(journeys, sample_size=limit)
    
    # Convert tuple keys to strings for JSON
    top_patterns = sorted(
        [(k, v) for k, v in report.by_source_medium.items()],
        key=lambda x: -x[1]
    )[:limit]
    
    return {
        "total_touchpoints": report.total_touchpoints,
        "unknown_count": report.unknown_count,
        "unknown_share": report.unknown_share,
        "by_source": report.by_source,
        "by_medium": report.by_medium,
        "top_unmapped_patterns": [
            {"source": s, "medium": m, "count": count}
            for (s, m), count in top_patterns
        ],
        "sample_unmapped": report.sample_unmapped,
    }


@app.get("/api/taxonomy/coverage")
def get_taxonomy_coverage(db=Depends(get_db)):
    """
    Get taxonomy coverage report.
    
    Returns channel distribution, source/medium coverage, rule usage, and top unmapped patterns.
    """
    from app.services_taxonomy import compute_taxonomy_coverage
    from app.services_conversions import load_journeys_from_db
    
    journeys = load_journeys_from_db(db, limit=10000)
    if not journeys:
        return {
            "channel_distribution": {},
            "source_coverage": 0.0,
            "medium_coverage": 0.0,
            "rule_usage": {},
            "top_unmapped_patterns": [],
        }
    
    coverage = compute_taxonomy_coverage(journeys)
    return coverage


@app.get("/api/taxonomy/channel-confidence")
def get_channel_confidence(channel: str, db=Depends(get_db)):
    """
    Get confidence metrics for a specific channel.
    
    Returns mean confidence, touchpoint count, low-confidence share, and samples.
    """
    from app.services_taxonomy import compute_channel_confidence
    from app.services_conversions import load_journeys_from_db
    
    journeys = load_journeys_from_db(db, limit=10000)
    if not journeys:
        return {
            "mean_confidence": 0.0,
            "touchpoint_count": 0,
            "low_confidence_count": 0,
            "low_confidence_share": 0.0,
            "sample_low_confidence": [],
        }
    
    confidence = compute_channel_confidence(journeys, channel)
    return confidence


@app.post("/api/taxonomy/compute-dq")
def compute_taxonomy_dq(db=Depends(get_db)):
    """
    Compute and persist taxonomy-specific DQ snapshots.
    
    Metrics: unknown_channel_share, mean_touchpoint_confidence, mean_journey_confidence,
    source_coverage, medium_coverage, low_confidence_touchpoint_share.
    """
    from app.services_taxonomy import persist_taxonomy_dq_snapshots
    from app.services_conversions import load_journeys_from_db
    
    journeys = load_journeys_from_db(db, limit=10000)
    if not journeys:
        return {"computed": 0, "message": "No journeys found"}
    
    snapshots = persist_taxonomy_dq_snapshots(db, journeys)
    
    return {
        "computed": len(snapshots),
        "metrics": [
            {
                "source": s.source,
                "metric_key": s.metric_key,
                "metric_value": s.metric_value,
                "ts_bucket": s.ts_bucket,
            }
            for s in snapshots
        ],
    }


# ==================== KPI / Conversion API ====================


class KpiDefinitionModel(BaseModel):
    id: str
    label: str
    type: str  # "primary" or "micro"
    event_name: str
    value_field: Optional[str] = None
    weight: float = 1.0
    lookback_days: Optional[int] = None


class KpiConfigModel(BaseModel):
    definitions: List[KpiDefinitionModel]
    primary_kpi_id: Optional[str] = None


class KpiTestPayload(BaseModel):
    definition: KpiDefinitionModel


@app.get("/api/kpis", response_model=KpiConfigModel)
def get_kpis():
    """Return configured KPI and micro-conversion definitions."""
    cfg = KPI_CONFIG
    return KpiConfigModel(
        definitions=[KpiDefinitionModel(**d.__dict__) for d in cfg.definitions],
        primary_kpi_id=cfg.primary_kpi_id,
    )


@app.post("/api/kpis/test")
def test_kpi_definition(payload: KpiTestPayload, db=Depends(get_db)):
    """Evaluate a KPI definition against loaded journeys for quick validation."""
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        return {
            "testAvailable": False,
            "eventsMatched": 0,
            "journeysMatched": 0,
            "journeysTotal": 0,
            "journeysPct": 0.0,
            "missingValueChecks": 0,
            "missingValueCount": 0,
            "missingValuePct": None,
            "message": None,
            "reason": "Load sample data to test",
        }

    definition = payload.definition
    total_journeys = len(JOURNEYS)
    target_event = (definition.event_name or definition.id or "").strip().lower()
    matched_events = 0
    journeys_matched = 0
    missing_value_checks = 0
    missing_value_count = 0
    fallback_used = False

    def _record_value(source: Dict[str, Any]) -> None:
        nonlocal missing_value_checks, missing_value_count
        if definition.value_field:
            missing_value_checks += 1
            value = source.get(definition.value_field)
            if value in (None, "", []):
                missing_value_count += 1

    for journey in JOURNEYS:
        journey_matches = False
        events = journey.get("events") or []

        if target_event:
            for event in events:
                name = str(event.get("name") or event.get("event_name") or "").strip().lower()
                if name and name == target_event:
                    matched_events += 1
                    journey_matches = True
                    _record_value(event)

        if not journey_matches:
            journey_type = str(journey.get("kpi_type") or "").strip().lower()
            if definition.id and journey_type == definition.id.strip().lower():
                matched_events += 1
                journey_matches = True
                _record_value(journey)
                fallback_used = True
            elif not target_event and definition.event_name == "":
                # If no specific event provided, treat conversion flag as match
                if journey.get("converted", False):
                    matched_events += 1
                    journey_matches = True
                    _record_value(journey)
                    fallback_used = True

        if journey_matches:
            journeys_matched += 1

    journeys_pct = (
        (journeys_matched / total_journeys) * 100.0 if total_journeys else 0.0
    )
    missing_value_pct = (
        (missing_value_count / missing_value_checks) * 100.0
        if missing_value_checks
        else None
    )

    return {
        "testAvailable": True,
        "eventsMatched": matched_events,
        "journeysMatched": journeys_matched,
        "journeysTotal": total_journeys,
        "journeysPct": round(journeys_pct, 2),
        "missingValueChecks": missing_value_checks,
        "missingValueCount": missing_value_count,
        "missingValuePct": round(missing_value_pct, 2) if missing_value_pct is not None else None,
        "message": (
            "Matched using KPI ID fallback; event stream not found."
            if fallback_used and target_event
            else None
        ),
        "reason": None,
    }


@app.post("/api/kpis", response_model=KpiConfigModel)
def update_kpis(cfg: KpiConfigModel):
    """Replace KPI configuration."""
    global KPI_CONFIG
    defs = [KpiDefinition(**d.dict()) for d in cfg.definitions]
    KPI_CONFIG = KpiConfig(definitions=defs, primary_kpi_id=cfg.primary_kpi_id)
    save_kpi_config(KPI_CONFIG)
    return get_kpis()


# ==================== Health ====================

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        **engine_info(),
        "journeys_loaded": len(JOURNEYS),
        "attribution_models": ATTRIBUTION_MODELS,
    }

# ==================== Attribution API ====================

@app.get("/api/attribution/models")
def list_attribution_models():
    """List available attribution models."""
    return {"models": ATTRIBUTION_MODELS}

def _compute_journey_validation(journeys: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute validation summary: error/warn counts and top issues."""
    error_list: List[str] = []
    warn_list: List[str] = []
    seen_ids: Dict[str, int] = {}
    for j in journeys:
        pid = str(j.get("customer_id") or j.get("profile_id") or j.get("id") or "")
        if not pid or pid.startswith("anon-"):
            warn_list.append("Missing or anonymous customer_id")
        else:
            seen_ids[pid] = seen_ids.get(pid, 0) + 1
        tps = j.get("touchpoints") or []
        if not tps:
            error_list.append("Journey has no touchpoints")
        for tp in tps:
            if not tp.get("channel") and not tp.get("source"):
                warn_list.append("Touchpoint missing channel/source")
            if not tp.get("timestamp"):
                warn_list.append("Touchpoint missing timestamp")
    dup_ids = [pid for pid, c in seen_ids.items() if c > 1]
    if dup_ids:
        warn_list.append(f"Duplicate customer_ids: {len(dup_ids)} ids")
    top_errors = list(dict.fromkeys(error_list))[:5]
    top_warnings = list(dict.fromkeys(warn_list))[:5]
    return {
        "error_count": len(error_list),
        "warn_count": len(warn_list),
        "top_errors": top_errors,
        "top_warnings": top_warnings,
        "duplicate_ids_count": len(dup_ids),
    }


def _compute_data_freshness_hours(last_ts) -> Optional[float]:
    """Hours since last touchpoint. None if no timestamps."""
    if last_ts is None:
        return None
    try:
        dt = pd.to_datetime(last_ts, errors="coerce") if isinstance(last_ts, str) else last_ts
        if dt is None or pd.isna(dt):
            return None
        delta = datetime.utcnow() - (dt.to_pydatetime() if hasattr(dt, "to_pydatetime") else dt)
        return delta.total_seconds() / 3600.0
    except Exception:
        return None


def _derive_system_state(loaded: bool, count: int, last_import_at: Optional[str], freshness_hours: Optional[float], error_count: int, warn_count: int) -> str:
    """Derive system state: Empty / Loading / Data Loaded / Stale / Partial / Error."""
    if not loaded or count == 0:
        return "empty"
    if error_count > 0:
        return "error"
    if warn_count > 0 and warn_count >= count * 0.1:
        return "partial"
    if freshness_hours is not None and freshness_hours > 168:  # > 7 days
        return "stale"
    return "data_loaded"


@app.get("/api/attribution/journeys")
def get_journeys_summary(db=Depends(get_db)):
    """Get summary of currently loaded conversion journeys including status, freshness, and system state."""
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        runs = get_import_runs(limit=1)
        last_run = runs[0] if runs and runs[0].get("status") == "success" else None
        return {
            "loaded": False,
            "count": 0,
            "converted": 0,
            "channels": [],
            "last_import_at": last_run.get("at") if last_run else None,
            "last_import_source": last_run.get("source") if last_run else None,
            "data_freshness_hours": None,
            "system_state": "empty",
            "validation": {"error_count": 0, "warn_count": 0},
        }

    converted = [j for j in JOURNEYS if j.get("converted", True)]
    channels: set = set()
    first_ts = None
    last_ts = None
    for j in JOURNEYS:
        for tp in j.get("touchpoints", []):
            channels.add(tp.get("channel", "unknown"))
            ts = tp.get("timestamp")
            if not ts:
                continue
            try:
                dt = pd.to_datetime(ts, errors="coerce")
            except Exception:
                dt = None
            if dt is None or pd.isna(dt):
                continue
            if first_ts is None or dt < first_ts:
                first_ts = dt
            if last_ts is None or dt > last_ts:
                last_ts = dt

    kpi_counts: Dict[str, int] = {}
    for j in JOURNEYS:
        ktype = j.get("kpi_type")
        if isinstance(ktype, str):
            kpi_counts[ktype] = kpi_counts.get(ktype, 0) + 1

    primary_kpi_id = KPI_CONFIG.primary_kpi_id
    primary_kpi_label = None
    if primary_kpi_id:
        for d in KPI_CONFIG.definitions:
            if d.id == primary_kpi_id:
                primary_kpi_label = d.label
                break
    primary_count = kpi_counts.get(primary_kpi_id, len(converted) if primary_kpi_id else len(converted))

    runs = get_import_runs(limit=50)
    last_run = next((r for r in runs if r.get("status") == "success"), None)
    last_import_at = last_run.get("at") if last_run else None
    last_import_source = last_run.get("source") if last_run else None
    freshness_hours = _compute_data_freshness_hours(last_ts)
    validation = _compute_journey_validation(JOURNEYS)
    system_state = _derive_system_state(
        True, len(JOURNEYS), last_import_at, freshness_hours,
        validation["error_count"], validation["warn_count"],
    )

    return {
        "loaded": True,
        "count": len(JOURNEYS),
        "converted": len(converted),
        "non_converted": len(JOURNEYS) - len(converted),
        "channels": sorted(channels),
        "total_value": sum(j.get("conversion_value", 0) for j in converted),
        "primary_kpi_id": primary_kpi_id,
        "primary_kpi_label": primary_kpi_label,
        "primary_kpi_count": primary_count,
        "kpi_counts": kpi_counts,
        "date_min": first_ts.isoformat() if first_ts is not None else None,
        "date_max": last_ts.isoformat() if last_ts is not None else None,
        "last_import_at": last_import_at,
        "last_import_source": last_import_source,
        "data_freshness_hours": round(freshness_hours, 1) if freshness_hours is not None else None,
        "system_state": system_state,
        "validation": validation,
    }


@app.get("/api/attribution/journeys/preview")
def get_journeys_preview(limit: int = Query(20, ge=1, le=100), db=Depends(get_db)):
    """Preview first N parsed journeys: customer_id, touchpoints_count, first_ts, last_ts, converted, conversion_value, channels_list."""
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        return {"rows": [], "columns": [], "total": 0}
    rows = []
    for idx, j in enumerate(JOURNEYS[:limit]):
        tps = j.get("touchpoints") or []
        first_ts = None
        last_ts = None
        for tp in tps:
            ts = tp.get("timestamp")
            if ts:
                try:
                    dt = pd.to_datetime(ts, errors="coerce")
                    if dt is not None and not pd.isna(dt):
                        if first_ts is None or dt < first_ts:
                            first_ts = dt
                        if last_ts is None or dt > last_ts:
                            last_ts = dt
                except Exception:
                    pass
        channels = list(dict.fromkeys(tp.get("channel", "?") for tp in tps))
        rows.append({
            "validIndex": idx,
            "customer_id": j.get("customer_id") or j.get("profile_id") or j.get("id") or "—",
            "touchpoints_count": len(tps),
            "first_ts": first_ts.isoformat() if first_ts is not None else None,
            "last_ts": last_ts.isoformat() if last_ts is not None else None,
            "converted": j.get("converted", True),
            "conversion_value": j.get("conversion_value"),
            "channels_list": channels,
        })
    return {
        "rows": rows,
        "columns": ["customer_id", "touchpoints_count", "first_ts", "last_ts", "converted", "conversion_value", "channels_list"],
        "total": len(JOURNEYS),
    }


@app.get("/api/attribution/journeys/validation")
def get_journeys_validation(db=Depends(get_db)):
    """Validation summary: structured items + import summary from last ingestion."""
    last = _load_last_import_result()
    items = last.get("validation_items", [])
    summary = last.get("import_summary", {})
    error_count = sum(1 for x in items if x.get("severity") == "error")
    warn_count = sum(1 for x in items if x.get("severity") == "warning")
    return {
        "error_count": error_count,
        "warn_count": warn_count,
        "validation_items": items,
        "import_summary": summary,
        "total": summary.get("total", 0),
        "top_errors": [x["message"] for x in items if x.get("severity") == "error"][:5],
        "top_warnings": [x["message"] for x in items if x.get("severity") == "warning"][:5],
    }


@app.get("/api/attribution/journeys/import-result")
def get_import_result():
    """Last import result for drawer (original vs normalized per row) and validation report."""
    return _load_last_import_result()


@app.get("/api/attribution/journeys/row-details")
def get_row_details(valid_index: int = Query(..., ge=0)):
    """Get original and normalized JSON for a preview row (by validIndex)."""
    last = _load_last_import_result()
    items = last.get("items_detail", [])
    for it in items:
        if it.get("valid") and it.get("validIndex") == valid_index:
            return {"original": it.get("original"), "normalized": it.get("normalized"), "journeyIndex": it.get("journeyIndex")}
    raise HTTPException(status_code=404, detail="Row not found")


@app.get("/api/attribution/journeys/validation-report")
def download_validation_report():
    """Download validation report as JSON file."""
    last = _load_last_import_result()
    content = json.dumps(last, indent=2, default=str)
    return Response(
        content=content,
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=validation-report.json"},
    )


@app.get("/api/attribution/import-log")
def get_import_log(
    status: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
    since: Optional[str] = Query(None),
    until: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=200),
):
    """Return import runs with filters for Import Log UI."""
    runs = get_import_runs(status=status, source=source, since=since, until=until, limit=limit)
    return {"runs": runs}


@app.get("/api/attribution/import-log/{run_id}")
def get_import_run_detail(run_id: str):
    """Return single import run detail for drawer."""
    run = get_import_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Import run not found")
    out = dict(run)
    out["at"] = run.get("finished_at") or run.get("started_at")
    out["count"] = run.get("valid") if run.get("status") == "success" else 0
    return out


class ImportPreCheckResponse(BaseModel):
    would_overwrite: bool
    current_count: int
    current_converted: int
    current_channels: List[str]
    incoming_count: int
    incoming_converted: int
    incoming_channels: List[str]
    converted_drop_pct: Optional[float] = None
    warning: Optional[str] = None


@app.get("/api/attribution/import-precheck")
def import_precheck(source: str = Query(...), db=Depends(get_db)):
    """Pre-check: what would change if we import. For confirmation modal."""
    from app.services_conversions import load_journeys_from_db
    current = load_journeys_from_db(db)
    current_count = len(current or [])
    current_converted = sum(1 for j in (current or []) if j.get("converted", True))
    current_channels = sorted(set(tp.get("channel", "unknown") for j in (current or []) for tp in j.get("touchpoints", [])))
    last_success = get_last_successful_run()
    # We don't know incoming counts until we actually run - use last run as proxy for "incoming"
    if last_success and last_success.get("source") == source:
        incoming_count = last_success.get("valid", 0)
        incoming_converted = last_success.get("converted", 0)
        incoming_channels = last_success.get("channels_detected", [])
    else:
        incoming_count = 0
        incoming_converted = 0
        incoming_channels = []
    would_overwrite = current_count > 0
    drop_pct = None
    warning = None
    if would_overwrite and current_converted > 0 and incoming_converted < current_converted:
        drop_pct = ((current_converted - incoming_converted) / current_converted) * 100
        if drop_pct > 10:
            warning = f"Converted journeys would drop by {drop_pct:.0f}%. Verify your import source and mapping."
    return ImportPreCheckResponse(
        would_overwrite=would_overwrite,
        current_count=current_count,
        current_converted=current_converted,
        current_channels=current_channels,
        incoming_count=incoming_count,
        incoming_converted=incoming_converted,
        incoming_channels=incoming_channels,
        converted_drop_pct=drop_pct,
        warning=warning,
    )


class ImportConfirmRequest(BaseModel):
    confirmed: bool = True
    import_note: Optional[str] = None


@app.post("/api/attribution/import-log/{run_id}/rerun")
def import_rerun(run_id: str, db=Depends(get_db)):
    """Re-run import with same settings as the given run."""
    run = get_import_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Import run not found")
    source = run.get("source", "")
    if source == "sample":
        return load_sample_journeys(req=LoadSampleRequest(), db=db)
    if source in ("meiro_webhook", "meiro_pull"):
        return import_journeys_from_cdp(req=FromCDPRequest(), db=db)
    if source == "meiro":
        return import_journeys_from_cdp(req=FromCDPRequest(), db=db)
    raise HTTPException(status_code=400, detail=f"Rerun not supported for source: {source}")


@app.get("/api/attribution/field-mapping")
def get_field_mapping():
    """Return default CDP field mapping (for Field Mapping tab)."""
    return {
        "touchpoint_attr": "touchpoints",
        "value_attr": "conversion_value",
        "id_attr": "customer_id",
        "channel_field": "channel",
        "timestamp_field": "timestamp",
    }


@app.get("/api/datasources/connections")
def get_datasource_connections(db=Depends(get_db)):
    """Combined connection status for Meiro CDP and ad platforms (for Connections section)."""
    config_status = ds_config.get_status()
    meiro_connected = meiro_cdp.is_connected()
    oauth_items = list_oauth_connections(db, workspace_id="default").get("items", [])
    by_provider = {row.get("provider_key"): row for row in oauth_items}

    def _status_for(provider_key: str) -> str:
        row = by_provider.get(provider_key)
        if not row:
            return "needs_attention" if not config_status.get(provider_key.split("_", 1)[0], {}).get("configured") else "not_connected"
        status = (row.get("status") or "").lower()
        if status in ("connected", "error", "disabled", "needs_reauth"):
            return status
        if status == "not_connected":
            return "not_connected"
        return "not_connected"

    connections = [
        {"id": "meiro", "label": "Meiro CDP", "status": "connected" if meiro_connected else "not_connected", "role": "journeys"},
        {
            "id": "meta",
            "provider_key": "meta_ads",
            "label": "Meta Ads",
            "status": _status_for("meta_ads"),
            "role": "spend",
            "selected_accounts_count": int((by_provider.get("meta_ads") or {}).get("selected_accounts_count") or 0),
            "last_error": (by_provider.get("meta_ads") or {}).get("last_error"),
            "last_tested_at": (by_provider.get("meta_ads") or {}).get("last_tested_at"),
        },
        {
            "id": "google",
            "provider_key": "google_ads",
            "label": "Google Ads",
            "status": _status_for("google_ads"),
            "role": "spend",
            "selected_accounts_count": int((by_provider.get("google_ads") or {}).get("selected_accounts_count") or 0),
            "last_error": (by_provider.get("google_ads") or {}).get("last_error"),
            "last_tested_at": (by_provider.get("google_ads") or {}).get("last_tested_at"),
        },
        {
            "id": "linkedin",
            "provider_key": "linkedin_ads",
            "label": "LinkedIn Ads",
            "status": _status_for("linkedin_ads"),
            "role": "spend",
            "selected_accounts_count": int((by_provider.get("linkedin_ads") or {}).get("selected_accounts_count") or 0),
            "last_error": (by_provider.get("linkedin_ads") or {}).get("last_error"),
            "last_tested_at": (by_provider.get("linkedin_ads") or {}).get("last_tested_at"),
        },
    ]
    # Append inventory-backed connectors (warehouses and other future systems).
    try:
        ds_rows = list_data_sources(db, workspace_id="default", category=None).get("items", [])
        for row in ds_rows:
            connections.append(
                {
                    "id": row.get("id"),
                    "label": row.get("name"),
                    "status": row.get("status", "not_connected"),
                    "role": row.get("category", "system"),
                    "type": row.get("type"),
                    "last_tested_at": row.get("last_tested_at"),
                }
            )
    except Exception:
        pass
    return {"connections": connections}


class DataSourceCreatePayload(BaseModel):
    workspace_id: str = "default"
    category: str = Field(..., pattern="^(warehouse|ad_platform|cdp)$")
    type: str
    name: str = Field(..., min_length=1, max_length=255)
    config_json: Dict[str, Any] = Field(default_factory=dict)
    secrets: Dict[str, Any] = Field(default_factory=dict)


class DataSourceUpdatePayload(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    status: Optional[str] = Field(None, pattern="^(connected|error|needs_reauth|disabled)$")
    config_json: Optional[Dict[str, Any]] = None


class DataSourceTestPayload(BaseModel):
    type: str
    config_json: Dict[str, Any] = Field(default_factory=dict)
    secrets: Dict[str, Any] = Field(default_factory=dict)


class DataSourceRotateCredentialsPayload(BaseModel):
    secrets: Dict[str, Any] = Field(default_factory=dict)


def get_datasource_user(
    x_user_id: Optional[str] = Header(None, alias="X-User-Id"),
    x_user_role: Optional[str] = Header(None, alias="X-User-Role"),
):
    user_id = x_user_id or "system"
    can_edit = (x_user_role or "").strip().lower() in ("admin", "editor")
    return user_id, can_edit


@app.get("/api/data-sources")
def api_list_data_sources(
    category: Optional[str] = Query(None, description="warehouse|ad_platform|cdp"),
    workspace_id: str = Query("default"),
    db=Depends(get_db),
):
    return list_data_sources(db, workspace_id=workspace_id, category=category)


@app.post("/api/data-sources")
def api_create_data_source(
    body: DataSourceCreatePayload,
    db=Depends(get_db),
    user_info: Tuple[str, bool] = Depends(get_datasource_user),
):
    _, can_edit = user_info
    if not can_edit:
        raise HTTPException(status_code=403, detail="Only admin or editor can create data sources")
    try:
        out = create_data_source(
            db,
            workspace_id=body.workspace_id,
            category=body.category,
            source_type=body.type,
            name=body.name,
            config_json=body.config_json or {},
            secrets=body.secrets or {},
        )
        return out
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.put("/api/data-sources/{source_id}")
def api_update_data_source(
    source_id: str,
    body: DataSourceUpdatePayload,
    db=Depends(get_db),
    user_info: Tuple[str, bool] = Depends(get_datasource_user),
):
    _, can_edit = user_info
    if not can_edit:
        raise HTTPException(status_code=403, detail="Only admin or editor can update data sources")
    try:
        out = update_data_source(
            db,
            source_id=source_id,
            name=body.name,
            status=body.status,
            config_json=body.config_json,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not out:
        raise HTTPException(status_code=404, detail="Data source not found")
    return out


@app.post("/api/data-sources/test")
def api_test_data_source(
    body: DataSourceTestPayload,
    db=Depends(get_db),
):
    try:
        return test_data_source_payload(
            db,
            source_type=body.type,
            config_json=body.config_json or {},
            secrets=body.secrets or {},
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/data-sources/{source_id}/test")
def api_test_saved_data_source(
    source_id: str,
    db=Depends(get_db),
):
    out = test_saved_data_source(db, source_id=source_id)
    if not out:
        raise HTTPException(status_code=404, detail="Data source not found")
    return out


@app.post("/api/data-sources/{source_id}/disable")
def api_disable_data_source(
    source_id: str,
    db=Depends(get_db),
    user_info: Tuple[str, bool] = Depends(get_datasource_user),
):
    _, can_edit = user_info
    if not can_edit:
        raise HTTPException(status_code=403, detail="Only admin or editor can disable data sources")
    if not disable_data_source(db, source_id):
        raise HTTPException(status_code=404, detail="Data source not found")
    return {"ok": True}


@app.delete("/api/data-sources/{source_id}")
def api_delete_data_source(
    source_id: str,
    db=Depends(get_db),
    user_info: Tuple[str, bool] = Depends(get_datasource_user),
):
    _, can_edit = user_info
    if not can_edit:
        raise HTTPException(status_code=403, detail="Only admin or editor can delete data sources")
    if not delete_data_source(db, source_id):
        raise HTTPException(status_code=404, detail="Data source not found")
    return {"ok": True}


@app.post("/api/data-sources/{source_id}/rotate-credentials")
def api_rotate_data_source_credentials(
    source_id: str,
    body: DataSourceRotateCredentialsPayload,
    db=Depends(get_db),
    user_info: Tuple[str, bool] = Depends(get_datasource_user),
):
    _, can_edit = user_info
    if not can_edit:
        raise HTTPException(status_code=403, detail="Only admin or editor can rotate credentials")
    try:
        out = rotate_data_source_credentials(db, source_id=source_id, secrets=body.secrets or {})
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not out:
        raise HTTPException(status_code=404, detail="Data source not found")
    return out


@app.post("/api/attribution/journeys/upload")
async def upload_journeys(file: UploadFile = File(...), import_note: Optional[str] = Form(None), db=Depends(get_db)):
    """Upload conversion path data as JSON. Validates, normalizes, persists valid journeys only."""
    global JOURNEYS
    try:
        content = await file.read()
        data = json.loads(content)
        if not (isinstance(data, list) or (isinstance(data, dict) and "journeys" in data)):
            if isinstance(data, dict) and data.get("schema_version") == "2.0":
                pass
            else:
                raise ValueError("Expected JSON array of journeys or v2 envelope with 'journeys'")
    except (json.JSONDecodeError, ValueError) as e:
        _append_import_run("upload", 0, "error", str(e))
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    result = validate_and_normalize(data)
    valid = result["valid_journeys"]
    summary = result["import_summary"]
    persist_journeys_as_conversion_paths(db, valid, replace=True)
    _refresh_journey_aggregates_after_import(db)
    JOURNEYS = []
    _save_last_import_result(result)
    errs = [i for i in result.get("validation_items", []) if i.get("severity") == "error"]
    warns = [i for i in result.get("validation_items", []) if i.get("severity") == "warning"]
    _append_import_run(
        "upload", len(valid), "success",
        total=summary.get("total", 0), valid=summary.get("valid", 0), invalid=summary.get("invalid", 0),
        converted=summary.get("converted", 0), channels_detected=summary.get("channels_detected"),
        validation_summary={"top_errors": errs[:10], "top_warnings": warns[:10]},
        config_snapshot={"schema_version": result.get("schema_version", "1.0")},
        preview_rows=[{"customer_id": j.get("customer", {}).get("id", "?"), "touchpoints": len(j.get("touchpoints", [])), "converted": bool(j.get("conversions"))} for j in valid[:20]],
        import_note=import_note,
    )
    return {
        "count": len(valid),
        "message": f"Loaded {len(valid)} valid journeys",
        "import_summary": summary,
        "validation_items": result["validation_items"],
    }

@app.post("/api/attribution/journeys/from-cdp")
def import_journeys_from_cdp(
    req: FromCDPRequest = Body(default=FromCDPRequest()),
    db=Depends(get_db),
):
    """Parse loaded CDP profiles into attribution journeys using configurable mapping."""
    global JOURNEYS
    cdp_json_path = DATA_DIR / "meiro_cdp_profiles.json"
    cdp_path = DATA_DIR / "meiro_cdp.csv"

    saved = get_mapping()
    base = AttributionMappingConfig(
        touchpoint_attr=saved.get("touchpoint_attr", "touchpoints"),
        value_attr=saved.get("value_attr", "conversion_value"),
        id_attr=saved.get("id_attr", "customer_id"),
        channel_field=saved.get("channel_field", "channel"),
        timestamp_field=saved.get("timestamp_field", "timestamp"),
    )
    mapping = base
    if req.mapping:
        mapping = AttributionMappingConfig(
            touchpoint_attr=req.mapping.touchpoint_attr or base.touchpoint_attr,
            value_attr=req.mapping.value_attr or base.value_attr,
            id_attr=req.mapping.id_attr or base.id_attr,
            channel_field=req.mapping.channel_field or base.channel_field,
            timestamp_field=req.mapping.timestamp_field or base.timestamp_field,
        )

    source_label = "meiro_webhook"
    if cdp_json_path.exists():
        with open(cdp_json_path) as f:
            profiles = json.load(f)
    elif cdp_path.exists():
        df = pd.read_csv(cdp_path)
        profiles = df.to_dict(orient="records")
        source_label = "meiro_pull"
    else:
        _append_import_run("meiro_webhook", 0, "error", error="No CDP data found. Fetch from Meiro CDP first.")
        raise HTTPException(status_code=404, detail="No CDP data found. Fetch from Meiro CDP first.")

    try:
        journeys = parse_conversion_paths(
            profiles,
            touchpoint_attr=mapping.touchpoint_attr,
            value_attr=mapping.value_attr,
            id_attr=mapping.id_attr,
            channel_field=mapping.channel_field,
            timestamp_field=mapping.timestamp_field,
        )
    except Exception as e:
        _append_import_run(source_label, 0, "error", error=str(e))
        raise

    effective_config_id = req.config_id or get_default_config_id()
    if effective_config_id:
        cfg = db.get(ORMModelConfig, effective_config_id)
        if cfg:
            journeys = apply_model_config_to_journeys(journeys, cfg.config_json or {})

    persist_journeys_as_conversion_paths(db, journeys, replace=True)
    _refresh_journey_aggregates_after_import(db)
    JOURNEYS = []
    converted = sum(1 for j in journeys if j.get("converted", True))
    ch_set = set()
    for j in journeys:
        for tp in j.get("touchpoints", []):
            ch_set.add(tp.get("channel", "unknown"))
    _append_import_run(
        "meiro_webhook", len(journeys), "success",
        total=len(journeys), valid=len(journeys), converted=converted, channels_detected=sorted(ch_set),
        config_snapshot={"mapping_preset": "saved", "schema_version": "1.0"},
        preview_rows=[{"customer_id": j.get("customer_id", "?"), "touchpoints": len(j.get("touchpoints", [])), "value": j.get("conversion_value", 0)} for j in journeys[:20]],
        import_note=req.import_note,
    )
    return {"count": len(journeys), "message": f"Parsed {len(journeys)} journeys from CDP data"}

class LoadSampleRequest(BaseModel):
    import_note: Optional[str] = None


@app.post("/api/attribution/journeys/load-sample")
def load_sample_journeys(req: LoadSampleRequest = Body(default=LoadSampleRequest()), db=Depends(get_db)):
    """Load the built-in sample conversion paths. Validates and normalizes."""
    global JOURNEYS
    sample_file = SAMPLE_DIR / "sample-conversion-paths.json"
    if not sample_file.exists():
        _append_import_run("sample", 0, "error", "Sample data not found")
        raise HTTPException(status_code=404, detail="Sample data not found")
    try:
        with open(sample_file) as f:
            raw_list = json.load(f)
    except Exception as e:
        _append_import_run("sample", 0, "error", str(e))
        raise HTTPException(status_code=500, detail=f"Failed to read sample: {e}")

    result = validate_and_normalize(raw_list)
    valid = result["valid_journeys"]
    summary = result["import_summary"]
    persist_journeys_as_conversion_paths(db, valid, replace=True)
    _refresh_journey_aggregates_after_import(db)
    JOURNEYS = []
    _save_last_import_result(result)
    errs = [i for i in result.get("validation_items", []) if i.get("severity") == "error"]
    warns = [i for i in result.get("validation_items", []) if i.get("severity") == "warning"]
    _append_import_run(
        "sample", len(valid), "success",
        total=summary.get("total", 0), valid=summary.get("valid", 0), invalid=summary.get("invalid", 0),
        converted=summary.get("converted", 0), channels_detected=summary.get("channels_detected"),
        validation_summary={"top_errors": errs[:10], "top_warnings": warns[:10]},
        config_snapshot={"schema_version": result.get("schema_version", "1.0")},
        preview_rows=[{"customer_id": j.get("customer", {}).get("id", "?"), "touchpoints": len(j.get("touchpoints", [])), "converted": bool(j.get("conversions"))} for j in valid[:20]],
        import_note=req.import_note,
    )
    return {
        "count": len(valid),
        "message": f"Loaded {len(valid)} valid journeys",
        "import_summary": summary,
        "validation_items": result["validation_items"],
    }

@app.post("/api/attribution/run")
def run_attribution_model(model: str = "linear", config_id: Optional[str] = None, db=Depends(get_db)):
    """Run a single attribution model on loaded journeys.

    Optional config_id applies time windows and conversion keys before attribution.
    """
    global JOURNEYS
    if not JOURNEYS:
        # Lazy-load from normalised storage if nothing is in memory yet.
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded. Upload, import, or persist data first.")
    if model not in ATTRIBUTION_MODELS:
        raise HTTPException(status_code=400, detail=f"Unknown model: {model}. Available: {ATTRIBUTION_MODELS}")
    resolved_cfg, meta = load_config_and_meta(db, config_id)
    journeys_for_model = JOURNEYS
    if resolved_cfg:
        journeys_for_model = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})

    kwargs: Dict[str, Any] = {}
    if model == "time_decay":
        kwargs["half_life_days"] = SETTINGS.attribution.time_decay_half_life_days
    elif model == "position_based":
        kwargs["first_pct"] = SETTINGS.attribution.position_first_pct
        kwargs["last_pct"] = SETTINGS.attribution.position_last_pct
    result = run_attribution(journeys_for_model, model=model, **kwargs)
    # Attach config metadata for measurement context if available
    if meta:
        result["config"] = meta
    ATTRIBUTION_RESULTS[model] = result
    return result

@app.post("/api/attribution/run-all")
def run_all_attribution_models(config_id: Optional[str] = None, db=Depends(get_db)):
    """Run all attribution models on loaded journeys."""
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded. Upload, import, or persist data first.")
    resolved_cfg, meta = load_config_and_meta(db, config_id)
    journeys_for_model = JOURNEYS
    if resolved_cfg:
        journeys_for_model = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})

    results = []
    for model in ATTRIBUTION_MODELS:
        try:
            kwargs: Dict[str, Any] = {}
            if model == "time_decay":
                kwargs["half_life_days"] = SETTINGS.attribution.time_decay_half_life_days
            elif model == "position_based":
                kwargs["first_pct"] = SETTINGS.attribution.position_first_pct
                kwargs["last_pct"] = SETTINGS.attribution.position_last_pct
            result = run_attribution(journeys_for_model, model=model, **kwargs)
            if meta:
                result["config"] = meta
            results.append(result)
            ATTRIBUTION_RESULTS[model] = result
        except Exception as exc:
            results.append({"model": model, "error": str(exc)})
    return {"results": results}

@app.get("/api/attribution/results")
def get_attribution_results():
    """Get cached attribution results for all models that have been run."""
    return ATTRIBUTION_RESULTS

@app.get("/api/attribution/results/{model}")
def get_attribution_result(model: str):
    """Get attribution result for a specific model."""
    result = ATTRIBUTION_RESULTS.get(model)
    if not result:
        raise HTTPException(status_code=404, detail=f"No results for model '{model}'. Run it first.")
    return result


def _attribution_weekly_series(
    journeys: List[Dict],
    model: str,
    date_start: str,
    date_end: str,
    config_id: Optional[str] = None,
    db=None,
) -> List[Dict[str, Any]]:
    """Return list of { date, attributed_value } for each week by running attribution on journeys that converted that week."""
    import pandas as pd
    WEEK_FREQ = "W-MON"
    ds = pd.to_datetime(date_start, errors="coerce")
    de = pd.to_datetime(date_end, errors="coerce")
    if pd.isna(ds) or pd.isna(de) or ds > de:
        return []
    date_start_ts = ds.normalize().to_period(WEEK_FREQ).start_time
    date_end_ts = de.normalize().to_period(WEEK_FREQ).start_time
    week_range = pd.date_range(start=date_start_ts, end=date_end_ts, freq=WEEK_FREQ)

    def _conversion_week(j):
        tps = j.get("touchpoints") or []
        last_ts = None
        for tp in tps:
            ts = tp.get("timestamp")
            if not ts:
                continue
            try:
                t = pd.to_datetime(ts, errors="coerce")
                if pd.notna(t):
                    last_ts = t if last_ts is None else max(last_ts, t)
            except Exception:
                continue
        if last_ts is None:
            return None
        return last_ts.normalize().to_period(WEEK_FREQ).start_time

    resolved_cfg, meta = load_config_and_meta(db, config_id) if db else (None, None)
    journeys_for_model = apply_model_config_to_journeys(journeys, (resolved_cfg.config_json or {}) if resolved_cfg else {}) if resolved_cfg else journeys
    kwargs = {}
    if model == "time_decay" and SETTINGS.attribution.time_decay_half_life_days:
        kwargs["half_life_days"] = SETTINGS.attribution.time_decay_half_life_days
    if model == "position_based":
        kwargs["first_pct"] = SETTINGS.attribution.position_first_pct
        kwargs["last_pct"] = SETTINGS.attribution.position_last_pct

    out = []
    for w in week_range:
        week_journeys = [
            j for j in journeys_for_model
            if j.get("converted", True) and _conversion_week(j) == w
        ]
        if not week_journeys:
            out.append({"date": w.strftime("%Y-%m-%d"), "attributed_value": 0.0})
            continue
        try:
            res = run_attribution(week_journeys, model=model, **kwargs)
            out.append({"date": w.strftime("%Y-%m-%d"), "attributed_value": float(res.get("total_value", 0) or 0)})
        except Exception:
            out.append({"date": w.strftime("%Y-%m-%d"), "attributed_value": 0.0})
    return out


@app.get("/api/attribution/weekly")
def get_attribution_weekly(
    model: str = Query(..., description="Attribution model id"),
    date_start: str = Query(..., description="ISO date start"),
    date_end: str = Query(..., description="ISO date end"),
    config_id: Optional[str] = Query(None),
    db=Depends(get_db),
):
    """Return attributed value (or conversions) per week for reconciliation with MMM KPI series."""
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db, limit=50000)
    series = _attribution_weekly_series(JOURNEYS, model, date_start, date_end, config_id, db)
    return {"model": model, "config_id": config_id, "series": series}


@app.get("/api/attribution/paths")
def get_path_analysis(
    config_id: Optional[str] = None,
    direct_mode: str = "include",
    path_scope: str = "converted",
    db=Depends(get_db),
):
    """Analyze conversion paths for common patterns and statistics. Includes next-best-action recommendations per path prefix (channel and optionally campaign level).

    Optional config_id pins to a specific model config; response includes config metadata.
    """
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")

    resolved_cfg, meta = load_config_and_meta(db, config_id)
    journeys_for_analysis = JOURNEYS
    if resolved_cfg:
        journeys_for_analysis = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})

    # Apply lightweight view filters that affect only the analysis, not the
    # underlying stored journeys.
    direct_mode_normalized = (direct_mode or "include").lower()
    if direct_mode_normalized not in ("include", "exclude"):
        direct_mode_normalized = "include"

    if direct_mode_normalized == "exclude":
        filtered_journeys = []
        for j in journeys_for_analysis:
            tps = j.get("touchpoints", [])
            kept = [tp for tp in tps if tp.get("channel", "").lower() != "direct"]
            if not kept:
                # If all touchpoints were direct, drop the journey from this view
                continue
            j2 = dict(j)
            j2["touchpoints"] = kept
            filtered_journeys.append(j2)
        journeys_for_analysis = filtered_journeys

    include_non_converted = (path_scope or "converted").lower() in ("all", "all_journeys", "include_non_converted")

    path_analysis = analyze_paths(journeys_for_analysis, include_non_converted=include_non_converted)

    # NBA recommendations (channel level)
    nba_channel_raw = compute_next_best_action(journeys_for_analysis, level="channel")
    filtered_channel, _channel_stats = _filter_nba_recommendations(
        nba_channel_raw, SETTINGS.nba
    )
    path_analysis["next_best_by_prefix"] = filtered_channel

    # NBA recommendations (campaign level, optional)
    if has_any_campaign(journeys_for_analysis):
        nba_campaign_raw = compute_next_best_action(
            journeys_for_analysis, level="campaign"
        )
        filtered_campaign, _campaign_stats = _filter_nba_recommendations(
            nba_campaign_raw, SETTINGS.nba
        )
        path_analysis["next_best_by_prefix_campaign"] = filtered_campaign
    path_analysis["config"] = meta
    path_analysis["view_filters"] = {
        "direct_mode": direct_mode_normalized,
        "path_scope": "all" if include_non_converted else "converted",
    }
    path_analysis["nba_config"] = SETTINGS.nba.model_dump()
    return path_analysis


@app.get("/api/conversion-paths/analysis")
def get_conversion_paths_analysis_aggregated(
    definition_id: Optional[str] = Query(None, description="Optional journey definition id"),
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    direct_mode: str = Query("include", description="include|exclude"),
    path_scope: str = Query("converted", description="converted|all"),
    channel_group: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None),
    device: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("attribution.view")),
):
    d_from = None
    d_to = None
    try:
        if date_from:
            d_from = datetime.fromisoformat(date_from).date()
        if date_to:
            d_to = datetime.fromisoformat(date_to).date()
    except Exception:
        raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
    if d_from and d_to and d_from > d_to:
        raise HTTPException(status_code=400, detail="date_from must be <= date_to")
    return build_conversion_paths_analysis_from_daily(
        db,
        definition_id=definition_id,
        date_from=d_from,
        date_to=d_to,
        direct_mode=(direct_mode or "include").lower(),
        path_scope=(path_scope or "converted").lower(),
        channel_group=channel_group,
        campaign_id=campaign_id,
        device=device,
        country=country,
        nba_config=SETTINGS.nba.model_dump(),
    )


@app.get("/api/paths/details")
def get_path_details(
    path: str,
    config_id: Optional[str] = None,
    direct_mode: str = "include",
    path_scope: str = "converted",
    db=Depends(get_db),
):
    """
    Return drilldown details for a single conversion path.

    This is a read-only view that reuses the same journey set and view filters
    as /api/attribution/paths so that counts, shares, and diagnostics line up
    with the main table.
    """
    if not path:
        raise HTTPException(status_code=400, detail="path is required")

    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")

    resolved_cfg, meta = load_config_and_meta(db, config_id)
    journeys_for_analysis = JOURNEYS
    if resolved_cfg:
        journeys_for_analysis = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})

    direct_mode_normalized = (direct_mode or "include").lower()
    if direct_mode_normalized not in ("include", "exclude"):
        direct_mode_normalized = "include"

    if direct_mode_normalized == "exclude":
        filtered_journeys = []
        for j in journeys_for_analysis:
            tps = j.get("touchpoints", [])
            kept = [tp for tp in tps if tp.get("channel", "").lower() != "direct"]
            if not kept:
                continue
            j2 = dict(j)
            j2["touchpoints"] = kept
            filtered_journeys.append(j2)
        journeys_for_analysis = filtered_journeys

    include_non_converted = (path_scope or "converted").lower() in ("all", "all_journeys", "include_non_converted")

    # Build basic universe stats for share calculations
    journeys_universe = journeys_for_analysis if include_non_converted else [j for j in journeys_for_analysis if j.get("converted", True)]
    if not journeys_universe:
        raise HTTPException(status_code=400, detail="No journeys for this view.")

    target_steps = [s for s in path.split(" > ") if s]

    matching_journeys = []
    for j in journeys_universe:
        steps = [_step_string(tp, "channel") for tp in j.get("touchpoints", [])]
        if " > ".join(steps) == path:
            matching_journeys.append(j)

    total_in_view = len(journeys_universe)
    count = len(matching_journeys)

    # Summary metrics
    avg_len = 0.0
    times: list[float] = []
    from datetime import datetime as _dt
    for j in matching_journeys:
        tps = j.get("touchpoints", [])
        if tps:
            avg_len += len(tps)
        if j.get("converted", True) and len(tps) >= 2:
            try:
                first_ts = pd.Timestamp(tps[0].get("timestamp", ""))
                last_ts = pd.Timestamp(tps[-1].get("timestamp", ""))
                if pd.notna(first_ts) and pd.notna(last_ts):
                    delta = (last_ts - first_ts).total_seconds() / 86400.0
                    if delta >= 0:
                        times.append(delta)
            except Exception:
                pass

    avg_len = avg_len / count if count else 0.0
    avg_time = sum(times) / len(times) if times else None

    # Step breakdown: for each position, estimate drop-off (journeys that stop after this step)
    step_breakdown = []
    if target_steps and count:
        step_count = len(target_steps)
        for idx, step in enumerate(target_steps):
            pos = idx + 1
            prefix = target_steps[:pos]
            prefix_matches = 0
            stops_here = 0
            for j in journeys_universe:
                steps = [_step_string(tp, "channel") for tp in j.get("touchpoints", [])]
                if len(steps) < pos:
                    continue
                if steps[:pos] == prefix:
                    prefix_matches += 1
                    if len(steps) == pos:
                        stops_here += 1
            dropoff_share = stops_here / prefix_matches if prefix_matches else 0.0
            step_breakdown.append(
                {
                    "step": step,
                    "position": pos,
                    "dropoff_share": round(dropoff_share, 4),
                    "prefix_journeys": prefix_matches,
                }
            )

    # Variant paths: top 5 most similar variants by shared prefix length
    from collections import defaultdict as _dd

    variant_counts: dict[str, int] = _dd(int)
    for j in journeys_universe:
        steps = [_step_string(tp, "channel") for tp in j.get("touchpoints", [])]
        candidate = " > ".join(steps)
        if candidate == path:
            continue
        variant_counts[candidate] += 1

    def _similarity_score(other: str) -> int:
        o_steps = [s for s in other.split(" > ") if s]
        score = 0
        for a, b in zip(target_steps, o_steps):
            if a == b:
                score += 2
            else:
                break
        # small bonus if same length, small penalty otherwise
        if len(o_steps) == len(target_steps):
            score += 1
        elif abs(len(o_steps) - len(target_steps)) == 1:
            score += 0
        else:
            score -= 1
        return score

    variants = sorted(
        [
            {
                "path": pth,
                "count": c,
                "share": round(c / total_in_view, 4),
            }
            for pth, c in variant_counts.items()
        ],
        key=lambda x: (_similarity_score(x["path"]), x["count"]),
        reverse=True,
    )[:5]

    # Data health for this path
    direct_unknown_touches = 0
    total_touches = 0
    journeys_ending_direct = 0
    for j in matching_journeys:
        tps = j.get("touchpoints", [])
        for tp in tps:
            ch = tp.get("channel", "unknown")
            total_touches += 1
            if ch.lower() in ("direct", "unknown"):
                direct_unknown_touches += 1
        if tps:
            last_ch = tps[-1].get("channel", "unknown")
            if last_ch and last_ch.lower() == "direct":
                journeys_ending_direct += 1

    direct_unknown_share = (
        direct_unknown_touches / total_touches if total_touches else 0.0
    )
    ending_direct_share = journeys_ending_direct / count if count else 0.0

    confidence = None
    if meta and meta.get("conversion_key"):
        snap = get_latest_quality_for_scope(
            db=db,
            scope="channel",
            scope_id=None,
            conversion_key=meta["conversion_key"],
        )
        if snap is not None:
            confidence = {
                "score": float(snap.confidence_score),
                "label": snap.confidence_label,
                "components": snap.components_json or {},
            }

    return {
        "path": path,
        "summary": {
            "count": count,
            "share": round(count / total_in_view, 4) if total_in_view else 0.0,
            "avg_touchpoints": round(avg_len, 2),
            "avg_time_to_convert_days": round(avg_time, 2) if avg_time is not None else None,
        },
        "step_breakdown": step_breakdown,
        "variants": variants,
        "data_health": {
            "direct_unknown_touch_share": round(direct_unknown_share, 4),
            "journeys_ending_direct_share": round(ending_direct_share, 4),
            "confidence": confidence,
        },
    }


@app.get("/api/conversion-paths/details")
def get_conversion_path_details_aggregated(
    path: str,
    definition_id: Optional[str] = Query(None, description="Optional journey definition id"),
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    direct_mode: str = Query("include", description="include|exclude"),
    path_scope: str = Query("converted", description="converted|all"),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("attribution.view")),
):
    if not path:
        raise HTTPException(status_code=400, detail="path is required")
    d_from = None
    d_to = None
    try:
        if date_from:
            d_from = datetime.fromisoformat(date_from).date()
        if date_to:
            d_to = datetime.fromisoformat(date_to).date()
    except Exception:
        raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
    if d_from and d_to and d_from > d_to:
        raise HTTPException(status_code=400, detail="date_from must be <= date_to")
    try:
        return build_conversion_path_details_from_daily(
            db,
            path=path,
            definition_id=definition_id,
            date_from=d_from,
            date_to=d_to,
            direct_mode=(direct_mode or "include").lower(),
            path_scope=(path_scope or "converted").lower(),
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/api/paths/archetypes")
def get_path_archetypes(
    conversion_key: Optional[str] = None,
    config_id: Optional[str] = None,
    k_mode: str = "auto",
    k: Optional[int] = None,
    k_min: int = 3,
    k_max: int = 10,
    direct_mode: str = "include",
    compare_previous: bool = False,
    recompute: bool = False,
    db=Depends(get_db),
):
    """Return simple path archetypes for current journeys."""
    global JOURNEYS, PATH_ARCHETYPES_CACHE
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")
    resolved_cfg, _meta = load_config_and_meta(db, config_id)
    journeys_for_analysis = JOURNEYS
    if resolved_cfg:
        journeys_for_analysis = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})
    # Apply direct handling as a view filter, similar to /api/attribution/paths.
    direct_mode_normalized = (direct_mode or "include").lower()
    if direct_mode_normalized not in ("include", "exclude"):
        direct_mode_normalized = "include"

    if direct_mode_normalized == "exclude":
        filtered_journeys = []
        for j in journeys_for_analysis:
            tps = j.get("touchpoints", [])
            kept = [tp for tp in tps if tp.get("channel", "").lower() != "direct"]
            if not kept:
                continue
            j2 = dict(j)
            j2["touchpoints"] = kept
            filtered_journeys.append(j2)
        journeys_for_analysis = filtered_journeys

    cache_key = (
        conversion_key or "",
        config_id or "",
        k_mode,
        int(k) if k is not None else None,
        int(k_min),
        int(k_max),
        direct_mode_normalized,
        bool(compare_previous),
        len(journeys_for_analysis),
    )

    if not recompute and cache_key in PATH_ARCHETYPES_CACHE:
        return PATH_ARCHETYPES_CACHE[cache_key]

    result = compute_path_archetypes(
        journeys_for_analysis,
        conversion_key,
        k_mode=k_mode,
        k=k,
        k_min=k_min,
        k_max=k_max,
        enable_stability=True,
        enable_compare_previous=bool(compare_previous),
    )
    # Attach view filters metadata for the frontend.
    result.setdefault("diagnostics", {})
    result["diagnostics"]["view_filters"] = {
        "direct_mode": direct_mode_normalized,
    }
    PATH_ARCHETYPES_CACHE[cache_key] = result
    return result


@app.get("/api/paths/anomalies")
def get_path_anomalies(conversion_key: Optional[str] = None, config_id: Optional[str] = None, db=Depends(get_db)):
    """Return simple anomaly hints for current journeys' paths."""
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        return {"anomalies": []}
    resolved_cfg, _meta = load_config_and_meta(db, config_id)
    journeys_for_analysis = JOURNEYS
    if resolved_cfg:
        journeys_for_analysis = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})
    anomalies = compute_path_anomalies(journeys_for_analysis, conversion_key)
    return {"anomalies": anomalies}


def _next_best_action_impl(path_so_far: str = "", level: str = "channel"):
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")
    prefix = path_so_far.strip().replace(",", " > ").replace("  ", " ").strip()
    use_level = "campaign" if level == "campaign" and has_any_campaign(JOURNEYS) else "channel"
    nba_raw = compute_next_best_action(JOURNEYS, level=use_level)
    filtered_map, _stats = _filter_nba_recommendations(nba_raw, SETTINGS.nba)
    recs = filtered_map.get(prefix, [])
    filtered = recs[:10]

    # Best-effort "why" samples: top continuation paths that start with the
    # requested prefix, summarised from raw journeys.
    why_samples = []
    if prefix:
        from collections import defaultdict as _dd

        prefix_steps = [s for s in prefix.split(" > ") if s]
        path_counts: dict[str, int] = _dd(int)
        direct_unknown_counts: dict[str, int] = _dd(int)
        total_for_prefix = 0

        for j in JOURNEYS:
            steps = [_step_string(tp, use_level) for tp in j.get("touchpoints", [])]
            if len(steps) < len(prefix_steps):
                continue
            if steps[: len(prefix_steps)] != prefix_steps:
                continue
            full_path = " > ".join(steps)
            path_counts[full_path] += 1
            total_for_prefix += 1
            for s in steps:
                if s.split(":", 1)[0].lower() in ("direct", "unknown"):
                    direct_unknown_counts[full_path] += 1

        if total_for_prefix:
            def _dk_share(p: str) -> float:
                dk = direct_unknown_counts.get(p, 0)
                length = len(p.split(" > ")) or 1
                return dk / length

            why_samples = sorted(
                [
                    {
                        "path": pth,
                        "count": cnt,
                        "share": round(cnt / total_for_prefix, 4),
                        "direct_unknown_share": round(_dk_share(pth), 4),
                    }
                    for pth, cnt in path_counts.items()
                ],
                key=lambda x: (x["count"], -x["direct_unknown_share"]),
                reverse=True,
            )[:3]

    return {
        "path_so_far": prefix or "(start)",
        "level": use_level,
        "recommendations": filtered,
        "why_samples": why_samples,
        "nba_config": SETTINGS.nba.model_dump(),
    }


@app.get("/api/attribution/next-best-action")
@app.get("/api/attribution/next_best_action")
def get_next_best_action(path_so_far: str = "", level: str = "channel", db=Depends(get_db)):
    """
    Given a path prefix (e.g. 'google_ads' or 'google_ads > email'), return recommended next channels
    (or channel:campaign when level=campaign and data has campaign). Use comma or ' > ' to separate steps.
    """
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")
    return _next_best_action_impl(path_so_far=path_so_far, level=level)

@app.get("/api/attribution/performance")
def get_channel_performance(model: str = "linear", config_id: Optional[str] = None, db=Depends(get_db)):
    """Get channel performance metrics combining attribution with expenses.

    Optional config_id pins to a specific model config; response includes config metadata.
    """
    # Resolve config (for now only surfaced in metadata, attribution math unchanged)
    resolved_cfg, meta = load_config_and_meta(db, config_id)
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    journeys_for_model = JOURNEYS
    if resolved_cfg:
        journeys_for_model = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})

    result = ATTRIBUTION_RESULTS.get(model)
    if not result:
        if not JOURNEYS:
            JOURNEYS = load_journeys_from_db(db)
        if not JOURNEYS:
            raise HTTPException(status_code=400, detail="No journeys loaded.")
        kwargs: Dict[str, Any] = {}
        if model == "time_decay":
            kwargs["half_life_days"] = SETTINGS.attribution.time_decay_half_life_days
        elif model == "position_based":
            kwargs["first_pct"] = SETTINGS.attribution.position_first_pct
            kwargs["last_pct"] = SETTINGS.attribution.position_last_pct
        result = run_attribution(journeys_for_model, model=model, **kwargs)
        ATTRIBUTION_RESULTS[model] = result

    expense_by_channel: Dict[str, float] = {}
    for exp in EXPENSES.values():
        if getattr(exp, "status", "active") == "deleted":
            continue
        converted = exp.converted_amount if getattr(exp, "converted_amount", None) is not None else exp.amount
        expense_by_channel[exp.channel] = expense_by_channel.get(exp.channel, 0) + converted

    performance = compute_channel_performance(result, expense_by_channel)

    # Attach per-channel confidence from latest quality snapshot (if available)
    for row in performance:
        snap = get_latest_quality_for_scope(
            db,
            scope="channel",
            scope_id=row["channel"],
            conversion_key=meta["conversion_key"] if meta else None,
        )
        if snap:
            row["confidence"] = {
                "score": snap.confidence_score,
                "label": snap.confidence_label,
                "components": snap.components_json,
            }
    return {
        "model": model,
        "channels": performance,
        "total_spend": sum(expense_by_channel.values()),
        "total_attributed_value": result.get("total_value", 0),
        "total_conversions": result.get("total_conversions", 0),
        "config": meta,
    }


@app.get("/api/attribution/campaign-performance")
def get_campaign_performance(
    model: str = "linear",
    config_id: Optional[str] = None,
    conversion_key: Optional[str] = None,
    db=Depends(get_db),
):
    """Campaign-level attribution (channel:campaign). Requires touchpoints with campaign. Returns campaigns with attributed value and optional suggested next (NBA)."""
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")
    if not has_any_campaign(JOURNEYS):
        return {
            "model": model,
            "campaigns": [],
            "total_conversions": 0,
            "total_value": 0,
            "message": "No campaign data in touchpoints. Add a 'campaign' field to use campaign performance.",
        }
    # Resolve config and apply to journeys
    resolved_cfg, meta = load_config_and_meta(db, config_id)
    journeys_for_model = JOURNEYS
    if resolved_cfg:
        journeys_for_model = apply_model_config_to_journeys(JOURNEYS, resolved_cfg.config_json or {})

    # Optional filter by conversion key (kpi_type on journeys).
    # If no explicit conversion_key is provided, fall back to model config's primary conversion when present.
    effective_conv = conversion_key or (meta.get("conversion_key") if meta else None)
    if effective_conv:
        journeys_for_model = [
            j for j in journeys_for_model if j.get("kpi_type") == effective_conv
        ]

    kwargs: Dict[str, Any] = {}
    if model == "time_decay":
        kwargs["half_life_days"] = SETTINGS.attribution.time_decay_half_life_days
    elif model == "position_based":
        kwargs["first_pct"] = SETTINGS.attribution.position_first_pct
        kwargs["last_pct"] = SETTINGS.attribution.position_last_pct
    result = run_attribution_campaign(journeys_for_model, model=model, **kwargs)
    expense_by_channel: Dict[str, float] = {}
    for exp in EXPENSES.values():
        if getattr(exp, "status", "active") == "deleted":
            continue
        converted = exp.converted_amount if getattr(exp, "converted_amount", None) is not None else exp.amount
        expense_by_channel[exp.channel] = expense_by_channel.get(exp.channel, 0) + converted

    # Mapping coverage: how much spend / value is mapped to known campaigns
    total_spend = sum(expense_by_channel.values())
    total_attributed_value = float(result.get("total_value", 0) or 0.0)
    mapped_spend = 0.0
    mapped_value = 0.0

    campaigns_list = []
    for ch in result.get("channels", []):
        step = ch["channel"]
        channel_name = step.split(":", 1)[0] if ":" in step else step
        campaign_name = step.split(":", 1)[1] if ":" in step else None
        spend = expense_by_channel.get(channel_name, 0)
        mapped_spend += spend
        attr_val = ch["attributed_value"]
        attr_conv = ch.get("attributed_conversions", 0)
        mapped_value += attr_val
        roi = ((attr_val - spend) / spend) if spend and spend > 0 else None  # ratio, e.g. 1.5 = 150%
        roas = (attr_val / spend) if spend and spend > 0 else None
        cpa = (spend / attr_conv) if spend and attr_conv and attr_conv > 0 else None
        campaigns_list.append({
            "campaign": step,
            "channel": channel_name,
            "campaign_name": campaign_name,
            "attributed_value": ch["attributed_value"],
            "attributed_share": ch["attributed_share"],
            "attributed_conversions": ch.get("attributed_conversions", 0),
            "spend": round(spend, 2),
            "roi": round(roi, 4) if roi is not None else None,
            "roas": round(roas, 2) if roas is not None else None,
            "cpa": round(cpa, 2) if cpa is not None else None,
        })

    # Attach NBA suggestion (campaign-level) and uplift vs synthetic holdout
    nba_campaign_raw = compute_next_best_action(JOURNEYS, level="campaign")
    nba_campaign, _nba_campaign_stats = _filter_nba_recommendations(
        nba_campaign_raw, SETTINGS.nba
    )
    uplift = compute_campaign_uplift(journeys_for_model)
    for c in campaigns_list:
        recs = nba_campaign.get(c["campaign"], [])
        c["suggested_next"] = recs[0] if recs else None
        u = uplift.get(c["campaign"])
        if u:
            c["treatment_rate"] = u["treatment_rate"]
            c["holdout_rate"] = u["holdout_rate"]
            c["uplift_abs"] = u["uplift_abs"]
            c["uplift_rel"] = u["uplift_rel"]
            c["treatment_n"] = u["treatment_n"]
            c["holdout_n"] = u["holdout_n"]

    # Attach campaign-level confidence where available
    for c in campaigns_list:
        snap = get_latest_quality_for_scope(
            db,
            scope="campaign",
            scope_id=c["campaign"],
            conversion_key=meta["conversion_key"] if meta else None,
        )
        if snap:
            c["confidence"] = {
                "score": snap.confidence_score,
                "label": snap.confidence_label,
                "components": snap.components_json,
            }
            c["confidence_score"] = snap.confidence_score

    coverage_spend_pct = (mapped_spend / total_spend * 100.0) if total_spend > 0 else 0.0
    coverage_value_pct = (mapped_value / total_attributed_value * 100.0) if total_attributed_value > 0 else 0.0

    return {
        "model": model,
        "campaigns": campaigns_list,
        "total_conversions": result.get("total_conversions", 0),
        "total_value": total_attributed_value,
        "total_spend": total_spend,
        "config": meta,
        "mapping_coverage": {
            "spend_mapped_pct": coverage_spend_pct,
            "value_mapped_pct": coverage_value_pct,
            "spend_mapped": mapped_spend,
            "spend_total": total_spend,
            "value_mapped": mapped_value,
            "value_total": total_attributed_value,
        },
    }


@app.get("/api/attribution/campaign-performance/trends")
def get_campaign_performance_trends(db=Depends(get_db)):
    """
    Time series for campaign performance:
      - transactions: number of converted journeys (last-touch attribution)
      - revenue: sum of conversion_value for those journeys

    Output:
      {
        \"campaigns\": [\"google_ads:Brand\", ...],
        \"dates\": [\"2024-01-01\", ...],
        \"series\": {
          \"google_ads:Brand\": [{\"date\": \"2024-01-01\", \"transactions\": 3, \"revenue\": 540.0}, ...],
          ...
        }
      }
    """
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")
    return compute_campaign_trends(JOURNEYS)


# ==================== Expense Management ====================

@app.get("/api/expenses")
def list_expenses(
    include_deleted: bool = Query(False),
    channel: Optional[str] = None,
    cost_type: Optional[str] = None,
    source_type: Optional[str] = None,
    currency: Optional[str] = None,
    status: Optional[str] = None,
    service_period_start: Optional[str] = None,
    service_period_end: Optional[str] = None,
    search: Optional[str] = None,
):
    """
    List expense entries with basic filtering.
    Soft-deleted records are excluded by default unless include_deleted=true.
    """
    items = []
    for expense_id, exp in EXPENSES.items():
        if not include_deleted and exp.status == "deleted":
            continue
        if channel and exp.channel != channel:
            continue
        if cost_type and exp.cost_type != cost_type:
            continue
        if source_type and exp.source_type != source_type:
            continue
        if currency and exp.currency != currency:
            continue
        if status and exp.status != status:
            continue
        if service_period_start and exp.service_period_start and exp.service_period_start < service_period_start:
            continue
        if service_period_end and exp.service_period_end and exp.service_period_end > service_period_end:
            continue
        if search:
            haystack = " ".join(
                filter(
                    None,
                    [
                        exp.notes,
                        exp.invoice_ref,
                        exp.external_link,
                    ],
                )
            ).lower()
            if search.lower() not in haystack:
                continue
        items.append({"id": expense_id, **exp.model_dump()})
    return items


@app.post("/api/expenses")
def add_expense(entry: ExpenseEntry):
    """
    Create a new expense entry.
    Optional change_note can be set on the entry for the audit trail.
    """
    expense_id = str(uuid.uuid4())
    now = _now_iso()
    entry.created_at = now
    entry.updated_at = now
    entry.actor_type = entry.actor_type or "manual"
    change_note = entry.change_note
    entry = _with_converted_amount(entry)
    EXPENSES[expense_id] = entry

    EXPENSE_AUDIT_LOG.append(
        ExpenseChangeEvent(
            expense_id=expense_id,
            timestamp=now,
            event_type="created",
            actor_type=entry.actor_type,
            note=change_note,
        )
    )
    return {"id": expense_id, **entry.model_dump()}


@app.patch("/api/expenses/{expense_id}")
def update_expense(expense_id: str, entry: ExpenseEntry):
    """
    Update an existing expense entry while preserving its audit history.
    Optional change_note can be set on the entry.
    """
    if expense_id not in EXPENSES:
        raise HTTPException(status_code=404, detail="Expense not found")
    existing = EXPENSES[expense_id]
    now = _now_iso()
    # Preserve created_at if already set
    entry.created_at = existing.created_at or now
    entry.updated_at = now
    entry.actor_type = entry.actor_type or "manual"
    change_note = entry.change_note
    entry = _with_converted_amount(entry)
    EXPENSES[expense_id] = entry

    EXPENSE_AUDIT_LOG.append(
        ExpenseChangeEvent(
            expense_id=expense_id,
            timestamp=now,
            event_type="updated",
            actor_type=entry.actor_type,
            note=change_note,
        )
    )
    return {"id": expense_id, **entry.model_dump()}


@app.delete("/api/expenses/{expense_id}")
def delete_expense(expense_id: str, change_note: Optional[str] = Query(None)):
    """
    Soft-delete an expense entry.
    """
    if expense_id not in EXPENSES:
        raise HTTPException(status_code=404, detail="Expense not found")
    entry = EXPENSES[expense_id]
    if entry.status == "deleted":
        return {"id": expense_id, **entry.model_dump()}

    now = _now_iso()
    entry.status = "deleted"
    entry.deleted_at = now
    entry.updated_at = now
    entry.change_note = change_note
    EXPENSES[expense_id] = entry

    EXPENSE_AUDIT_LOG.append(
        ExpenseChangeEvent(
            expense_id=expense_id,
            timestamp=now,
            event_type="deleted",
            actor_type=entry.actor_type or "manual",
            note=change_note,
        )
    )
    return {"id": expense_id, **entry.model_dump()}


@app.post("/api/expenses/{expense_id}/restore")
def restore_expense(expense_id: str, change_note: Optional[str] = Query(None)):
    """
    Restore a soft-deleted expense entry.
    """
    if expense_id not in EXPENSES:
        raise HTTPException(status_code=404, detail="Expense not found")
    entry = EXPENSES[expense_id]
    if entry.status != "deleted":
        return {"id": expense_id, **entry.model_dump()}

    now = _now_iso()
    entry.status = "active"
    entry.deleted_at = None
    entry.updated_at = now
    entry.change_note = change_note
    EXPENSES[expense_id] = entry

    EXPENSE_AUDIT_LOG.append(
        ExpenseChangeEvent(
            expense_id=expense_id,
            timestamp=now,
            event_type="restored",
            actor_type=entry.actor_type or "manual",
            note=change_note,
        )
    )
    return {"id": expense_id, **entry.model_dump()}


@app.get("/api/expenses/{expense_id}/audit")
def get_expense_audit(expense_id: str):
    """
    Return audit trail events for a single expense.
    """
    events = [e.model_dump() for e in EXPENSE_AUDIT_LOG if e.expense_id == expense_id]
    # Sort by timestamp ascending for timeline display
    events.sort(key=lambda e: e["timestamp"])
    return events


@app.get("/api/expenses/summary")
def expense_summary(
    include_deleted: bool = Query(False),
    service_period_start: Optional[str] = None,
    service_period_end: Optional[str] = None,
):
    """
    Get aggregated expense summary by channel.
    Uses converted_amount in reporting currency and can be filtered by service period.
    """
    by_channel: Dict[str, float] = {}
    total_manual = 0.0
    total_imported = 0.0
    total_unknown = 0.0
    reporting_currency = _default_reporting_currency()

    for exp in EXPENSES.values():
        if not include_deleted and getattr(exp, "status", "active") == "deleted":
            continue
        if service_period_start and exp.service_period_start and exp.service_period_start < service_period_start:
            continue
        if service_period_end and exp.service_period_end and exp.service_period_end > service_period_end:
            continue

        converted = exp.converted_amount if exp.converted_amount is not None else exp.amount
        by_channel[exp.channel] = by_channel.get(exp.channel, 0.0) + converted

        source_type = getattr(exp, "source_type", "manual")
        if source_type == "import":
            total_imported += converted
        elif source_type == "manual":
            total_manual += converted
        else:
            total_unknown += converted

    total = sum(by_channel.values())
    imported_share_pct = (total_imported / total * 100.0) if total > 0 else 0.0

    return {
        "by_channel": by_channel,
        "total": total,
        "channels": sorted(by_channel.keys()),
        "reporting_currency": reporting_currency,
        "imported_share_pct": imported_share_pct,
        "manual_total": total_manual,
        "imported_total": total_imported,
        "unknown_total": total_unknown,
    }


# ==================== Import Health & Reconciliation ====================

# Known expense sources that can be synced (aligned with connectors)
IMPORT_SOURCES = ["google_ads", "meta_ads", "linkedin_ads"]


def _import_status_from_state(state: Dict[str, Any]) -> str:
    """Derive Healthy / Stale / Broken / Partial from sync state."""
    if not state:
        return "unknown"
    status = state.get("status") or "unknown"
    if status in ("Healthy", "Stale", "Broken", "Partial"):
        return status
    # Infer from last success / attempt
    last_ok = state.get("last_success_at")
    last_attempt = state.get("last_attempt_at")
    err = state.get("last_error")
    if err:
        return "Broken"
    if last_ok:
        return "Healthy"
    if last_attempt:
        return "Partial"
    return "unknown"


@app.get("/api/imports/health")
def get_import_health():
    """
    Return import health per source: status, last sync times, records, error hint.
    Overall freshness is computed from worst source status.
    """
    result = []
    for source in IMPORT_SOURCES:
        state = IMPORT_SYNC_STATE.get(source, {})
        status = _import_status_from_state(state)
        result.append({
            "source": source,
            "status": status,
            "last_success_at": state.get("last_success_at"),
            "last_attempt_at": state.get("last_attempt_at"),
            "records_imported": state.get("records_imported"),
            "period_start": state.get("period_start"),
            "period_end": state.get("period_end"),
            "last_error": state.get("last_error"),
            "action_hint": state.get("action_hint"),
            "syncing": source in SYNC_IN_PROGRESS,
        })
    # Overall freshness: worst status among sources that have been attempted
    status_order = {"Broken": 3, "Stale": 2, "Partial": 1, "Healthy": 0, "unknown": -1}
    attempted = [r for r in result if r["last_attempt_at"]]
    overall = "Healthy"
    if attempted:
        worst = max(attempted, key=lambda r: status_order.get(r["status"], -1))
        overall = worst["status"] if worst["status"] != "unknown" else "Stale"
    return {"sources": result, "overall_freshness": overall}


@app.post("/api/imports/sync/{source}")
def trigger_sync(
    source: str,
    since: Optional[str] = Query(None),
    until: Optional[str] = Query(None),
):
    """
    Trigger a manual sync for the given source. Uses default date range if not provided.
    Returns 409 if a sync for this source is already running.
    """
    if source not in IMPORT_SOURCES:
        raise HTTPException(status_code=404, detail=f"Unknown source: {source}")
    if source in SYNC_IN_PROGRESS:
        raise HTTPException(status_code=409, detail="Sync already in progress for this source")

    from datetime import timedelta
    today = datetime.utcnow().date()
    if not since:
        since = (today - timedelta(days=30)).isoformat()
    if not until:
        until = today.isoformat()

    SYNC_IN_PROGRESS.add(source)
    now_iso = _now_iso()
    IMPORT_SYNC_STATE.setdefault(source, {})["last_attempt_at"] = now_iso
    IMPORT_SYNC_STATE[source]["period_start"] = since
    IMPORT_SYNC_STATE[source]["period_end"] = until
    IMPORT_SYNC_STATE[source]["last_error"] = None
    IMPORT_SYNC_STATE[source]["action_hint"] = None

    try:
        if source == "meta_ads":
            token_data = get_token("meta")
            access_token = token_data.get("access_token") if token_data else None
            if not access_token:
                access_token = get_access_token_for_provider(db, workspace_id="default", provider_key="meta_ads")
            if not access_token:
                raise HTTPException(status_code=401, detail="No Meta access token. Connect your Meta account first.")
            # Need ad_account_id - use first from config or a placeholder
            ad_account_id = ds_config.get_effective("meta", "ad_account_id") or "me"
            r = fetch_meta(ad_account_id=ad_account_id, since=since, until=until, access_token=access_token)
        elif source == "google_ads":
            r = fetch_google(segments_date_from=since, segments_date_to=until)
        elif source == "linkedin_ads":
            token_data = get_token("linkedin")
            access_token = token_data.get("access_token") if token_data else None
            if not access_token:
                access_token = get_access_token_for_provider(db, workspace_id="default", provider_key="linkedin_ads")
            if not access_token:
                raise HTTPException(status_code=401, detail="No LinkedIn access token. Connect your account first.")
            r = fetch_linkedin(since=since, until=until, access_token=access_token)
        else:
            r = {"rows": 0, "path": ""}

        rows = r.get("rows", 0)
        IMPORT_SYNC_STATE[source]["last_success_at"] = _now_iso()
        IMPORT_SYNC_STATE[source]["status"] = "Healthy"
        IMPORT_SYNC_STATE[source]["records_imported"] = rows
        platform_total = 0.0
        csv_name = {"meta_ads": "meta_ads.csv", "google_ads": "google_ads.csv", "linkedin_ads": "linkedin_ads.csv"}.get(source)
        if csv_name and DATA_DIR.joinpath(csv_name).exists():
            try:
                df = pd.read_csv(DATA_DIR / csv_name)
                platform_total = float(df["spend"].sum()) if "spend" in df.columns else 0.0
            except Exception:
                pass
        IMPORT_SYNC_STATE[source]["platform_total"] = platform_total
        return {"source": source, "status": "success", "rows": rows, "platform_total": platform_total}
    except HTTPException:
        raise
    except Exception as e:
        IMPORT_SYNC_STATE[source]["status"] = "Broken"
        IMPORT_SYNC_STATE[source]["last_error"] = str(e)
        IMPORT_SYNC_STATE[source]["action_hint"] = "Reconnect credentials or check connection."
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        SYNC_IN_PROGRESS.discard(source)


@app.get("/api/imports/reconciliation")
def get_reconciliation(
    service_period_start: Optional[str] = Query(None),
    service_period_end: Optional[str] = Query(None),
):
    """
    Per-source reconciliation for the selected service period: platform total, app normalized total, delta, status.
    """
    period_start = service_period_start or ""
    period_end = service_period_end or ""

    rows = []
    for source in IMPORT_SOURCES:
        state = IMPORT_SYNC_STATE.get(source, {})
        platform_total = state.get("platform_total") or 0.0
        # App normalized total = sum of expenses from this source in period (converted amount, active only)
        app_total = 0.0
        for exp in EXPENSES.values():
            if getattr(exp, "status", "active") == "deleted":
                continue
            if getattr(exp, "source_name", None) != source:
                continue
            if period_start and getattr(exp, "service_period_start", None) and exp.service_period_start < period_start:
                continue
            if period_end and getattr(exp, "service_period_end", None) and exp.service_period_end > period_end:
                continue
            app_total += exp.converted_amount if getattr(exp, "converted_amount", None) is not None else exp.amount

        delta = platform_total - app_total
        delta_pct = (delta / platform_total * 100.0) if platform_total else 0.0
        # Status: OK if delta within 1%, Warning 1-5%, Critical >5%
        if abs(delta_pct) <= 1.0:
            rec_status = "OK"
        elif abs(delta_pct) <= 5.0:
            rec_status = "Warning"
        else:
            rec_status = "Critical"

        rows.append({
            "source": source,
            "platform_total": platform_total,
            "app_normalized_total": app_total,
            "delta": delta,
            "delta_pct": delta_pct,
            "status": rec_status,
        })
    return {"period_start": period_start, "period_end": period_end, "rows": rows}


@app.get("/api/imports/reconciliation/drilldown")
def get_reconciliation_drilldown(
    source: str,
    service_period_start: Optional[str] = Query(None),
    service_period_end: Optional[str] = Query(None),
):
    """
    Lightweight drilldown: top missing days or campaigns (from stored CSV if available).
    """
    if source not in IMPORT_SOURCES:
        raise HTTPException(status_code=404, detail="Unknown source")
    out = {"source": source, "missing_days": [], "missing_campaigns": []}
    csv_name = f"{source}.csv".replace("google_ads", "google_ads").replace("meta_ads", "meta_ads").replace("linkedin_ads", "linkedin_ads")
    path = DATA_DIR / csv_name
    if path.exists():
        try:
            df = pd.read_csv(path)
            if "date" in df.columns and service_period_start and service_period_end:
                in_range = df[df["date"].astype(str).between(service_period_start, service_period_end)]
                if not in_range.empty:
                    out["missing_days"] = in_range["date"].astype(str).unique().tolist()[:10]
            if "campaign" in df.columns:
                out["missing_campaigns"] = df["campaign"].dropna().unique().tolist()[:20]
        except Exception:
            pass
    return out


# ==================== OAuth Routes ====================


@app.post("/api/auth/login")
def login_with_session(payload: AuthLoginPayload, response: Response, request: Request, db=Depends(get_db)):
    """
    Session login endpoint.
    - provider=local_password: username/email + password
    - provider=bootstrap: legacy dev bootstrap by email (no password)
    """
    provider = (payload.provider or "bootstrap").strip().lower()
    workspace_id = (payload.workspace_id or DEFAULT_WORKSPACE_ID).strip() or DEFAULT_WORKSPACE_ID
    if provider not in {"bootstrap", "local_password"}:
        raise HTTPException(status_code=400, detail="Unsupported auth provider")

    user = None
    if provider == "local_password":
        identifier = (payload.username or payload.email or "").strip().lower()
        if not identifier:
            raise HTTPException(status_code=400, detail="username or email is required")
        if not payload.password:
            raise HTTPException(status_code=400, detail="password is required")
        ensure_local_password_seed_users(db, workspace_id=workspace_id)
        user = authenticate_local_user(
            db,
            identifier=identifier,
            password=payload.password,
            workspace_id=workspace_id,
        )
        if not user:
            raise HTTPException(status_code=401, detail="Invalid credentials")
    else:
        email = (payload.email or "").strip()
        if not email:
            raise HTTPException(status_code=400, detail="email is required")
        try:
            user = ensure_user_and_membership(
                db,
                email=email,
                name=payload.name,
                workspace_id=workspace_id,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    session_tokens = create_session(
        db,
        user=user,
        workspace_id=workspace_id,
        request=request,
    )
    _set_session_cookie(response, session_tokens["session_id"])
    ctx = resolve_auth_context(db, raw_session_id=session_tokens["session_id"])
    if not ctx:
        raise HTTPException(status_code=500, detail="Failed to establish session")
    return {
        "user": {
            "id": ctx.user.id,
            "email": ctx.user.email,
            "name": ctx.user.name,
            "status": ctx.user.status,
        },
        "workspace": {
            "id": ctx.workspace.id,
            "name": ctx.workspace.name,
            "slug": ctx.workspace.slug,
        },
        "role": ctx.role.name if ctx.role else None,
        "permissions": sorted(ctx.permissions),
        "csrf_token": session_tokens["csrf_token"],
        "provider": provider,
    }


@app.get("/api/auth/providers")
def auth_providers():
    """Auth provider registry scaffold for future SSO/Google integrations."""
    return {
        "providers": [
            {"id": "local_password", "label": "Username & Password", "enabled": True},
            {"id": "google_oauth", "label": "Google", "enabled": False, "coming_soon": True},
            {"id": "sso_oidc", "label": "SSO (OIDC/SAML)", "enabled": False, "coming_soon": True},
        ]
    }


@app.get("/api/auth/me")
def get_auth_me(request: Request, db=Depends(get_db)):
    ctx = require_auth_context(db, request)
    csrf_token = issue_csrf_token(db, request.cookies.get(SESSION_COOKIE_NAME))
    return {
        "authenticated": True,
        "user": {
            "id": ctx.user.id,
            "username": getattr(ctx.user, "username", None),
            "email": ctx.user.email,
            "name": ctx.user.name,
            "status": ctx.user.status,
            "last_login_at": ctx.user.last_login_at,
        },
        "workspace": {
            "id": ctx.workspace.id,
            "name": ctx.workspace.name,
            "slug": ctx.workspace.slug,
        },
        "membership": {
            "id": ctx.membership.id,
            "status": ctx.membership.status,
            "role_id": ctx.membership.role_id,
            "role_name": ctx.role.name if ctx.role else None,
        },
        "permissions": sorted(ctx.permissions),
        "csrf_token": csrf_token,
    }


@app.post("/api/auth/workspace")
def switch_auth_workspace(
    payload: AuthWorkspaceSwitchPayload,
    request: Request,
    response: Response,
    db=Depends(get_db),
):
    current_ctx = require_auth_context(db, request)
    membership = (
        db.query(WorkspaceMembership)
        .filter(
            WorkspaceMembership.workspace_id == payload.workspace_id,
            WorkspaceMembership.user_id == current_ctx.user.id,
            WorkspaceMembership.status == "active",
        )
        .first()
    )
    if not membership:
        raise HTTPException(status_code=403, detail="No active membership for requested workspace")
    revoke_session(db, request.cookies.get(SESSION_COOKIE_NAME) or "")
    tokens = create_session(
        db,
        user=current_ctx.user,
        workspace_id=payload.workspace_id,
        request=request,
    )
    _set_session_cookie(response, tokens["session_id"])
    next_ctx = resolve_auth_context(db, raw_session_id=tokens["session_id"])
    if not next_ctx:
        raise HTTPException(status_code=500, detail="Failed to switch workspace")
    return {
        "workspace": {
            "id": next_ctx.workspace.id,
            "name": next_ctx.workspace.name,
            "slug": next_ctx.workspace.slug,
        },
        "role": next_ctx.role.name if next_ctx.role else None,
        "permissions": sorted(next_ctx.permissions),
        "csrf_token": tokens["csrf_token"],
    }


@app.post("/api/auth/logout")
def logout_session(request: Request, response: Response, db=Depends(get_db)):
    raw = request.cookies.get(SESSION_COOKIE_NAME)
    if raw:
        revoke_session(db, raw)
    _clear_session_cookie(response)
    return {"ok": True}


@app.post("/api/auth/logout-all")
def logout_all_sessions(request: Request, response: Response, db=Depends(get_db)):
    ctx = require_auth_context(db, request)
    revoked = revoke_all_user_sessions(db, ctx.user.id)
    _clear_session_cookie(response)
    return {"ok": True, "sessions_revoked": revoked}

@app.get("/api/auth/status")
def auth_status():
    connected = get_all_connected_platforms()
    if meiro_cdp.is_connected() and "meiro_cdp" not in connected:
        connected.append("meiro_cdp")
    return {"connected": connected}

def _resolve_workspace_user_from_request(request: Request, db) -> Tuple[str, str]:
    raw_session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if raw_session_id:
        ctx = require_auth_context(db, request)
        return ctx.workspace.id, ctx.user.id
    workspace_id = (request.headers.get("X-Workspace-Id") or request.query_params.get("workspace_id") or "default").strip() or "default"
    user_id = (request.headers.get("X-User-Id") or request.query_params.get("user_id") or "system").strip() or "system"
    return workspace_id, user_id


def _oauth_redirect_uri(provider_key: str) -> str:
    return f"{BASE_URL}/oauth/{provider_key}/callback"


class OAuthStartPayload(BaseModel):
    return_url: Optional[str] = None


class OAuthSelectAccountsPayload(BaseModel):
    account_ids: List[str] = Field(default_factory=list)


@app.get("/api/connections")
def api_list_connections(
    request: Request,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.view")),
):
    workspace_id, _user_id = _resolve_workspace_user_from_request(request, db)
    return list_oauth_connections(db, workspace_id=workspace_id)


@app.post("/api/connections/{provider}/start")
def api_start_connection(
    provider: str,
    body: OAuthStartPayload = Body(default=OAuthStartPayload()),
    request: Request = None,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    provider_key = normalize_provider_key(provider)
    if provider_key not in OAUTH_PROVIDER_LABELS:
        raise HTTPException(status_code=404, detail="Unsupported provider")
    workspace_id, user_id = _resolve_workspace_user_from_request(request, db)
    try:
        session_data = create_oauth_session(
            db,
            workspace_id=workspace_id,
            user_id=user_id,
            provider_key=provider_key,
            return_url=body.return_url,
        )
        auth_url = build_authorization_url(
            provider_key=provider_key,
            state=session_data["state"],
            code_challenge=session_data["code_challenge"],
            redirect_uri=_oauth_redirect_uri(provider_key),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"authorization_url": auth_url}


@app.post("/api/connections/{provider}/reauth")
def api_reauth_connection(
    provider: str,
    body: OAuthStartPayload = Body(default=OAuthStartPayload()),
    request: Request = None,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    return api_start_connection(provider=provider, body=body, request=request, db=db)


@app.get("/oauth/{provider}/callback")
def oauth_callback(provider: str, code: Optional[str] = Query(None), state: Optional[str] = Query(None), error: Optional[str] = Query(None), db=Depends(get_db)):
    provider_key = normalize_provider_key(provider)
    if provider_key not in OAUTH_PROVIDER_LABELS:
        return RedirectResponse(url=f"{FRONTEND_URL}/datasources?oauth_error=unsupported_provider")

    if error:
        return RedirectResponse(url=f"{FRONTEND_URL}/datasources?oauth_provider={provider_key}&oauth_error={error}")
    if not code or not state:
        return RedirectResponse(url=f"{FRONTEND_URL}/datasources?oauth_provider={provider_key}&oauth_error=missing_code_or_state")
    try:
        connection, return_url, normalized_error = complete_oauth_callback(
            db,
            provider_key=provider_key,
            code=code,
            state=state,
            redirect_uri=_oauth_redirect_uri(provider_key),
        )
        if normalized_error:
            message = normalized_error.message.replace(" ", "+")
            return RedirectResponse(
                url=f"{FRONTEND_URL}/datasources?oauth_provider={provider_key}&oauth_status=error&oauth_error={normalized_error.code}&oauth_message={message}"
            )
        redirect_base = f"{FRONTEND_URL}/datasources"
        if return_url and (return_url.startswith("/") or return_url.startswith(FRONTEND_URL)):
            redirect_base = f"{FRONTEND_URL}{return_url}" if return_url.startswith("/") else return_url
        return RedirectResponse(
            url=f"{redirect_base}?oauth_provider={provider_key}&oauth_status=connected&accounts={len((connection.config_json or {}).get('available_accounts') or [])}"
        )
    except Exception:
        return RedirectResponse(url=f"{FRONTEND_URL}/datasources?oauth_provider={provider_key}&oauth_status=error&oauth_error=callback_failed")


@app.get("/api/connections/{provider}/accounts")
def api_connection_accounts(
    provider: str,
    request: Request,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.view")),
):
    provider_key = normalize_provider_key(provider)
    workspace_id, _user_id = _resolve_workspace_user_from_request(request, db)
    try:
        accounts = list_provider_accounts(db, workspace_id=workspace_id, provider_key=provider_key)
        return {"provider_key": provider_key, "accounts": accounts}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/connections/{provider}/select-accounts")
def api_connection_select_accounts(
    provider: str,
    body: OAuthSelectAccountsPayload,
    request: Request,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    provider_key = normalize_provider_key(provider)
    workspace_id, _user_id = _resolve_workspace_user_from_request(request, db)
    try:
        out = select_accounts(
            db,
            workspace_id=workspace_id,
            provider_key=provider_key,
            account_ids=body.account_ids,
        )
        return out
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/api/connections/{provider}/test")
def api_connection_test(
    provider: str,
    request: Request,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    provider_key = normalize_provider_key(provider)
    workspace_id, _user_id = _resolve_workspace_user_from_request(request, db)
    try:
        return test_connection_health(db, workspace_id=workspace_id, provider_key=provider_key)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/api/connections/{provider}/disconnect")
def api_connection_disconnect(
    provider: str,
    request: Request,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    provider_key = normalize_provider_key(provider)
    workspace_id, _user_id = _resolve_workspace_user_from_request(request, db)
    try:
        out = disconnect_connection(db, workspace_id=workspace_id, provider_key=provider_key)
        return {"ok": True, "connection": out}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.delete("/api/auth/{platform}")
def disconnect_platform(platform: str):
    if delete_token(platform):
        return {"message": f"Disconnected {platform}"}
    raise HTTPException(status_code=404, detail=f"No connection found for {platform}")


# ==================== Data Source Administration ====================

class DatasourceCredentialUpdate(BaseModel):
    """Update OAuth credentials for a platform. Only provided fields are updated."""
    platform: str  # "google" | "meta" | "linkedin"
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    developer_token: Optional[str] = None  # Google Ads only
    app_id: Optional[str] = None      # Meta only
    app_secret: Optional[str] = None   # Meta only


@app.get("/api/admin/datasource-config")
def get_datasource_config_status(
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    """Return which platforms have credentials configured (no secret values)."""
    return ds_config.get_status()


@app.post("/api/admin/datasource-config")
def update_datasource_config(
    body: DatasourceCredentialUpdate,
    _ctx: PermissionContext = Depends(require_permission("settings.manage")),
):
    """Store OAuth credentials for a platform. Encrypted at rest. Env vars override if set."""
    platform = body.platform.lower()
    if platform not in ("google", "meta", "linkedin"):
        raise HTTPException(status_code=400, detail="platform must be google, meta, or linkedin")
    try:
        if platform == "google":
            if body.client_id is not None:
                ds_config.set_stored("google", client_id=body.client_id)
            if body.client_secret is not None:
                ds_config.set_stored("google", client_secret=body.client_secret)
            if body.developer_token is not None:
                ds_config.set_stored("google", developer_token=body.developer_token)
        elif platform == "meta":
            if body.app_id is not None:
                ds_config.set_stored("meta", app_id=body.app_id)
            if body.app_secret is not None:
                ds_config.set_stored("meta", app_secret=body.app_secret)
        elif platform == "linkedin":
            if body.client_id is not None:
                ds_config.set_stored("linkedin", client_id=body.client_id)
            if body.client_secret is not None:
                ds_config.set_stored("linkedin", client_secret=body.client_secret)
        return {"message": "Credentials updated", "platform": platform, "configured": ds_config.get_platform_configured(platform)}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/auth/{platform}")
def start_oauth(platform: str, request: Request, db=Depends(get_db)):
    provider_key = normalize_provider_key(platform)
    if provider_key not in OAUTH_PROVIDER_LABELS:
        raise HTTPException(status_code=400, detail=f"Unknown platform: {platform}")
    workspace_id, user_id = _resolve_workspace_user_from_request(request, db)
    try:
        session_data = create_oauth_session(
            db,
            workspace_id=workspace_id,
            user_id=user_id,
            provider_key=provider_key,
            return_url="/datasources",
        )
        auth_url = build_authorization_url(
            provider_key=provider_key,
            state=session_data["state"],
            code_challenge=session_data["code_challenge"],
            redirect_uri=_oauth_redirect_uri(provider_key),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return RedirectResponse(url=auth_url)


class AdminUserUpdatePayload(BaseModel):
    status: str = Field(..., pattern="^(active|disabled)$")


class AdminMembershipUpdatePayload(BaseModel):
    role_id: str


class AdminInvitationCreatePayload(BaseModel):
    email: str
    role_id: str
    workspace_id: Optional[str] = None
    expires_in_days: int = Field(default=7, ge=1, le=30)


class AdminRoleCreatePayload(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    description: Optional[str] = None
    permission_keys: List[str] = Field(default_factory=list)
    workspace_id: Optional[str] = None


class AdminRoleUpdatePayload(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=128)
    description: Optional[str] = None
    permission_keys: Optional[List[str]] = None


class InvitationAcceptPayload(BaseModel):
    token: str
    name: Optional[str] = None


@app.get("/api/admin/permissions")
def admin_list_permissions(
    category: Optional[str] = Query(None),
    _ctx: PermissionContext = Depends(require_permission("roles.manage")),
    db=Depends(get_db),
):
    q = db.query(ORMPermission)
    if category:
        q = q.filter(ORMPermission.category == category)
    rows = q.order_by(ORMPermission.category.asc(), ORMPermission.key.asc()).all()
    return [
        {"key": r.key, "description": r.description, "category": r.category}
        for r in rows
    ]


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


@app.get("/api/admin/audit-log")
def admin_list_audit_log(
    workspaceId: Optional[str] = Query(None),
    action: Optional[str] = Query(None),
    actor: Optional[str] = Query(None, description="Actor name/email contains"),
    date_from: Optional[str] = Query(None, description="ISO datetime"),
    date_to: Optional[str] = Query(None, description="ISO datetime"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _ctx: PermissionContext = Depends(require_any_permission(["audit.view", "settings.manage"])),
    db=Depends(get_db),
):
    if not getattr(SETTINGS.feature_flags, "audit_log_enabled", False):
        raise HTTPException(status_code=404, detail="audit_log_enabled flag is off")
    workspace_id = _workspace_scope_or_403(_ctx, workspaceId)
    from_dt = _parse_admin_audit_datetime(date_from, field_name="date_from")
    to_dt = _parse_admin_audit_datetime(date_to, field_name="date_to")
    if from_dt and to_dt and from_dt > to_dt:
        raise HTTPException(status_code=400, detail="date_from must be <= date_to")

    q = (
        db.query(
            ORMSecurityAuditLog,
            ORMUser.name.label("actor_name"),
            ORMUser.email.label("actor_email"),
        )
        .outerjoin(ORMUser, ORMUser.id == ORMSecurityAuditLog.actor_user_id)
        .filter(ORMSecurityAuditLog.workspace_id == workspace_id)
    )

    if action:
        action_like = action.strip()
        if action_like:
            q = q.filter(ORMSecurityAuditLog.action_key.ilike(f"%{action_like}%"))
    if actor:
        actor_like = actor.strip()
        if actor_like:
            q = q.filter(
                (ORMUser.name.ilike(f"%{actor_like}%"))
                | (ORMUser.email.ilike(f"%{actor_like}%"))
            )
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
    items = []
    for row, actor_name, actor_email in rows:
        items.append(
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
        )
    return {
        "items": items,
        "page": page,
        "page_size": page_size,
        "total": total,
    }


@app.get("/api/admin/roles")
def admin_list_roles(
    workspaceId: Optional[str] = Query(None),
    _ctx: PermissionContext = Depends(require_permission("roles.manage")),
    db=Depends(get_db),
):
    workspace_id = _workspace_scope_or_403(_ctx, workspaceId)
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
            p[0]
            for p in db.query(ORMRolePermission.permission_key)
            .filter(ORMRolePermission.role_id == role.id)
            .all()
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


@app.post("/api/admin/roles")
def admin_create_role(
    body: AdminRoleCreatePayload,
    request: Request,
    _ctx: PermissionContext = Depends(require_permission("roles.manage")),
    db=Depends(get_db),
):
    if not SETTINGS.feature_flags.custom_roles_enabled:
        raise HTTPException(status_code=403, detail="custom_roles_enabled flag is off")
    workspace_id = _workspace_scope_or_403(_ctx, body.workspace_id)
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    exists = (
        db.query(ORMRole)
        .filter(ORMRole.workspace_id == workspace_id, ORMRole.name == name)
        .first()
    )
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
    _write_security_audit(
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


@app.put("/api/admin/roles/{role_id}")
def admin_update_role(
    role_id: str,
    body: AdminRoleUpdatePayload,
    request: Request,
    _ctx: PermissionContext = Depends(require_permission("roles.manage")),
    db=Depends(get_db),
):
    if not SETTINGS.feature_flags.custom_roles_enabled:
        raise HTTPException(status_code=403, detail="custom_roles_enabled flag is off")
    role = db.get(ORMRole, role_id)
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    workspace_id = _workspace_scope_or_403(_ctx, role.workspace_id or _ctx.workspace_id)
    if role.is_system:
        raise HTTPException(status_code=400, detail="System roles are read-only")
    if body.name is not None:
        next_name = body.name.strip()
        if not next_name:
            raise HTTPException(status_code=400, detail="name is required")
        dupe = (
            db.query(ORMRole)
            .filter(
                ORMRole.workspace_id == workspace_id,
                ORMRole.name == next_name,
                ORMRole.id != role.id,
            )
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
    _write_security_audit(
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


@app.delete("/api/admin/roles/{role_id}")
def admin_delete_role(
    role_id: str,
    request: Request,
    _ctx: PermissionContext = Depends(require_permission("roles.manage")),
    db=Depends(get_db),
):
    if not SETTINGS.feature_flags.custom_roles_enabled:
        raise HTTPException(status_code=403, detail="custom_roles_enabled flag is off")
    role = db.get(ORMRole, role_id)
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    workspace_id = _workspace_scope_or_403(_ctx, role.workspace_id or _ctx.workspace_id)
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
    _write_security_audit(
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


@app.get("/api/admin/users")
def admin_list_users(
    search: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    workspaceId: Optional[str] = Query(None),
    _ctx: PermissionContext = Depends(require_permission("users.manage")),
    db=Depends(get_db),
):
    workspace_id = _workspace_scope_or_403(_ctx, workspaceId)
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
        q = q.filter(
            func.lower(ORMUser.email).like(term) | func.lower(func.coalesce(ORMUser.name, "")).like(term)
        )
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


@app.patch("/api/admin/users/{user_id}")
def admin_update_user(
    user_id: str,
    body: AdminUserUpdatePayload,
    request: Request,
    _ctx: PermissionContext = Depends(require_permission("users.manage")),
    db=Depends(get_db),
):
    user = db.get(ORMUser, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    membership = (
        db.query(WorkspaceMembership)
        .filter(
            WorkspaceMembership.workspace_id == _ctx.workspace_id,
            WorkspaceMembership.user_id == user.id,
        )
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
    _write_security_audit(
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


@app.post("/api/admin/users/{user_id}/reset-sessions")
def admin_reset_user_sessions(
    user_id: str,
    request: Request,
    _ctx: PermissionContext = Depends(require_permission("users.manage")),
    db=Depends(get_db),
):
    user = db.get(ORMUser, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    membership = (
        db.query(WorkspaceMembership)
        .filter(
            WorkspaceMembership.workspace_id == _ctx.workspace_id,
            WorkspaceMembership.user_id == user.id,
        )
        .first()
    )
    if not membership:
        raise HTTPException(status_code=404, detail="User is not a member of this workspace")
    revoked = revoke_all_user_sessions(db, user.id)
    _write_security_audit(
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


@app.get("/api/admin/memberships")
def admin_list_memberships(
    workspaceId: Optional[str] = Query(None),
    _ctx: PermissionContext = Depends(require_permission("users.manage")),
    db=Depends(get_db),
):
    workspace_id = _workspace_scope_or_403(_ctx, workspaceId)
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


@app.patch("/api/admin/memberships/{membership_id}")
def admin_update_membership(
    membership_id: str,
    body: AdminMembershipUpdatePayload,
    request: Request,
    _ctx: PermissionContext = Depends(require_permission("users.manage")),
    db=Depends(get_db),
):
    membership = db.get(WorkspaceMembership, membership_id)
    if not membership:
        raise HTTPException(status_code=404, detail="Membership not found")
    workspace_id = _workspace_scope_or_403(_ctx, membership.workspace_id)
    role = db.get(ORMRole, body.role_id)
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    if role.workspace_id not in (None, workspace_id):
        raise HTTPException(status_code=400, detail="Role is not available in this workspace")
    membership.role_id = role.id
    membership.updated_at = datetime.utcnow()
    db.add(membership)
    db.commit()
    _write_security_audit(
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


@app.delete("/api/admin/memberships/{membership_id}")
def admin_remove_membership(
    membership_id: str,
    request: Request,
    _ctx: PermissionContext = Depends(require_permission("users.manage")),
    db=Depends(get_db),
):
    membership = db.get(WorkspaceMembership, membership_id)
    if not membership:
        raise HTTPException(status_code=404, detail="Membership not found")
    workspace_id = _workspace_scope_or_403(_ctx, membership.workspace_id)
    membership.status = "removed"
    membership.role_id = None
    membership.updated_at = datetime.utcnow()
    db.add(membership)
    db.commit()
    _write_security_audit(
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


@app.post("/api/admin/invitations")
def admin_create_invitation(
    body: AdminInvitationCreatePayload,
    request: Request,
    _ctx: PermissionContext = Depends(require_permission("users.manage")),
    db=Depends(get_db),
):
    workspace_id = _workspace_scope_or_403(_ctx, body.workspace_id)
    email = (body.email or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="email is required")
    role = db.get(ORMRole, body.role_id)
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    if role.workspace_id not in (None, workspace_id):
        raise HTTPException(status_code=400, detail="Role is not available in this workspace")
    raw_token = _invite_token()
    inv = ORMInvitation(
        id=str(uuid.uuid4()),
        workspace_id=workspace_id,
        email=email,
        role_id=role.id,
        token_hash=_hash_invite_token(raw_token),
        expires_at=datetime.utcnow() + timedelta(days=body.expires_in_days),
        invited_by_user_id=_ctx.user_id,
        accepted_at=None,
        created_at=datetime.utcnow(),
    )
    db.add(inv)
    db.commit()
    _write_security_audit(
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


@app.get("/api/admin/invitations")
def admin_list_invitations(
    workspaceId: Optional[str] = Query(None),
    _ctx: PermissionContext = Depends(require_permission("users.manage")),
    db=Depends(get_db),
):
    workspace_id = _workspace_scope_or_403(_ctx, workspaceId)
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


@app.post("/api/admin/invitations/{invitation_id}/resend")
def admin_resend_invitation(
    invitation_id: str,
    request: Request,
    _ctx: PermissionContext = Depends(require_permission("users.manage")),
    db=Depends(get_db),
):
    inv = db.get(ORMInvitation, invitation_id)
    if not inv:
        raise HTTPException(status_code=404, detail="Invitation not found")
    workspace_id = _workspace_scope_or_403(_ctx, inv.workspace_id)
    raw_token = _invite_token()
    inv.token_hash = _hash_invite_token(raw_token)
    inv.expires_at = datetime.utcnow() + timedelta(days=7)
    inv.created_at = datetime.utcnow()
    db.add(inv)
    db.commit()
    _write_security_audit(
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


@app.delete("/api/admin/invitations/{invitation_id}/revoke")
def admin_revoke_invitation(
    invitation_id: str,
    request: Request,
    _ctx: PermissionContext = Depends(require_permission("users.manage")),
    db=Depends(get_db),
):
    inv = db.get(ORMInvitation, invitation_id)
    if not inv:
        raise HTTPException(status_code=404, detail="Invitation not found")
    workspace_id = _workspace_scope_or_403(_ctx, inv.workspace_id)
    db.delete(inv)
    db.commit()
    _write_security_audit(
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


@app.post("/api/invitations/accept")
def accept_invitation(
    body: InvitationAcceptPayload,
    request: Request,
    db=Depends(get_db),
):
    token = (body.token or "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="token is required")
    token_hash = _hash_invite_token(token)
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
        .filter(
            WorkspaceMembership.workspace_id == inv.workspace_id,
            WorkspaceMembership.user_id == user.id,
        )
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
    _write_security_audit(
        db,
        actor_user_id=user.id,
        workspace_id=inv.workspace_id,
        action_key="invitation.accepted",
        target_type="invitation",
        target_id=inv.id,
        metadata={"membership_id": membership.id},
        request=request,
    )
    return {
        "ok": True,
        "workspace_id": inv.workspace_id,
        "user_id": user.id,
        "membership_id": membership.id,
    }


# ==================== Data Quality API ====================


@app.post("/api/data-quality/run")
def run_data_quality(db=Depends(get_db)):
    """Compute a new DQ snapshot bucket and evaluate alert rules."""
    started = time.time()
    snaps = compute_dq_snapshots(db)
    alerts = evaluate_alert_rules(db)
    # Derive coarse confidence snapshots for channel/campaign scopes from latest DQ metrics
    quality_snaps = compute_overall_quality_from_dq(db)
    duration_ms = int((time.time() - started) * 1000)
    return {
        "snapshots_created": len(snaps),
        "alerts_created": len(alerts),
        "latest_bucket": snaps[0].ts_bucket.isoformat() if snaps else None,
        "quality_snapshots_created": len(quality_snaps),
        "duration_ms": duration_ms,
    }


@app.get("/api/data-quality/last-run")
def get_data_quality_last_run(db=Depends(get_db)):
    """Return last DQ run metadata (derived from latest snapshot bucket)."""
    row = db.query(DQSnapshot.ts_bucket).order_by(DQSnapshot.ts_bucket.desc()).first()
    if not row:
        return {"last_bucket": None, "has_data": False}
    return {"last_bucket": row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0]), "has_data": True}


# Drilldown definitions and recommended actions per metric
_DQ_DRILLDOWN = {
    "freshness_lag_minutes": {
        "definition": "How delayed the most recent event is per source. High lag means attribution windows may miss recent touchpoints.",
        "recommended_actions": [
            "Check Meiro event ingestion delay in Data Sources",
            "Verify ad platform connector sync schedule",
            "Review Expenses reconciliation timing",
        ],
    },
    "missing_profile_pct": {
        "definition": "Share of journeys without a profile/customer ID. Without IDs, paths cannot be joined to conversions.",
        "recommended_actions": [
            "Verify gclid/fbclid capture and consent on landing pages",
            "Check Meiro identity resolution and profile stitching",
            "Ensure event tracking includes customer_id or equivalent",
        ],
    },
    "missing_timestamp_pct": {
        "definition": "Share of journeys with no touchpoint timestamps. Without timestamps, windowing and path ordering break.",
        "recommended_actions": [
            "Ensure all events include timestamp or event_time",
            "Check UTM taxonomy mapping for server-side events",
            "Verify event enrichment pipelines add timestamps",
        ],
    },
    "duplicate_id_pct": {
        "definition": "Share of duplicate profile IDs across journeys. Duplicates can cause double-counting in attribution.",
        "recommended_actions": [
            "Review identity resolution and deduplication rules",
            "Check for multiple event streams emitting same profile_id",
            "Verify Conversion Paths deduplication logic",
        ],
    },
    "conversion_attributable_pct": {
        "definition": "Share of conversions with at least one eligible touchpoint. Low values mean many conversions cannot be attributed.",
        "recommended_actions": [
            "Review eligible touchpoints config in Model Configurator",
            "Fix UTM taxonomy mapping for new sources in Taxonomy",
            "Check Connectors for missing channel mapping",
        ],
    },
}


@app.get("/api/data-quality/drilldown")
def get_data_quality_drilldown(
    metric_key: str,
    source: Optional[str] = None,
    limit: int = 10,
    db=Depends(get_db),
):
    """Return drilldown: definition, breakdown by source, top offenders (if available), recommended actions."""
    info = _DQ_DRILLDOWN.get(metric_key, {})
    definition = info.get("definition", f" metric: {metric_key}")
    recommended_actions = info.get("recommended_actions", [])

    q = db.query(DQSnapshot).filter(DQSnapshot.metric_key == metric_key).order_by(DQSnapshot.ts_bucket.desc())
    if source:
        q = q.filter(DQSnapshot.source == source)
    rows = q.limit(limit * 3).all()
    by_source: Dict[str, float] = {}
    top_offenders: List[Dict[str, Any]] = []
    seen_sources = set()
    for r in rows:
        if r.source not in seen_sources:
            seen_sources.add(r.source)
            by_source[r.source] = r.metric_value
        if r.meta_json and isinstance(r.meta_json, dict):
            offenders = r.meta_json.get("top_offenders") or r.meta_json.get("top_campaigns")
            if offenders and not top_offenders:
                for i, o in enumerate(offenders[:10]):
                    if isinstance(o, dict):
                        top_offenders.append(o)
                    elif isinstance(o, (str, int, float)):
                        top_offenders.append({"key": str(o), "value": None})
    breakdown = [{"source": s, "value": v} for s, v in sorted(by_source.items(), key=lambda x: -x[1])[:limit]]
    return {
        "metric_key": metric_key,
        "definition": definition,
        "breakdown": breakdown,
        "top_offenders": top_offenders[:10] if top_offenders else [],
        "recommended_actions": recommended_actions,
    }


@app.get("/api/data-quality/snapshots")
def list_data_quality_snapshots(
    metric_key: Optional[str] = None,
    source: Optional[str] = None,
    ts_bucket_since: Optional[str] = None,
    ts_bucket_until: Optional[str] = None,
    limit: int = 200,
    db=Depends(get_db),
):
    q = db.query(DQSnapshot).order_by(DQSnapshot.ts_bucket.desc())
    if metric_key:
        q = q.filter(DQSnapshot.metric_key == metric_key)
    if source:
        q = q.filter(DQSnapshot.source == source)
    if ts_bucket_since:
        try:
            since = datetime.fromisoformat(ts_bucket_since.replace("Z", "+00:00"))
            q = q.filter(DQSnapshot.ts_bucket >= since)
        except ValueError:
            pass
    if ts_bucket_until:
        try:
            until = datetime.fromisoformat(ts_bucket_until.replace("Z", "+00:00"))
            q = q.filter(DQSnapshot.ts_bucket <= until)
        except ValueError:
            pass
    rows = q.limit(limit).all()
    return [
        {
            "id": r.id,
            "ts_bucket": r.ts_bucket.isoformat() if hasattr(r.ts_bucket, "isoformat") else str(r.ts_bucket),
            "source": r.source,
            "metric_key": r.metric_key,
            "metric_value": r.metric_value,
            "meta": r.meta_json,
        }
        for r in rows
    ]


@app.get("/api/data-quality/alerts")
def list_data_quality_alerts(
    status: Optional[str] = None,
    severity: Optional[str] = None,
    source: Optional[str] = None,
    limit: int = 100,
    db=Depends(get_db),
):
    q = db.query(DQAlert).order_by(DQAlert.triggered_at.desc())
    if status:
        q = q.filter(DQAlert.status == status)
    rows = q.limit(limit).all()
    rule_by_id = {
        r.id: r
        for r in db.query(DQAlertRule)
        .filter(DQAlertRule.id.in_({a.rule_id for a in rows}))
        .all()
    }
    out = []
    for a in rows:
        rule = rule_by_id.get(a.rule_id)
        if severity and rule and rule.severity != severity:
            continue
        if source and rule and rule.source != source:
            continue
        out.append(
            {
                "id": a.id,
                "rule_id": a.rule_id,
                "triggered_at": a.triggered_at.isoformat() if hasattr(a.triggered_at, "isoformat") else str(a.triggered_at),
                "ts_bucket": a.ts_bucket.isoformat() if hasattr(a.ts_bucket, "isoformat") else str(a.ts_bucket),
                "metric_value": a.metric_value,
                "baseline_value": a.baseline_value,
                "status": a.status,
                "message": a.message,
                "note": getattr(a, "note", None),
                "rule": {
                    "name": rule.name if rule else None,
                    "metric_key": rule.metric_key if rule else None,
                    "source": rule.source if rule else None,
                    "severity": rule.severity if rule else None,
                }
                if rule
                else None,
            }
        )
    return out


class AlertStatusUpdate(BaseModel):
    status: str  # open/acked/resolved


class AlertNoteUpdate(BaseModel):
    note: str


class PerformanceQueryContext(BaseModel):
    date_from: str
    date_to: str
    timezone: str
    currency: Optional[str] = None
    workspace: Optional[str] = None
    account: Optional[str] = None
    model_id: Optional[str] = None
    kpi_key: str = "revenue"
    grain: str = "auto"
    compare: bool = True
    channels: Optional[List[str]] = None
    conversion_key: Optional[str] = None
    current_period: Optional[Dict[str, Any]] = None
    previous_period: Optional[Dict[str, Any]] = None


def _normalize_channel_filter(channels: Optional[List[str]]) -> Optional[List[str]]:
    if not channels:
        return None
    normalized: List[str] = []
    for raw in channels:
        if raw is None:
            continue
        parts = [p.strip() for p in str(raw).split(",")]
        for part in parts:
            if not part:
                continue
            if part.lower() == "all":
                return None
            normalized.append(part)
    uniq_sorted = sorted(set(normalized))
    return uniq_sorted or None


def _build_performance_query_context(
    *,
    date_from: str,
    date_to: str,
    timezone: str,
    currency: Optional[str],
    workspace: Optional[str],
    account: Optional[str],
    model_id: Optional[str],
    kpi_key: str,
    grain: str,
    compare: bool,
    channels: Optional[List[str]],
    conversion_key: Optional[str],
) -> PerformanceQueryContext:
    windows = resolve_period_windows(date_from=date_from, date_to=date_to, grain=grain)
    return PerformanceQueryContext(
        date_from=windows["current_period"]["date_from"],
        date_to=windows["current_period"]["date_to"],
        timezone=(timezone or "UTC").strip() or "UTC",
        currency=currency,
        workspace=workspace,
        account=account,
        model_id=model_id,
        kpi_key=(kpi_key or "revenue").strip().lower(),
        grain=grain,
        compare=bool(compare),
        channels=_normalize_channel_filter(channels),
        conversion_key=(conversion_key.strip() if conversion_key else None),
        current_period=windows.get("current_period"),
        previous_period=windows.get("previous_period"),
    )


def _safe_zoneinfo(timezone_name: Optional[str]) -> ZoneInfo:
    try:
        return ZoneInfo((timezone_name or "UTC").strip() or "UTC")
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _local_date_from_ts(ts_value: Any, timezone_name: Optional[str]) -> Optional[date]:
    if not ts_value:
        return None
    try:
        dt = datetime.fromisoformat(str(ts_value).replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_safe_zoneinfo(timezone_name)).date()


def _compute_total_spend_for_period(
    *,
    expenses: Any,
    date_from: str,
    date_to: str,
    timezone_name: Optional[str],
    channels: Optional[List[str]],
) -> float:
    start_d = datetime.fromisoformat(date_from[:10]).date()
    end_d = datetime.fromisoformat(date_to[:10]).date()
    if end_d < start_d:
        start_d, end_d = end_d, start_d
    allowed = set(channels or [])
    total = 0.0
    records = expenses.values() if isinstance(expenses, dict) else (expenses or [])
    for exp in records:
        if isinstance(exp, dict):
            status = str(exp.get("status", "active"))
            channel = exp.get("channel")
            ts_raw = exp.get("service_period_start")
            amount = float(exp.get("converted_amount") or exp.get("amount") or 0.0)
        else:
            status = str(getattr(exp, "status", "active"))
            channel = getattr(exp, "channel", None)
            ts_raw = getattr(exp, "service_period_start", None)
            amount = float(getattr(exp, "converted_amount", None) or getattr(exp, "amount", 0.0) or 0.0)
        if status == "deleted" or not channel:
            continue
        channel = str(channel)
        if allowed and channel not in allowed:
            continue
        day = _local_date_from_ts(ts_raw, timezone_name)
        if day is None:
            continue
        if start_d <= day <= end_d:
            total += amount
    return total


def _compute_total_converted_value_for_period(
    *,
    journeys: List[Dict[str, Any]],
    date_from: str,
    date_to: str,
    timezone_name: Optional[str],
    channels: Optional[List[str]],
    conversion_key: Optional[str],
) -> float:
    start_d = datetime.fromisoformat(date_from[:10]).date()
    end_d = datetime.fromisoformat(date_to[:10]).date()
    if end_d < start_d:
        start_d, end_d = end_d, start_d
    allowed = set(channels or [])
    total = 0.0
    dedupe_seen: set[str] = set()
    for journey in journeys or []:
        if not journey.get("converted", True):
            continue
        if conversion_key:
            journey_key = str(journey.get("kpi_type") or journey.get("conversion_key") or "")
            if journey_key != conversion_key:
                continue
        touchpoints = journey.get("touchpoints") or []
        if not touchpoints:
            continue
        last_tp = touchpoints[-1] if isinstance(touchpoints[-1], dict) else {}
        channel = str(last_tp.get("channel") or "unknown")
        if allowed and channel not in allowed:
            continue
        day = _local_date_from_ts(last_tp.get("timestamp"), timezone_name)
        if day is None:
            continue
        if start_d <= day <= end_d:
            entries = journey.get("_revenue_entries")
            if isinstance(entries, list):
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    dedup_key = str(entry.get("dedup_key") or "")
                    if dedup_key:
                        if dedup_key in dedupe_seen:
                            continue
                        dedupe_seen.add(dedup_key)
                    try:
                        total += float(entry.get("value_in_base") or 0.0)
                    except Exception:
                        continue
            else:
                total += float(journey.get("conversion_value") or 0.0)
    return total


@app.post("/api/data-quality/alerts/{alert_id}/status")
def update_alert_status(alert_id: int, body: AlertStatusUpdate, db=Depends(get_db)):
    alert = db.get(DQAlert, alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    alert.status = body.status
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return {"id": alert.id, "status": alert.status}


@app.post("/api/data-quality/alerts/{alert_id}/note")
def update_alert_note(alert_id: int, body: AlertNoteUpdate, db=Depends(get_db)):
    alert = db.get(DQAlert, alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    alert.note = body.note
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return {"id": alert.id, "note": alert.note}


# ==================== Overview (Cover) Dashboard ====================


@app.get("/api/overview/summary")
def overview_summary(
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
    timezone: str = Query("UTC", description="Timezone for display"),
    currency: Optional[str] = Query(None, description="Filter/display currency"),
    workspace: Optional[str] = Query(None, description="Workspace filter"),
    account: Optional[str] = Query(None, description="Account filter"),
    model_id: Optional[str] = Query(None, description="Optional model config id (for metadata only)"),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("attribution.view")),
):
    """
    Overview summary: KPI tiles, highlights (what changed), freshness.
    Does not block on MMM/Incrementality; robust to missing data.
    """
    return get_overview_summary(
        db=db,
        date_from=date_from,
        date_to=date_to,
        timezone=timezone,
        currency=currency,
        workspace=workspace,
        account=account,
        model_id=model_id,
        expenses=EXPENSES,
        import_runs_get_last_successful=get_last_successful_run,
    )


@app.get("/api/overview/drivers")
def overview_drivers(
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
    top_campaigns_n: int = Query(10, ge=1, le=50, description="Top N campaigns"),
    conversion_key: Optional[str] = Query(None, description="Filter by conversion key"),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("attribution.view")),
):
    """
    Top drivers: by_channel (spend, conversions, revenue, delta), by_campaign (top N), biggest_movers.
    Uses raw paths + expenses; does not require MMM.
    """
    return get_overview_drivers(
        db=db,
        date_from=date_from,
        date_to=date_to,
        expenses=EXPENSES,
        top_campaigns_n=top_campaigns_n,
        conversion_key=conversion_key,
    )


@app.get("/api/performance/channel/trend")
def performance_channel_trend(
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
    timezone: str = Query("UTC", description="IANA timezone for bucketing"),
    currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
    workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
    account: Optional[str] = Query(None, description="Account filter (reserved)"),
    channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
    model_id: Optional[str] = Query(None, description="Optional model config id"),
    kpi_key: str = Query("revenue", description="spend|conversions|revenue|cpa|roas"),
    conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
    grain: str = Query("auto", description="auto|daily|weekly"),
    compare: bool = Query(True, description="Include previous-period series"),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("attribution.view")),
):
    """
    Channel performance trend for selected KPI and period.
    Returns current period series and optional previous period series.
    """
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    try:
        query_ctx = _build_performance_query_context(
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            currency=currency,
            workspace=workspace,
            account=account,
            model_id=model_id,
            kpi_key=kpi_key,
            grain=grain,
            compare=compare,
            channels=channels,
            conversion_key=conversion_key,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    try:
        out = build_channel_trend_response(
            journeys=JOURNEYS or [],
            expenses=EXPENSES,
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone=query_ctx.timezone,
            kpi_key=query_ctx.kpi_key,
            grain=query_ctx.grain,
            compare=query_ctx.compare,
            channels=query_ctx.channels,
            conversion_key=query_ctx.conversion_key,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    out["meta"] = {
        "workspace": query_ctx.workspace,
        "account": query_ctx.account,
        "model_id": query_ctx.model_id,
        "currency": query_ctx.currency,
        "kpi_key": query_ctx.kpi_key,
        "conversion_key": query_ctx.conversion_key,
        "timezone": query_ctx.timezone,
        "channels": query_ctx.channels or [],
        "query_context": {
            "current_period": query_ctx.current_period,
            "previous_period": query_ctx.previous_period,
            "grain": query_ctx.grain,
            "compare": query_ctx.compare,
        },
    }
    return out


@app.get("/api/performance/channel/summary")
def performance_channel_summary(
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
    timezone: str = Query("UTC", description="IANA timezone for bucketing"),
    currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
    workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
    account: Optional[str] = Query(None, description="Account filter (reserved)"),
    model_id: Optional[str] = Query(None, description="Optional model config id"),
    conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
    compare: bool = Query(True, description="Include previous-period summary"),
    channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("attribution.view")),
):
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    query_ctx = _build_performance_query_context(
        date_from=date_from,
        date_to=date_to,
        timezone=timezone,
        currency=currency,
        workspace=workspace,
        account=account,
        model_id=model_id,
        kpi_key="revenue",
        grain="daily",
        compare=compare,
        channels=channels,
        conversion_key=conversion_key,
    )
    _resolved_cfg, config_meta = load_config_and_meta(db, query_ctx.model_id)
    effective_conversion_key = query_ctx.conversion_key or (config_meta.get("conversion_key") if config_meta else None)
    out = build_channel_summary_response(
        journeys=JOURNEYS or [],
        expenses=EXPENSES,
        date_from=query_ctx.date_from,
        date_to=query_ctx.date_to,
        timezone=query_ctx.timezone,
        compare=query_ctx.compare,
        channels=query_ctx.channels,
        conversion_key=effective_conversion_key,
    )
    for item in out.get("items", []):
        channel_scope_id = item.get("channel")
        if not channel_scope_id:
            continue
        snap = get_latest_quality_for_scope(db, "channel", str(channel_scope_id), effective_conversion_key)
        if snap:
            item["confidence"] = {
                "score": snap.confidence_score,
                "label": snap.confidence_label,
                "components": snap.components_json,
            }
    mapped_spend = sum(float((row.get("current") or {}).get("spend", 0.0)) for row in out.get("items", []))
    mapped_value = sum(float((row.get("current") or {}).get("revenue", 0.0)) for row in out.get("items", []))
    spend_total = _compute_total_spend_for_period(
        expenses=EXPENSES,
        date_from=query_ctx.date_from,
        date_to=query_ctx.date_to,
        timezone_name=query_ctx.timezone,
        channels=query_ctx.channels,
    )
    value_total = _compute_total_converted_value_for_period(
        journeys=JOURNEYS or [],
        date_from=query_ctx.date_from,
        date_to=query_ctx.date_to,
        timezone_name=query_ctx.timezone,
        channels=query_ctx.channels,
        conversion_key=effective_conversion_key,
    )
    out["config"] = config_meta
    out["mapping_coverage"] = {
        "spend_mapped_pct": (mapped_spend / spend_total * 100.0) if spend_total > 0 else 0.0,
        "value_mapped_pct": (mapped_value / value_total * 100.0) if value_total > 0 else 0.0,
        "spend_mapped": mapped_spend,
        "spend_total": spend_total,
        "value_mapped": mapped_value,
        "value_total": value_total,
    }
    out["meta"] = {
        "workspace": query_ctx.workspace,
        "account": query_ctx.account,
        "model_id": query_ctx.model_id,
        "currency": query_ctx.currency,
        "timezone": query_ctx.timezone,
        "channels": query_ctx.channels or [],
        "conversion_key": effective_conversion_key,
        "query_context": {
            "current_period": query_ctx.current_period,
            "previous_period": query_ctx.previous_period,
            "compare": query_ctx.compare,
        },
    }
    return out


@app.get("/api/performance/campaign/trend")
def performance_campaign_trend(
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
    timezone: str = Query("UTC", description="IANA timezone for bucketing"),
    currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
    workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
    account: Optional[str] = Query(None, description="Account filter (reserved)"),
    channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
    model_id: Optional[str] = Query(None, description="Optional model config id"),
    kpi_key: str = Query("revenue", description="spend|conversions|revenue|cpa|roas"),
    conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
    grain: str = Query("auto", description="auto|daily|weekly"),
    compare: bool = Query(True, description="Include previous-period series"),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("attribution.view")),
):
    """
    Campaign performance trend for selected KPI and period.
    Returns current period series and optional previous period series.
    """
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    try:
        query_ctx = _build_performance_query_context(
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            currency=currency,
            workspace=workspace,
            account=account,
            model_id=model_id,
            kpi_key=kpi_key,
            grain=grain,
            compare=compare,
            channels=channels,
            conversion_key=conversion_key,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    try:
        out = build_campaign_trend_response(
            journeys=JOURNEYS or [],
            expenses=EXPENSES,
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone=query_ctx.timezone,
            kpi_key=query_ctx.kpi_key,
            grain=query_ctx.grain,
            compare=query_ctx.compare,
            channels=query_ctx.channels,
            conversion_key=query_ctx.conversion_key,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    out["meta"] = {
        "workspace": query_ctx.workspace,
        "account": query_ctx.account,
        "model_id": query_ctx.model_id,
        "currency": query_ctx.currency,
        "kpi_key": query_ctx.kpi_key,
        "conversion_key": query_ctx.conversion_key,
        "timezone": query_ctx.timezone,
        "channels": query_ctx.channels or [],
        "query_context": {
            "current_period": query_ctx.current_period,
            "previous_period": query_ctx.previous_period,
            "grain": query_ctx.grain,
            "compare": query_ctx.compare,
        },
    }
    return out


@app.get("/api/performance/campaign/summary")
def performance_campaign_summary(
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
    timezone: str = Query("UTC", description="IANA timezone for bucketing"),
    currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
    workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
    account: Optional[str] = Query(None, description="Account filter (reserved)"),
    model_id: Optional[str] = Query(None, description="Optional model config id"),
    conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
    compare: bool = Query(True, description="Include previous-period summary"),
    channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("attribution.view")),
):
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    query_ctx = _build_performance_query_context(
        date_from=date_from,
        date_to=date_to,
        timezone=timezone,
        currency=currency,
        workspace=workspace,
        account=account,
        model_id=model_id,
        kpi_key="revenue",
        grain="daily",
        compare=compare,
        channels=channels,
        conversion_key=conversion_key,
    )
    _resolved_cfg, config_meta = load_config_and_meta(db, query_ctx.model_id)
    effective_conversion_key = query_ctx.conversion_key or (config_meta.get("conversion_key") if config_meta else None)
    out = build_campaign_summary_response(
        journeys=JOURNEYS or [],
        expenses=EXPENSES,
        date_from=query_ctx.date_from,
        date_to=query_ctx.date_to,
        timezone=query_ctx.timezone,
        compare=query_ctx.compare,
        channels=query_ctx.channels,
        conversion_key=effective_conversion_key,
    )
    for item in out.get("items", []):
        scope_id = item.get("campaign_id")
        if not scope_id:
            continue
        snap = get_latest_quality_for_scope(db, "campaign", str(scope_id), effective_conversion_key)
        if snap:
            item["confidence"] = {
                "score": snap.confidence_score,
                "label": snap.confidence_label,
                "components": snap.components_json,
            }
    mapped_spend = sum(float((row.get("current") or {}).get("spend", 0.0)) for row in out.get("items", []))
    mapped_value = sum(float((row.get("current") or {}).get("revenue", 0.0)) for row in out.get("items", []))
    spend_total = _compute_total_spend_for_period(
        expenses=EXPENSES,
        date_from=query_ctx.date_from,
        date_to=query_ctx.date_to,
        timezone_name=query_ctx.timezone,
        channels=query_ctx.channels,
    )
    value_total = _compute_total_converted_value_for_period(
        journeys=JOURNEYS or [],
        date_from=query_ctx.date_from,
        date_to=query_ctx.date_to,
        timezone_name=query_ctx.timezone,
        channels=query_ctx.channels,
        conversion_key=effective_conversion_key,
    )
    out["config"] = config_meta
    out["mapping_coverage"] = {
        "spend_mapped_pct": (mapped_spend / spend_total * 100.0) if spend_total > 0 else 0.0,
        "value_mapped_pct": (mapped_value / value_total * 100.0) if value_total > 0 else 0.0,
        "spend_mapped": mapped_spend,
        "spend_total": spend_total,
        "value_mapped": mapped_value,
        "value_total": value_total,
    }
    out["meta"] = {
        "workspace": query_ctx.workspace,
        "account": query_ctx.account,
        "model_id": query_ctx.model_id,
        "currency": query_ctx.currency,
        "timezone": query_ctx.timezone,
        "channels": query_ctx.channels or [],
        "conversion_key": effective_conversion_key,
        "query_context": {
            "current_period": query_ctx.current_period,
            "previous_period": query_ctx.previous_period,
            "compare": query_ctx.compare,
        },
    }
    return out


@app.get("/api/overview/alerts")
def overview_alerts(
    scope: Optional[str] = Query(None, description="Filter by workspace/account scope"),
    status: Optional[str] = Query(None, description="Filter: open, resolved, ack"),
    limit: int = Query(50, ge=1, le=200, description="Max alerts to return"),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("alerts.view")),
):
    """
    Latest alert events (open + recent resolved) with deep links to drilldown.
    """
    try:
        return get_overview_alerts(db=db, scope=scope, status_filter=status, limit=limit)
    except Exception as e:
        logger.warning("Overview alerts failed (tables or schema may be missing): %s", e, exc_info=True)
        return {"alerts": [], "total": 0}


class OverviewAlertStatusUpdate(BaseModel):
    status: str = Field(..., description="open | ack | snoozed | resolved")
    snooze_until: Optional[str] = Field(None, description="ISO datetime when snooze expires (for status=snoozed)")


@app.post("/api/overview/alerts/{alert_id}/status")
def overview_alert_update_status(
    alert_id: int,
    body: OverviewAlertStatusUpdate,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("alerts.manage")),
):
    """Update overview alert event status (ack / snooze / resolve)."""
    ev = db.get(AlertEvent, alert_id)
    if not ev:
        raise HTTPException(status_code=404, detail="Alert not found")
    status = (body.status or "").strip().lower()
    if status not in ("open", "ack", "snoozed", "resolved"):
        raise HTTPException(status_code=400, detail="status must be one of: open, ack, snoozed, resolved")
    ev.status = status
    if status == "snoozed" and body.snooze_until:
        try:
            ev.snooze_until = datetime.fromisoformat(body.snooze_until.replace("Z", "+00:00"))
        except Exception:
            pass
    elif status != "snoozed":
        ev.snooze_until = None
    db.add(ev)
    db.commit()
    db.refresh(ev)
    return {"id": ev.id, "status": ev.status, "snooze_until": ev.snooze_until.isoformat() if ev.snooze_until else None}


# ==================== Alerts API (events + rules) ====================


class AlertSnoozeBody(BaseModel):
    duration_minutes: int = Field(..., ge=1, le=10080, description="Snooze duration in minutes (max 7 days)")


class AlertRuleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    is_enabled: bool = True
    scope: str = Field(..., min_length=1, max_length=64)
    severity: str = Field(..., pattern="^(info|warn|critical)$")
    rule_type: str = Field(..., pattern="^(anomaly_kpi|threshold|data_freshness|pipeline_health)$")
    kpi_key: Optional[str] = Field(None, max_length=128)
    dimension_filters_json: Optional[Dict[str, Any]] = None
    params_json: Optional[Dict[str, Any]] = None
    schedule: str = Field(..., pattern="^(hourly|daily)$")


class AlertRuleUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    is_enabled: Optional[bool] = None
    severity: Optional[str] = Field(None, pattern="^(info|warn|critical)$")
    kpi_key: Optional[str] = Field(None, max_length=128)
    dimension_filters_json: Optional[Dict[str, Any]] = None
    params_json: Optional[Dict[str, Any]] = None
    schedule: Optional[str] = Field(None, pattern="^(hourly|daily)$")


class JourneyDefinitionCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=5000)
    conversion_kpi_id: Optional[str] = Field(None, max_length=64)
    lookback_window_days: int = Field(30, ge=1, le=365)
    mode_default: str = Field("conversion_only", pattern="^(conversion_only|all_journeys)$")


class JourneyDefinitionUpdate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=5000)
    conversion_kpi_id: Optional[str] = Field(None, max_length=64)
    lookback_window_days: int = Field(..., ge=1, le=365)
    mode_default: str = Field(..., pattern="^(conversion_only|all_journeys)$")


class FunnelCreatePayload(BaseModel):
    journey_definition_id: str = Field(..., min_length=1, max_length=64)
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=5000)
    steps: List[str] = Field(..., min_length=2)
    counting_method: str = Field("ordered", pattern="^(ordered)$")
    window_days: int = Field(30, ge=1, le=365)
    workspace_id: Optional[str] = Field("default", max_length=128)


class JourneyAlertCreatePayload(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    type: str = Field(..., description="path_cr_drop | path_volume_change | funnel_dropoff_spike | ttc_shift")
    domain: str = Field(..., description="journeys | funnels")
    scope: Dict[str, Any] = Field(default_factory=dict)
    metric: str = Field(..., min_length=1, max_length=128)
    condition: Dict[str, Any] = Field(default_factory=dict)
    schedule: Optional[Dict[str, Any]] = None
    is_enabled: bool = True


class JourneyAlertUpdatePayload(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    condition: Optional[Dict[str, Any]] = None
    schedule: Optional[Dict[str, Any]] = None
    is_enabled: Optional[bool] = None


class JourneyAlertPreviewPayload(BaseModel):
    type: str = Field(..., description="path_cr_drop | path_volume_change | funnel_dropoff_spike | ttc_shift")
    scope: Dict[str, Any] = Field(default_factory=dict)
    metric: str = Field(..., min_length=1, max_length=128)
    condition: Dict[str, Any] = Field(default_factory=dict)


def _validate_conversion_kpi_id(conversion_kpi_id: Optional[str]) -> Optional[str]:
    value = (conversion_kpi_id or "").strip()
    if not value:
        return None
    available = {d.id for d in KPI_CONFIG.definitions}
    if value not in available:
        raise HTTPException(status_code=400, detail=f"conversion_kpi_id '{value}' is not defined in KPI config")
    return value


def _validate_journey_alert_payload(type: str, domain: str, metric: str, condition: Dict[str, Any]) -> None:
    if type not in JOURNEY_ALERT_TYPES:
        raise HTTPException(status_code=400, detail=f"type must be one of: {', '.join(sorted(JOURNEY_ALERT_TYPES))}")
    if domain not in JOURNEY_ALERT_DOMAINS:
        raise HTTPException(status_code=400, detail=f"domain must be one of: {', '.join(sorted(JOURNEY_ALERT_DOMAINS))}")
    metric_clean = (metric or "").strip()
    if not metric_clean:
        raise HTTPException(status_code=400, detail="metric is required")
    mode = str((condition or {}).get("comparison_mode") or "previous_period")
    if mode not in {"previous_period", "rolling_baseline"}:
        raise HTTPException(status_code=400, detail="condition.comparison_mode must be previous_period or rolling_baseline")


def _validate_journey_alert_scope_and_metric(
    type: str,
    scope: Dict[str, Any],
    metric: str,
) -> None:
    scope = scope or {}
    metric = (metric or "").strip()
    allowed_metrics = {
        "path_cr_drop": {"conversion_rate"},
        "path_volume_change": {"count_journeys"},
        "funnel_dropoff_spike": {"dropoff_rate"},
        "ttc_shift": {"p50_time_to_convert_sec"},
    }
    if metric not in allowed_metrics.get(type, set()):
        raise HTTPException(
            status_code=400,
            detail=f"metric '{metric}' is not valid for alert type '{type}'",
        )

    if type in {"path_cr_drop", "path_volume_change", "ttc_shift"}:
        journey_definition_id = str(scope.get("journey_definition_id") or "").strip()
        if not journey_definition_id:
            raise HTTPException(status_code=400, detail="scope.journey_definition_id is required")

    if type == "funnel_dropoff_spike":
        funnel_id = str(scope.get("funnel_id") or "").strip()
        if not funnel_id:
            raise HTTPException(status_code=400, detail="scope.funnel_id is required")
        try:
            step_index = int(scope.get("step_index"))
        except Exception:
            raise HTTPException(status_code=400, detail="scope.step_index is required")
        if step_index < 0:
            raise HTTPException(status_code=400, detail="scope.step_index must be >= 0")


@app.get("/api/journeys/definitions")
def api_list_journey_definitions(
    page: int = Query(1, ge=1, description="Page number"),
    per_page: Optional[int] = Query(None, description="Items per page"),
    page_size: Optional[int] = Query(None, description="Alias for per_page"),
    limit: Optional[int] = Query(None, description="Alias for per_page"),
    search: Optional[str] = Query(None, description="Search by name/description"),
    sort: Optional[str] = Query(None, description="Sort direction asc|desc"),
    order: Optional[str] = Query(None, description="Alias for sort direction asc|desc"),
    include_archived: bool = Query(False, description="Include archived definitions"),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("journeys.view")),
):
    """List journey definitions with pagination and search."""
    resolved_page = clamp_int(page, default=1, minimum=1, maximum=1_000_000)
    resolved_per_page = resolve_per_page(
        per_page=per_page,
        page_size=page_size,
        limit=limit,
        default=20,
        maximum=100,
    )
    resolved_sort_dir = resolve_sort_dir(sort=sort, order=order, default="desc")
    try:
        return list_journey_definitions(
            db,
            page=resolved_page,
            per_page=resolved_per_page,
            search=search,
            sort_dir=resolved_sort_dir,
            include_archived=include_archived,
        )
    except Exception as e:
        logger.warning("List journey definitions failed: %s", e, exc_info=True)
        return {"items": [], "total": 0, "page": resolved_page, "per_page": resolved_per_page}


@app.post("/api/journeys/definitions")
def api_create_journey_definition(
    body: JourneyDefinitionCreate,
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("journeys.manage")),
):
    """Create journey definition. Requires edit role."""
    user_id = ctx.user_id
    if not body.name.strip():
        raise HTTPException(status_code=400, detail="name is required")
    conversion_kpi_id = _validate_conversion_kpi_id(body.conversion_kpi_id)
    item = create_journey_definition(
        db,
        name=body.name,
        description=body.description,
        conversion_kpi_id=conversion_kpi_id,
        lookback_window_days=body.lookback_window_days,
        mode_default=body.mode_default,
        created_by=user_id,
    )
    return serialize_journey_definition(item)


@app.put("/api/journeys/definitions/{definition_id}")
def api_update_journey_definition(
    definition_id: str,
    body: JourneyDefinitionUpdate,
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("journeys.manage")),
):
    """Update journey definition. Requires edit role."""
    user_id = ctx.user_id
    if not body.name.strip():
        raise HTTPException(status_code=400, detail="name is required")
    conversion_kpi_id = _validate_conversion_kpi_id(body.conversion_kpi_id)
    item = update_journey_definition(
        db,
        definition_id,
        name=body.name,
        description=body.description,
        conversion_kpi_id=conversion_kpi_id,
        lookback_window_days=body.lookback_window_days,
        mode_default=body.mode_default,
        updated_by=user_id,
    )
    if not item:
        raise HTTPException(status_code=404, detail="Journey definition not found")
    return serialize_journey_definition(item)


@app.delete("/api/journeys/definitions/{definition_id}")
def api_delete_journey_definition(
    definition_id: str,
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("journeys.manage")),
):
    """Archive journey definition (soft delete). Requires edit role."""
    user_id = ctx.user_id
    item = archive_journey_definition(db, definition_id, archived_by=user_id)
    if not item:
        raise HTTPException(status_code=404, detail="Journey definition not found")
    return {"id": item.id, "status": "archived"}


@app.get("/api/journeys/{definition_id}/paths")
def api_get_journey_paths(
    definition_id: str,
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
    mode: str = Query("conversion_only", pattern="^(conversion_only|all_journeys)$"),
    channel_group: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None),
    device: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(50, ge=1, le=200, description="Items per page (max 200)"),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("journeys.view")),
):
    """Return pre-aggregated journey paths from journey_paths_daily only."""
    jd = get_journey_definition(db, definition_id)
    if not jd or jd.is_archived:
        raise HTTPException(status_code=404, detail="Journey definition not found")
    try:
        d_from = datetime.fromisoformat(date_from).date()
        d_to = datetime.fromisoformat(date_to).date()
    except Exception:
        raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
    if d_from > d_to:
        raise HTTPException(status_code=400, detail="date_from must be <= date_to")

    return list_paths_for_journey_definition(
        db,
        journey_definition_id=definition_id,
        date_from=d_from,
        date_to=d_to,
        mode=mode,
        channel_group=channel_group,
        campaign_id=campaign_id,
        device=device,
        country=country,
        page=page,
        limit=limit,
    )


@app.get("/api/journeys/{definition_id}/transitions")
def api_get_journey_transitions(
    definition_id: str,
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
    mode: str = Query("conversion_only", pattern="^(conversion_only|all_journeys)$"),
    channel_group: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None),
    device: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    min_count: int = Query(5, ge=1, le=100000),
    max_nodes: int = Query(20, ge=2, le=200),
    max_depth: int = Query(5, ge=1, le=20),
    group_other: bool = Query(True, description="Group rare steps into 'Other'"),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("journeys.view")),
):
    """Return Sankey-ready transition graph from journey_transitions_daily."""
    jd = get_journey_definition(db, definition_id)
    if not jd or jd.is_archived:
        raise HTTPException(status_code=404, detail="Journey definition not found")
    try:
        d_from = datetime.fromisoformat(date_from).date()
        d_to = datetime.fromisoformat(date_to).date()
    except Exception:
        raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
    if d_from > d_to:
        raise HTTPException(status_code=400, detail="date_from must be <= date_to")

    return list_transitions_for_journey_definition(
        db,
        journey_definition_id=definition_id,
        date_from=d_from,
        date_to=d_to,
        mode=mode,
        channel_group=channel_group,
        campaign_id=campaign_id,
        device=device,
        country=country,
        min_count=min_count,
        max_nodes=max_nodes,
        max_depth=max_depth,
        group_other=group_other,
    )


@app.get("/api/journeys/{definition_id}/attribution-summary")
def api_get_journey_attribution_summary(
    definition_id: str,
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
    model: str = Query("linear", description="Attribution model"),
    mode: str = Query("conversion_only", pattern="^(conversion_only|all_journeys)$"),
    channel_group: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None),
    device: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    path_hash: Optional[str] = Query(None),
    include_campaign: bool = Query(False, description="Include campaign-level credit split"),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("journeys.view")),
):
    """Return attribution credit split for a journey definition (and optional path hash)."""
    jd = get_journey_definition(db, definition_id)
    if not jd or jd.is_archived:
        raise HTTPException(status_code=404, detail="Journey definition not found")
    try:
        _ = datetime.fromisoformat(date_from).date()
        _ = datetime.fromisoformat(date_to).date()
    except Exception:
        raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
    try:
        return build_journey_attribution_summary(
            db,
            definition=jd,
            date_from=date_from,
            date_to=date_to,
            model=model,
            mode=mode,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
            path_hash=path_hash,
            include_campaign=include_campaign,
            settings_obj=SETTINGS,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/funnels")
def api_list_funnels(
    workspace_id: str = Query("default"),
    user_id: Optional[str] = Query(None),
    journey_definition_id: Optional[str] = Query(None),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("funnels.view")),
):
    """List non-archived funnel definitions for workspace/user."""
    return {
        "items": list_funnels(
            db,
            workspace_id=workspace_id,
            user_id=user_id,
            journey_definition_id=journey_definition_id,
        )
    }


@app.post("/api/funnels")
def api_create_funnel(
    body: FunnelCreatePayload,
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("funnels.manage")),
):
    """Create funnel definition. Requires edit role."""
    user_id = ctx.user_id
    jd = get_journey_definition(db, body.journey_definition_id)
    if not jd or jd.is_archived:
        raise HTTPException(status_code=404, detail="Journey definition not found")
    steps = [str(s).strip() for s in (body.steps or []) if str(s).strip()]
    if len(steps) < 2:
        raise HTTPException(status_code=400, detail="steps must include at least 2 items")
    return create_funnel(
        db,
        journey_definition_id=body.journey_definition_id,
        workspace_id=body.workspace_id or "default",
        user_id=user_id,
        name=body.name,
        description=body.description,
        steps=steps,
        counting_method=body.counting_method,
        window_days=body.window_days,
        actor=user_id,
    )


@app.get("/api/funnels/{funnel_id}/results")
def api_get_funnel_results(
    funnel_id: str,
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
    device: Optional[str] = Query(None),
    channel_group: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("funnels.view")),
):
    """Compute funnel results from transitions aggregates when possible, with raw fallback when needed."""
    funnel = get_funnel(db, funnel_id)
    if not funnel or funnel.is_archived:
        raise HTTPException(status_code=404, detail="Funnel not found")
    jd = get_journey_definition(db, funnel.journey_definition_id)
    if not jd or jd.is_archived:
        raise HTTPException(status_code=404, detail="Journey definition not found")
    try:
        d_from = datetime.fromisoformat(date_from).date()
        d_to = datetime.fromisoformat(date_to).date()
    except Exception:
        raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
    if d_from > d_to:
        raise HTTPException(status_code=400, detail="date_from must be <= date_to")
    return compute_funnel_results(
        db,
        funnel=funnel,
        journey_definition=jd,
        date_from=d_from,
        date_to=d_to,
        device=device,
        channel_group=channel_group,
        country=country,
        campaign_id=campaign_id,
    )


@app.get("/api/funnels/{funnel_id}/diagnostics")
def api_get_funnel_diagnostics(
    funnel_id: str,
    step: str = Query(..., description="Funnel step label to diagnose"),
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
    device: Optional[str] = Query(None),
    channel_group: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("funnels.view")),
):
    """Evidence-based hypotheses for funnel drop-off changes at a specific step."""
    funnel = get_funnel(db, funnel_id)
    if not funnel or funnel.is_archived:
        raise HTTPException(status_code=404, detail="Funnel not found")
    jd = get_journey_definition(db, funnel.journey_definition_id)
    if not jd or jd.is_archived:
        raise HTTPException(status_code=404, detail="Journey definition not found")
    try:
        d_from = datetime.fromisoformat(date_from).date()
        d_to = datetime.fromisoformat(date_to).date()
    except Exception:
        raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
    if d_from > d_to:
        raise HTTPException(status_code=400, detail="date_from must be <= date_to")
    return compute_funnel_diagnostics(
        db,
        funnel=funnel,
        journey_definition=jd,
        step=step,
        date_from=d_from,
        date_to=d_to,
        device=device,
        channel_group=channel_group,
        country=country,
        campaign_id=campaign_id,
    )


@app.get("/api/alerts")
def api_list_alerts(
    domain: Optional[str] = Query(None, description="journeys | funnels (new journey/funnel alerts domain)"),
    status: str = Query("open", description="open | all"),
    severity: Optional[str] = Query(None, description="Filter by severity"),
    rule_type: Optional[str] = Query(None, description="Filter by rule type"),
    search: Optional[str] = Query(None, description="Search in title, message, rule name"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: Optional[int] = Query(None, description="Items per page"),
    page_size: Optional[int] = Query(None, description="Alias for per_page"),
    limit: Optional[int] = Query(None, description="Alias for per_page"),
    scope: Optional[str] = Query(None, description="Filter by scope (workspace/account)"),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("alerts.view")),
):
    """List alerts with pagination and filters. All roles can view."""
    resolved_page = clamp_int(page, default=1, minimum=1, maximum=1_000_000)
    resolved_per_page = resolve_per_page(
        per_page=per_page,
        page_size=page_size,
        limit=limit,
        default=20,
        maximum=100,
    )
    try:
        if domain in JOURNEY_ALERT_DOMAINS:
            return list_journey_alert_definitions(
                db=db,
                domain=domain,
                page=resolved_page,
                per_page=resolved_per_page,
            )
        return list_alerts(
            db=db,
            status=status,
            severity=severity,
            rule_type=rule_type,
            search=search,
            page=resolved_page,
            per_page=resolved_per_page,
            scope=scope,
        )
    except Exception as e:
        logger.warning("List alerts failed (tables or schema may be missing): %s", e, exc_info=True)
        return {"items": [], "total": 0, "page": resolved_page, "per_page": resolved_per_page}


@app.get("/api/alerts/events")
def api_list_alert_events(
    domain: Optional[str] = Query(None, description="journeys | funnels"),
    page: int = Query(1, ge=1),
    per_page: Optional[int] = Query(None),
    page_size: Optional[int] = Query(None, description="Alias for per_page"),
    limit: Optional[int] = Query(None, description="Alias for per_page"),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("alerts.view")),
):
    """List recent journey/funnel alert events."""
    resolved_page = clamp_int(page, default=1, minimum=1, maximum=1_000_000)
    resolved_per_page = resolve_per_page(
        per_page=per_page,
        page_size=page_size,
        limit=limit,
        default=20,
        maximum=200,
    )
    if domain not in JOURNEY_ALERT_DOMAINS:
        raise HTTPException(status_code=400, detail="domain must be journeys or funnels")
    try:
        return list_journey_alert_events(db=db, domain=domain, page=resolved_page, per_page=resolved_per_page)
    except Exception as e:
        logger.warning("List journey alert events failed: %s", e, exc_info=True)
        return {"items": [], "total": 0, "page": resolved_page, "per_page": resolved_per_page}


@app.post("/api/alerts")
def api_create_alert(
    body: JourneyAlertCreatePayload,
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("alerts.manage")),
):
    """Create journey/funnel alert definition. Requires edit role."""
    user_id = ctx.user_id
    _validate_journey_alert_payload(body.type, body.domain, body.metric, body.condition)
    _validate_journey_alert_scope_and_metric(body.type, body.scope or {}, body.metric)
    return create_journey_alert_definition(
        db,
        name=body.name,
        type=body.type,
        domain=body.domain,
        scope=body.scope or {},
        metric=body.metric,
        condition=body.condition or {},
        schedule=body.schedule or {"cadence": "daily"},
        is_enabled=body.is_enabled,
        actor=user_id,
    )


@app.put("/api/alerts/{alert_definition_id}")
def api_update_alert(
    alert_definition_id: str,
    body: JourneyAlertUpdatePayload,
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("alerts.manage")),
):
    """Update journey/funnel alert definition fields. Requires edit role."""
    user_id = ctx.user_id
    out = update_journey_alert_definition(
        db,
        definition_id=alert_definition_id,
        actor=user_id,
        name=body.name,
        is_enabled=body.is_enabled,
        condition=body.condition,
        schedule=body.schedule,
    )
    if not out:
        raise HTTPException(status_code=404, detail="Alert definition not found")
    return out


@app.post("/api/alerts/preview")
def api_preview_alert(
    body: JourneyAlertPreviewPayload,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("alerts.view")),
):
    """Preview baseline/current values for alert authoring."""
    _validate_journey_alert_payload(body.type, "journeys" if body.type != "funnel_dropoff_spike" else "funnels", body.metric, body.condition)
    _validate_journey_alert_scope_and_metric(body.type, body.scope or {}, body.metric)
    return preview_journey_alert(
        db,
        type=body.type,
        scope=body.scope or {},
        metric=body.metric,
        condition=body.condition or {},
    )


@app.get("/api/alerts/{alert_id}")
def api_get_alert(
    alert_id: int,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("alerts.view")),
):
    """Alert detail including context_json, related_entities, deep_link (entity_type, entity_id, url)."""
    out = get_alert_by_id(db=db, alert_id=alert_id)
    if not out:
        raise HTTPException(status_code=404, detail="Alert not found")
    return out


@app.post("/api/alerts/{alert_id}/ack")
def api_ack_alert(
    alert_id: int,
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("alerts.manage")),
):
    """Acknowledge alert. Requires edit role (X-User-Role: admin or editor)."""
    user_id = ctx.user_id
    ev = ack_alert(db=db, alert_id=alert_id, user_id=user_id)
    if not ev:
        raise HTTPException(status_code=404, detail="Alert not found")
    return {"id": ev.id, "status": ev.status}


@app.post("/api/alerts/{alert_id}/snooze")
def api_snooze_alert(
    alert_id: int,
    body: AlertSnoozeBody,
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("alerts.manage")),
):
    """Snooze alert for duration_minutes. Requires edit role."""
    user_id = ctx.user_id
    ev = snooze_alert(db=db, alert_id=alert_id, duration_minutes=body.duration_minutes, user_id=user_id)
    if not ev:
        raise HTTPException(status_code=404, detail="Alert not found")
    return {"id": ev.id, "status": ev.status, "snooze_until": ev.snooze_until.isoformat() if getattr(ev, "snooze_until", None) and ev.snooze_until else None}


@app.post("/api/alerts/{alert_id}/resolve")
def api_resolve_alert(
    alert_id: int,
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("alerts.manage")),
):
    """Manually resolve alert. Requires edit role."""
    user_id = ctx.user_id
    ev = resolve_alert(db=db, alert_id=alert_id, user_id=user_id)
    if not ev:
        raise HTTPException(status_code=404, detail="Alert not found")
    return {"id": ev.id, "status": ev.status}


@app.get("/api/alert-rules")
def api_list_alert_rules(
    scope: Optional[str] = Query(None),
    is_enabled: Optional[bool] = Query(None, description="Filter by enabled"),
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("alerts.view")),
):
    """List alert rules. All roles can view."""
    try:
        return list_alert_rules(db=db, scope=scope, is_enabled=is_enabled)
    except Exception as e:
        logger.warning("List alert rules failed (tables or schema may be missing): %s", e, exc_info=True)
        return []


@app.post("/api/alert-rules")
def api_create_alert_rule(
    body: AlertRuleCreate,
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("alerts.manage")),
):
    """Create alert rule. Requires edit role. params_json validated per rule_type."""
    user_id = ctx.user_id
    try:
        r = create_alert_rule(
            db=db,
            name=body.name,
            scope=body.scope,
            severity=body.severity,
            rule_type=body.rule_type,
            schedule=body.schedule,
            created_by=user_id,
            description=body.description,
            is_enabled=body.is_enabled,
            kpi_key=body.kpi_key,
            dimension_filters_json=body.dimension_filters_json,
            params_json=body.params_json,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return get_alert_rule_by_id(db=db, rule_id=r.id)


@app.get("/api/alert-rules/{rule_id}")
def api_get_alert_rule(
    rule_id: int,
    db=Depends(get_db),
    _ctx: PermissionContext = Depends(require_permission("alerts.view")),
):
    """Get single alert rule."""
    out = get_alert_rule_by_id(db=db, rule_id=rule_id)
    if not out:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    return out


@app.put("/api/alert-rules/{rule_id}")
def api_update_alert_rule(
    rule_id: int,
    body: AlertRuleUpdate,
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("alerts.manage")),
):
    """Update alert rule. Requires edit role. params_json validated."""
    user_id = ctx.user_id
    try:
        r = update_alert_rule(
            db=db,
            rule_id=rule_id,
            updated_by=user_id,
            name=body.name,
            description=body.description,
            is_enabled=body.is_enabled,
            severity=body.severity,
            kpi_key=body.kpi_key,
            dimension_filters_json=body.dimension_filters_json,
            params_json=body.params_json,
            schedule=body.schedule,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not r:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    return get_alert_rule_by_id(db=db, rule_id=r.id)


@app.delete("/api/alert-rules/{rule_id}")
def api_delete_alert_rule(
    rule_id: int,
    disable_only: bool = Query(True, description="If true, disable rule; if false, delete"),
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("alerts.manage")),
):
    """Disable or delete alert rule. Requires edit role."""
    user_id = ctx.user_id
    ok = delete_alert_rule(db=db, rule_id=rule_id, updated_by=user_id, disable_only=disable_only)
    if not ok:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    return {"id": rule_id, "disabled": disable_only}


# ==================== Explainability API ====================


class ExplainabilityDriver(BaseModel):
    metric: str
    delta: float
    current_value: float
    previous_value: float
    top_contributors: List[Dict[str, Any]] = []
    contribution_pct: Optional[float] = None  # For attributed_value: this driver's share of total change


class FeatureImportanceItem(BaseModel):
    id: str
    name: str
    importance: float  # 0–1 share in current period
    share_pct: float
    delta: float
    direction: str  # "up" | "down" | "neutral"


class NarrativeBlock(BaseModel):
    summary: str
    period_notes: List[str] = []
    config_notes: List[str] = []
    data_notes: List[str] = []


class ChangeDecompositionBucket(BaseModel):
    key: str  # "volume" | "mix" | "rate"
    label: str
    contribution: float
    pct_of_total: Optional[float] = None
    is_estimated: bool = False


class ChangeDecomposition(BaseModel):
    total_delta: float
    buckets: List[ChangeDecompositionBucket] = []
    basis: str = "journeys"  # journeys / conversions
    is_estimated: bool = True


class ComparabilityWarning(BaseModel):
    severity: str  # info | warn | critical
    message: str
    action: Optional[str] = None


class ComparabilitySummary(BaseModel):
    rating: str  # high | medium | low
    warnings: List[ComparabilityWarning] = []


class ExplainabilitySummary(BaseModel):
    period: Dict[str, Any]
    drivers: List[ExplainabilityDriver]
    data_health: Dict[str, Any]
    config: Dict[str, Any]
    mechanics: Dict[str, Any]
    feature_importance: List[FeatureImportanceItem] = []
    narrative: Optional[NarrativeBlock] = None
    change_decomposition: Optional[ChangeDecomposition] = None
    comparability: Optional[ComparabilitySummary] = None
    config_diff: Dict[str, Any] = {}
    data_quality_delta: Dict[str, Any] = {}
    timeline: List[Dict[str, Any]] = []
    channel_breakdowns: Dict[str, Any] = {}


def _filter_journeys_by_period(journeys: List[Dict[str, Any]], start: datetime, end: datetime) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for j in journeys:
        tps = j.get("touchpoints", [])
        if not tps:
            continue
        ts = tps[-1].get("timestamp")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            try:
                dt = datetime.fromisoformat(str(ts).split("T")[0])
            except Exception:
                continue
        # Normalize timezone-aware timestamps to naive to match start/end.
        if dt.tzinfo is not None:
            dt = dt.astimezone(tz=None).replace(tzinfo=None)
        if start <= dt <= end:
            out.append(j)
    return out


def _build_explainability_narrative(
    scope: str,
    drivers: List[ExplainabilityDriver],
    curr_j_count: int,
    prev_j_count: int,
    config_info: Dict[str, Any],
    data_health: Dict[str, Any],
    mechanics: Dict[str, Any],
) -> NarrativeBlock:
    """Build human-readable narrative tied to config and data changes."""
    period_notes: List[str] = []
    if prev_j_count > 0 and curr_j_count != prev_j_count:
        pct = ((curr_j_count - prev_j_count) / prev_j_count) * 100.0
        period_notes.append(
            f"Journey count: current period {curr_j_count:,}, previous {prev_j_count:,} ({pct:+.1f}% change). "
            "Large volume shifts can affect attribution comparability."
        )
    elif curr_j_count == 0:
        period_notes.append("No journeys in the current period; metrics are not comparable.")
    else:
        period_notes.append(f"Current period: {curr_j_count:,} journeys; previous: {prev_j_count:,}.")

    config_notes: List[str] = []
    cfg_id = config_info.get("config_id")
    version = config_info.get("version")
    changes = config_info.get("changes") or []
    if cfg_id and version is not None:
        config_notes.append(f"Using measurement config '{cfg_id}' (version {version}).")
        if changes:
            config_notes.append(
                f"{len(changes)} config change(s) in the comparison window – "
                "recent changes to windows or conversion keys may affect period comparability."
            )
    else:
        config_notes.append("No versioned model config; global attribution settings apply.")

    windows = (mechanics.get("windows") or {}) or {}
    if windows.get("click_lookback_days") is not None:
        config_notes.append(f"Click lookback: {windows['click_lookback_days']} days.")

    data_notes: List[str] = []
    conf = (data_health or {}).get("confidence")
    if conf:
        data_notes.append(f"Data confidence: {conf.get('label', '—')} ({conf.get('score', 0):.0f}/100).")
    for note in (data_health or {}).get("notes") or []:
        data_notes.append(note)

    summary_parts: List[str] = []
    if scope in ("channel", "campaign") and drivers:
        d = drivers[0]
        delta_val = d.delta
        curr_val = d.current_value
        prev_val = d.previous_value
        if prev_val != 0:
            pct_change = (delta_val / prev_val) * 100.0
            summary_parts.append(
                f"Attributed value {'increased' if delta_val >= 0 else 'decreased'} by "
                f"{abs(delta_val):,.0f} ({pct_change:+.1f}%) from the previous period "
                f"(from {prev_val:,.0f} to {curr_val:,.0f})."
            )
        else:
            summary_parts.append(f"Attributed value is {curr_val:,.0f} (previous period had no value).")
        if getattr(d, "top_contributors", None) and len(d.top_contributors) > 0:
            top = d.top_contributors[0]
            summary_parts.append(f" Top positive driver: {top.get('id', '—')} (+{top.get('delta', 0):,.0f}).")
    elif scope == "paths" and drivers:
        parts = []
        for d in drivers:
            if abs(d.delta) > 0.01:
                parts.append(f"{d.metric}: {d.delta:+.2f} (now {d.current_value:.2f})")
        if parts:
            summary_parts.append("Path metrics changed: " + "; ".join(parts) + ".")
        else:
            summary_parts.append("Path metrics are stable between periods.")
    else:
        summary_parts.append("Compare the two periods above; narrative is based on journey volume and config.")

    return NarrativeBlock(
        summary=" ".join(summary_parts).strip() or "No summary available.",
        period_notes=period_notes,
        config_notes=config_notes,
        data_notes=data_notes,
    )


def _compute_change_decomposition(
    curr_val: float,
    prev_val: float,
    curr_j_count: int,
    prev_j_count: int,
) -> Optional[ChangeDecomposition]:
    """
    Best-effort decomposition of attributed value delta into
    volume vs mix vs rate/value effects.

    We treat journey count as the volume proxy and value per journey
    as the rate/value proxy. Mix is the residual.
    """
    delta_val = curr_val - prev_val
    if abs(delta_val) < 1e-9:
        return ChangeDecomposition(
            total_delta=0.0,
            buckets=[],
            basis="journeys",
            is_estimated=True,
        )

    # If we do not have both journey counts, we cannot decompose reliably.
    if curr_j_count <= 0 or prev_j_count <= 0:
        bucket = ChangeDecompositionBucket(
            key="rate",
            label="Rate / value",
            contribution=delta_val,
            pct_of_total=100.0,
            is_estimated=True,
        )
        return ChangeDecomposition(
            total_delta=delta_val,
            buckets=[bucket],
            basis="journeys",
            is_estimated=True,
        )

    prev_avg = prev_val / prev_j_count if prev_j_count > 0 else 0.0
    curr_avg = curr_val / curr_j_count if curr_j_count > 0 else 0.0

    # Simple two-factor volume × rate decomposition, treat mix as residual.
    volume_effect = (curr_j_count - prev_j_count) * prev_avg
    rate_effect = curr_j_count * (curr_avg - prev_avg)
    mix_effect = delta_val - volume_effect - rate_effect

    buckets = [
        ChangeDecompositionBucket(
            key="volume",
            label="Volume (journeys / conversions)",
            contribution=round(volume_effect, 2),
            is_estimated=True,
        ),
        ChangeDecompositionBucket(
            key="mix",
            label="Mix (channel share shift)",
            contribution=round(mix_effect, 2),
            is_estimated=True,
        ),
        ChangeDecompositionBucket(
            key="rate",
            label="Rate / value per journey",
            contribution=round(rate_effect, 2),
            is_estimated=True,
        ),
    ]

    # Compute contribution % of each bucket.
    for b in buckets:
        b.pct_of_total = (b.contribution / delta_val * 100.0) if abs(delta_val) > 1e-9 else None

    return ChangeDecomposition(
        total_delta=round(delta_val, 2),
        buckets=buckets,
        basis="journeys",
        is_estimated=True,
    )


@app.get("/api/explainability/summary", response_model=ExplainabilitySummary)
def explainability_summary(
    scope: str = Query(..., pattern="^(channel|campaign|paths)$"),
    scope_id: Optional[str] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    config_id: Optional[str] = None,
    conversion_key: Optional[str] = None,
    model: str = "linear",
    db=Depends(get_db),
):
    """
    Explainability: period-over-period drivers, feature importance, and narrative.

    - scope=channel: attributed value by channel; feature importance = share of value + delta.
    - scope=campaign: attributed value by campaign; same.
    - scope=paths: path length and time-to-convert drivers.
    Narrative ties explanations to config and data changes.
    """
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    if not JOURNEYS:
        raise HTTPException(status_code=400, detail="No journeys loaded.")

    # Parse period
    def _parse_iso(val: str) -> datetime:
        # Support both plain ISO and ISO with trailing 'Z', and normalize to naive UTC.
        v = val.replace("Z", "+00:00")
        dt = datetime.fromisoformat(v)
        # Drop tzinfo so we consistently compare naive datetimes everywhere.
        if dt.tzinfo is not None:
            dt = dt.astimezone(tz=None).replace(tzinfo=None)
        return dt

    try:
        if to:
            end = _parse_iso(to)
        else:
            end = datetime.utcnow()
        if from_:
            start = _parse_iso(from_)
        else:
            # Default to last 30 days
            from datetime import timedelta

            start = end - timedelta(days=30)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid from/to: {exc}")

    # Previous period of same length
    delta = end - start
    prev_end = start
    prev_start = start - delta

    resolved_cfg, meta = load_config_and_meta(db, config_id)
    journeys = JOURNEYS
    if resolved_cfg:
        journeys = apply_model_config_to_journeys(journeys, resolved_cfg.config_json or {})

    curr_j = _filter_journeys_by_period(journeys, start, end)
    prev_j = _filter_journeys_by_period(journeys, prev_start, prev_end)

    drivers: List[ExplainabilityDriver] = []
    mechanics: Dict[str, Any] = {
        "model": model,
        "windows": meta["time_window"] if meta else None,
        "eligibility": {
            "uses_model_config": resolved_cfg is not None,
        },
    }

    curr_res: Dict[str, Any] = {}
    prev_res: Dict[str, Any] = {}
    contrib_deltas: List[Dict[str, Any]] = []
    channel_breakdowns: Dict[str, Any] = {}

    if scope in ("channel", "campaign"):
        # Use attribution engine to get totals, but be defensive if there are no journeys in the period.
        if not curr_j and not prev_j:
            curr_val = 0.0
            prev_val = 0.0
        else:
            try:
                if scope == "channel":
                    curr_res = run_attribution(curr_j, model=model) if curr_j else {"total_value": 0, "channels": []}
                    prev_res = run_attribution(prev_j, model=model) if prev_j else {"total_value": 0, "channels": []}
                else:
                    curr_res = (
                        run_attribution_campaign(curr_j, model=model)
                        if curr_j
                        else {"total_value": 0, "channels": []}
                    )
                    prev_res = (
                        run_attribution_campaign(prev_j, model=model)
                        if prev_j
                        else {"total_value": 0, "channels": []}
                    )
                curr_val = float(curr_res.get("total_value", 0.0) or 0.0)
                prev_val = float(prev_res.get("total_value", 0.0) or 0.0)

                # Per-entity driver breakdown: all channels/campaigns with deltas
                def _to_map(res: Dict[str, Any]) -> Dict[str, float]:
                    m: Dict[str, float] = {}
                    for ch in res.get("channels", []):
                        key = ch.get("channel")
                        if key:
                            m[key] = float(ch.get("attributed_value", 0.0) or 0.0)
                    return m

                curr_map = _to_map(curr_res)
                prev_map = _to_map(prev_res)
                contrib_deltas = []
                keys = set(curr_map.keys()) | set(prev_map.keys())
                for k in keys:
                    cv = curr_map.get(k, 0.0)
                    pv = prev_map.get(k, 0.0)
                    contrib_deltas.append(
                        {"id": k, "delta": cv - pv, "current_value": cv, "previous_value": pv}
                    )
                contrib_deltas.sort(key=lambda x: x["delta"], reverse=True)
            except Exception:
                curr_val = 0.0
                prev_val = 0.0
                contrib_deltas = []

        delta_val = curr_val - prev_val
        # Contribution % of each top contributor to total change (for narrative)
        contribution_pct: Optional[float] = None
        if abs(delta_val) > 1e-9 and contrib_deltas:
            top_delta = contrib_deltas[0].get("delta", 0.0)
            contribution_pct = (top_delta / delta_val) * 100.0

        drivers.append(
            ExplainabilityDriver(
                metric="attributed_value",
                delta=delta_val,
                current_value=curr_val,
                previous_value=prev_val,
                top_contributors=contrib_deltas[:10],
                contribution_pct=contribution_pct,
            )
        )

        # Build per-channel breakdowns for drill-down (scope=channel only).
        if scope == "channel":
            # Helper maps for quick lookup.
            curr_channels: Dict[str, Any] = {ch.get("channel"): ch for ch in curr_res.get("channels", []) if ch.get("channel")}
            prev_channels: Dict[str, Any] = {ch.get("channel"): ch for ch in prev_res.get("channels", []) if ch.get("channel")}
            contrib_map: Dict[str, Any] = {c["id"]: c for c in contrib_deltas}

            # Simple expense aggregation by period for spend deltas (best-effort, marked estimated in UI).
            def _parse_date(d: Optional[str]) -> Optional[datetime]:
                if not d:
                    return None
                try:
                    return datetime.fromisoformat(d)
                except Exception:
                    try:
                        return datetime.fromisoformat(d.split("T")[0])
                    except Exception:
                        return None

            curr_spend: Dict[str, float] = {}
            prev_spend: Dict[str, float] = {}
            for exp in EXPENSES.values():
                if getattr(exp, "status", "active") == "deleted":
                    continue
                ch_name = exp.channel
                # Prefer service period if available, fall back to entry_date.
                d_start = _parse_date(exp.service_period_start or exp.entry_date or exp.period)
                if not d_start:
                    continue
                amount = exp.converted_amount if exp.converted_amount is not None else exp.amount
                if start.date() <= d_start.date() <= end.date():
                    curr_spend[ch_name] = curr_spend.get(ch_name, 0.0) + float(amount or 0.0)
                elif prev_start.date() <= d_start.date() <= prev_end.date():
                    prev_spend[ch_name] = prev_spend.get(ch_name, 0.0) + float(amount or 0.0)

            all_channels = set(curr_channels.keys()) | set(prev_channels.keys()) | set(contrib_map.keys())
            for cid in all_channels:
                c_curr = curr_channels.get(cid, {})
                c_prev = prev_channels.get(cid, {})
                contrib = contrib_map.get(cid, {})

                curr_val_ch = float(c_curr.get("attributed_value", 0.0) or 0.0)
                prev_val_ch = float(c_prev.get("attributed_value", 0.0) or 0.0)
                curr_conv_ch = float(c_curr.get("attributed_conversions", 0.0) or 0.0)
                prev_conv_ch = float(c_prev.get("attributed_conversions", 0.0) or 0.0)
                curr_spend_ch = float(curr_spend.get(cid, 0.0) or 0.0)
                prev_spend_ch = float(prev_spend.get(cid, 0.0) or 0.0)

                def _safe_ratio(num: float, den: float) -> Optional[float]:
                    return num / den if den > 0 else None

                curr_roas = _safe_ratio(curr_val_ch, curr_spend_ch)
                prev_roas = _safe_ratio(prev_val_ch, prev_spend_ch)
                curr_cpa = _safe_ratio(curr_spend_ch, curr_conv_ch)
                prev_cpa = _safe_ratio(prev_spend_ch, prev_conv_ch)

                channel_breakdowns[cid] = {
                    "channel": cid,
                    "spend": {
                        "current": round(curr_spend_ch, 2),
                        "previous": round(prev_spend_ch, 2),
                        "delta": round(curr_spend_ch - prev_spend_ch, 2),
                    },
                    "conversions": {
                        "current": round(curr_conv_ch, 2),
                        "previous": round(prev_conv_ch, 2),
                        "delta": round(curr_conv_ch - prev_conv_ch, 2),
                    },
                    "attributed_value": {
                        "current": round(curr_val_ch, 2),
                        "previous": round(prev_val_ch, 2),
                        "delta": round(contrib.get("delta", curr_val_ch - prev_val_ch), 2),
                    },
                    "roas": {
                        "current": round(curr_roas, 4) if curr_roas is not None else None,
                        "previous": round(prev_roas, 4) if prev_roas is not None else None,
                        "delta": round((curr_roas - prev_roas), 4) if curr_roas is not None and prev_roas is not None else None,
                    },
                    "cpa": {
                        "current": round(curr_cpa, 4) if curr_cpa is not None else None,
                        "previous": round(prev_cpa, 4) if prev_cpa is not None else None,
                        "delta": round((curr_cpa - prev_cpa), 4) if curr_cpa is not None and prev_cpa is not None else None,
                    },
                }
    elif scope == "paths":
        # Summarise average path length and time-to-convert using existing analyze_paths,
        # but be defensive: any error should degrade gracefully to zeros instead of 500.
        curr_len = 0.0
        prev_len = 0.0
        curr_time_f = 0.0
        prev_time_f = 0.0
        try:
            curr_pa = analyze_paths(curr_j) if curr_j else {}
            prev_pa = analyze_paths(prev_j) if prev_j else {}
            curr_len = float(curr_pa.get("avg_path_length") or 0.0)
            prev_len = float(prev_pa.get("avg_path_length") or 0.0)
            curr_time = curr_pa.get("avg_time_to_conversion_days")
            prev_time = prev_pa.get("avg_time_to_conversion_days")
            curr_time_f = float(curr_time or 0.0)
            prev_time_f = float(prev_time or 0.0)
        except Exception:
            # If path analysis fails for any reason, fall back to neutral deltas.
            curr_len = prev_len = 0.0
            curr_time_f = prev_time_f = 0.0

        drivers.append(
            ExplainabilityDriver(
                metric="avg_path_length",
                delta=curr_len - prev_len,
                current_value=curr_len,
                previous_value=prev_len,
                top_contributors=[],
            )
        )
        drivers.append(
            ExplainabilityDriver(
                metric="avg_time_to_conversion_days",
                delta=curr_time_f - prev_time_f,
                current_value=curr_time_f,
                previous_value=prev_time_f,
                top_contributors=[],
            )
        )

    # Data health: confidence + notes
    health_notes: List[str] = []
    conf = None
    if scope == "channel":
        conf_row = get_latest_quality_for_scope(db, "channel", scope_id, meta["conversion_key"] if meta else None)
        if conf_row:
            conf = {
                "score": conf_row.confidence_score,
                "label": conf_row.confidence_label,
                "components": conf_row.components_json,
            }
            if conf_row.confidence_label == "low":
                health_notes.append("Low confidence in channel attribution – check identity match and tracking completeness.")
    elif scope == "campaign":
        conf_row = get_latest_quality_for_scope(db, "campaign", scope_id, meta["conversion_key"] if meta else None)
        if conf_row:
            conf = {
                "score": conf_row.confidence_score,
                "label": conf_row.confidence_label,
                "components": conf_row.components_json,
            }
            if conf_row.confidence_label == "low":
                health_notes.append("Low confidence in campaign attribution – possible mapping or tracking gaps.")

    data_health = {
        "confidence": conf,
        "notes": health_notes,
    }

    # Data quality delta (current vs previous snapshot) for comparability & table.
    dq_delta: Dict[str, Any] = {"metrics": []}
    try:
        # For now, use global channel scope (scope_id=None) as the backbone for deltas.
        q = (
            db.query(AttributionQualitySnapshot)
            .filter(AttributionQualitySnapshot.scope == "channel")
            .order_by(AttributionQualitySnapshot.ts_bucket.desc())
            .limit(2)
            .all()
        )
        if q:
            current_snap = q[0]
            prev_snap = q[1] if len(q) > 1 else None
            comp_curr = current_snap.components_json or {}
            comp_prev = prev_snap.components_json if prev_snap else {}

            def _metric_row(key: str, label: str, unit: str = "") -> Dict[str, Any]:
                curr_v = None
                prev_v = None
                if key == "confidence_score":
                    curr_v = current_snap.confidence_score
                    prev_v = prev_snap.confidence_score if prev_snap else None
                else:
                    curr_v = comp_curr.get(key)
                    prev_v = comp_prev.get(key) if prev_snap else None
                delta_v = None
                if curr_v is not None and prev_v is not None:
                    delta_v = curr_v - prev_v
                return {
                    "key": key,
                    "label": label,
                    "unit": unit,
                    "current": curr_v,
                    "previous": prev_v,
                    "delta": delta_v,
                }

            dq_delta["metrics"] = [
                _metric_row("confidence_score", "Confidence score", "/100"),
                _metric_row("join_rate", "Join rate", ""),
                _metric_row("missing_rate", "Missing IDs / UTMs rate", ""),
                _metric_row("freshness_lag_minutes", "Freshness lag (minutes)", "min"),
                _metric_row("dedup_rate", "Duplication rate", ""),
            ]
    except Exception:
        dq_delta = {"metrics": []}

    # Config changes
    cfg_changes: List[Dict[str, Any]] = []
    if meta and meta.get("config_id"):
        cfg_changes = summarize_config_changes(db, meta["config_id"], prev_start)

    config_info = {
        "config_id": meta["config_id"] if meta else None,
        "version": meta["config_version"] if meta else None,
        "changes": cfg_changes,
    }

    # Human-readable config diff summary for inline display.
    config_diff: Dict[str, Any] = {
        "has_changes": bool(cfg_changes),
        "changes_count": len(cfg_changes),
        "lines": [],
    }
    if resolved_cfg:
        cfg_json = resolved_cfg.config_json or {}
        windows = cfg_json.get("windows", {})
        eligibility = cfg_json.get("eligible_touchpoints", {})
        conversions = cfg_json.get("conversions", {})
        lines: List[str] = []
        if "click_lookback_days" in windows:
            lines.append(f"Click lookback: {windows.get('click_lookback_days')}d (current)")
        if "impression_lookback_days" in windows:
            lines.append(f"Impression lookback: {windows.get('impression_lookback_days')}d (current)")
        if "conversion_latency_days" in windows:
            lines.append(f"Conversion latency: {windows.get('conversion_latency_days')}d (current)")
        if conversions.get("primary_key"):
            lines.append(f"Conversion key: {conversions.get('primary_key')}")
        if eligibility:
            lines.append("Eligibility rules updated (touchpoint filters applied).")
        if not lines and cfg_changes:
            lines.append("Config changed recently; see Settings → Measurement model configs for full details.")
        config_diff["lines"] = lines

    # Feature importance: share of attributed value in current period + direction of change
    feature_importance: List[FeatureImportanceItem] = []
    if scope in ("channel", "campaign") and curr_res.get("channels"):
        total_curr = float(curr_res.get("total_value", 0.0) or 0.0)
        delta_val_total = 0.0
        if drivers and drivers[0].metric == "attributed_value":
            delta_val_total = drivers[0].delta
        contrib_by_id = {c["id"]: c for c in contrib_deltas}
        for ch in curr_res.get("channels", []):
            cid = ch.get("channel")
            if not cid:
                continue
            share = float(ch.get("attributed_share", 0.0) or 0.0)
            contrib = contrib_by_id.get(cid, {})
            d = contrib.get("delta", 0.0)
            if abs(d) < 1e-9:
                direction = "neutral"
            else:
                direction = "up" if d > 0 else "down"
            feature_importance.append(
                FeatureImportanceItem(
                    id=cid,
                    name=cid,
                    importance=share,
                    share_pct=round(share * 100.0, 2),
                    delta=round(d, 2),
                    direction=direction,
                )
            )
        feature_importance.sort(key=lambda x: -x.importance)

    # Narrative tied to config and data changes
    narrative = _build_explainability_narrative(
        scope=scope,
        drivers=drivers,
        curr_j_count=len(curr_j),
        prev_j_count=len(prev_j),
        config_info=config_info,
        data_health=data_health,
        mechanics=mechanics,
    )

    # Serialize period datetimes for JSON
    def _dt_iso(d: datetime) -> str:
        return d.isoformat() + "Z" if d.tzinfo is None else d.isoformat()

    period_serialized = {
        "current": {"from": _dt_iso(start), "to": _dt_iso(end)},
        "previous": {"from": _dt_iso(prev_start), "to": _dt_iso(prev_end)},
    }

    # Change decomposition (volume vs mix vs rate/value)
    total_curr_val = float(curr_res.get("total_value", 0.0) or 0.0) if curr_res else 0.0
    total_prev_val = float(prev_res.get("total_value", 0.0) or 0.0) if prev_res else 0.0
    change_decomposition = _compute_change_decomposition(
        curr_val=total_curr_val,
        prev_val=total_prev_val,
        curr_j_count=len(curr_j),
        prev_j_count=len(prev_j),
    )

    # Comparability rating based on config changes, journey shifts, and data health.
    comparability_rating = "high"
    comp_warnings: List[ComparabilityWarning] = []

    # Journey volume shift
    if len(prev_j) == 0 or len(curr_j) == 0:
        comparability_rating = "low"
        comp_warnings.append(
            ComparabilityWarning(
                severity="critical",
                message="One of the periods has no journeys; results are not comparable.",
                action="Use a period with sufficient volume or adjust filters.",
            )
        )
    else:
        vol_ratio = len(curr_j) / float(len(prev_j))
        if vol_ratio > 2.0 or vol_ratio < 0.5:
            if comparability_rating != "low":
                comparability_rating = "medium"
            comp_warnings.append(
                ComparabilityWarning(
                    severity="warn",
                    message=f"Journey volume shifted significantly between periods (current {len(curr_j):,}, previous {len(prev_j):,}).",
                    action="Confirm no major tracking changes or seasonality explain this shift.",
                )
            )

    # Config changes
    if cfg_changes:
        if comparability_rating == "high":
            comparability_rating = "medium"
        comp_warnings.append(
            ComparabilityWarning(
                severity="warn",
                message=f"Model configuration changed {len(cfg_changes)} time(s) in the comparison window.",
                action="Pin analysis to a specific config version in Settings → Measurement model configs.",
            )
        )

    # Data quality issues
    if data_health.get("confidence"):
        conf_score = data_health["confidence"].get("score") or 0.0
        conf_label = data_health["confidence"].get("label", "unknown")
        if conf_label == "low" or conf_score < 60:
            comparability_rating = "low"
            comp_warnings.append(
                ComparabilityWarning(
                    severity="critical",
                    message=f"Data quality confidence is {conf_label} ({conf_score:.0f}/100).",
                    action="Investigate identifier completeness, join rate, and deduplication metrics in Data Quality.",
                )
            )
        elif conf_label == "medium" or conf_score < 80:
            if comparability_rating == "high":
                comparability_rating = "medium"
            comp_warnings.append(
                ComparabilityWarning(
                    severity="warn",
                    message=f"Data quality confidence is {conf_label} ({conf_score:.0f}/100).",
                    action="Review recent data quality alerts and fix ingestion issues before acting on fine-grained changes.",
                )
            )

    comparability = ComparabilitySummary(rating=comparability_rating, warnings=comp_warnings[:3])

    # Simple timeline: daily attributed value in current period (optionally used for "When did it change?")
    timeline: List[Dict[str, Any]] = []
    daily_values: Dict[str, float] = {}
    for j in curr_j:
        tps = j.get("touchpoints", [])
        if not tps:
            continue
        ts = tps[-1].get("timestamp")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            continue
        day_str = dt.date().isoformat()
        daily_values[day_str] = daily_values.get(day_str, 0.0) + float(j.get("conversion_value", 0.0) or 0.0)
    for day in sorted(daily_values.keys()):
        timeline.append({"date": day, "attributed_value": round(daily_values[day], 2)})

    return ExplainabilitySummary(
        period=period_serialized,
        drivers=drivers,
        data_health=data_health,
        config=config_info,
        mechanics=mechanics,
        feature_importance=feature_importance,
        narrative=narrative,
        change_decomposition=change_decomposition,
        comparability=comparability,
        config_diff=config_diff,
        data_quality_delta=dq_delta,
        timeline=timeline,
        channel_breakdowns=channel_breakdowns,
    )


# ==================== Incrementality Experiments API ====================


class ExperimentCreate(BaseModel):
    name: str
    channel: str
    start_at: datetime
    end_at: datetime
    conversion_key: Optional[str] = None
    notes: Optional[str] = None


class ExperimentSummary(BaseModel):
    id: int
    name: str
    channel: str
    start_at: datetime
    end_at: datetime
    status: str
    conversion_key: Optional[str] = None


@app.get("/api/experiments", response_model=List[ExperimentSummary])
def list_experiments(db=Depends(get_db)):
    rows = db.query(Experiment).order_by(Experiment.created_at.desc()).all()
    return [
        ExperimentSummary(
            id=r.id,
            name=r.name,
            channel=r.channel,
            start_at=r.start_at,
            end_at=r.end_at,
            status=r.status,
            conversion_key=r.conversion_key,
        )
        for r in rows
    ]


@app.post("/api/experiments", response_model=ExperimentSummary)
def create_experiment(body: ExperimentCreate, db=Depends(get_db)):
    exp = Experiment(
        name=body.name,
        channel=body.channel,
        start_at=body.start_at,
        end_at=body.end_at,
        status="draft",
        conversion_key=body.conversion_key,
        notes=body.notes,
        created_at=datetime.utcnow(),
    )
    db.add(exp)
    db.commit()
    db.refresh(exp)
    return ExperimentSummary(
        id=exp.id,
        name=exp.name,
        channel=exp.channel,
        start_at=exp.start_at,
        end_at=exp.end_at,
        status=exp.status,
        conversion_key=exp.conversion_key,
    )


@app.get("/api/experiments/{exp_id}")
def get_experiment(exp_id: int, db=Depends(get_db)):
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    return {
        "id": exp.id,
        "name": exp.name,
        "channel": exp.channel,
        "start_at": exp.start_at,
        "end_at": exp.end_at,
        "status": exp.status,
        "conversion_key": exp.conversion_key,
        "notes": exp.notes,
    }


class ExperimentStatusUpdate(BaseModel):
    status: str  # draft/running/completed


@app.post("/api/experiments/{exp_id}/status", response_model=ExperimentSummary)
def update_experiment_status(exp_id: int, body: ExperimentStatusUpdate, db=Depends(get_db)):
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    if body.status not in ("draft", "running", "completed"):
        raise HTTPException(status_code=400, detail="Invalid status")
    exp.status = body.status
    db.add(exp)
    db.commit()
    db.refresh(exp)
    return ExperimentSummary(
        id=exp.id,
        name=exp.name,
        channel=exp.channel,
        start_at=exp.start_at,
        end_at=exp.end_at,
        status=exp.status,
        conversion_key=exp.conversion_key,
    )


class AssignmentRequest(BaseModel):
    profile_ids: List[str]
    treatment_rate: float = 0.5


@app.post("/api/experiments/{exp_id}/assign")
def assign_experiment(exp_id: int, body: AssignmentRequest, db=Depends(get_db)):
    """
    Assign profiles to treatment/control using deterministic hashing.
    
    This ensures stable, reproducible assignments across calls.
    """
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    if not body.profile_ids:
        return {"assigned": 0, "treatment": 0, "control": 0}

    counts = assign_profiles_deterministic(
        db=db,
        experiment_id=exp_id,
        profile_ids=body.profile_ids,
        treatment_rate=body.treatment_rate,
    )

    return {
        "assigned": len(body.profile_ids),
        "treatment": counts["treatment"],
        "control": counts["control"],
    }


class OutcomePayload(BaseModel):
    profile_id: str
    conversion_ts: datetime
    value: float = 0.0


class OutcomesRequest(BaseModel):
    outcomes: List[OutcomePayload]


@app.post("/api/experiments/{exp_id}/outcomes")
def record_outcomes(exp_id: int, body: OutcomesRequest, db=Depends(get_db)):
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    if not body.outcomes:
        return {"inserted": 0}
    count = 0
    for o in body.outcomes:
        db.add(
            ExperimentOutcome(
                experiment_id=exp_id,
                profile_id=o.profile_id,
                conversion_ts=o.conversion_ts,
                value=o.value,
            )
        )
        count += 1
    db.commit()
    return {"inserted": count}


@app.get("/api/experiments/{exp_id}/results")
def get_experiment_results(exp_id: int, db=Depends(get_db)):
    import math

    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")

    assignments = db.query(ExperimentAssignment).filter(ExperimentAssignment.experiment_id == exp_id).all()
    if not assignments:
        return {"experiment_id": exp_id, "status": exp.status, "insufficient_data": True}

    outcomes = {
        o.profile_id: o
        for o in db.query(ExperimentOutcome).filter(ExperimentOutcome.experiment_id == exp_id).all()
    }

    treat_n = 0
    control_n = 0
    treat_conv = 0
    control_conv = 0
    treat_value = 0.0
    control_value = 0.0

    for a in assignments:
        out = outcomes.get(a.profile_id)
        if a.group == "treatment":
            treat_n += 1
            if out is not None:
                treat_conv += 1
                treat_value += float(out.value or 0.0)
        else:
            control_n += 1
            if out is not None:
                control_conv += 1
                control_value += float(out.value or 0.0)

    if treat_n == 0 or control_n == 0:
        return {"experiment_id": exp_id, "status": exp.status, "insufficient_data": True}

    p_t = treat_conv / treat_n if treat_n > 0 else 0.0
    p_c = control_conv / control_n if control_n > 0 else 0.0
    diff = p_t - p_c
    uplift_abs = diff
    uplift_rel = diff / p_c if p_c > 0 else None

    # Simple normal-approx CI for diff in proportions
    se = math.sqrt(p_t * (1 - p_t) / treat_n + p_c * (1 - p_c) / control_n)
    if se > 0:
        z = 1.96
        ci_low = diff - z * se
        ci_high = diff + z * se
        # Two-sided z-test p-value (approx)
        z_score = diff / se
        # approximate using complementary error function
        from math import erf

        p_value = 2 * (1 - 0.5 * (1 + erf(abs(z_score) / math.sqrt(2))))
    else:
        ci_low = None
        ci_high = None
        p_value = None

    # Upsert ExperimentResult
    res = db.query(ExperimentResult).filter(ExperimentResult.experiment_id == exp_id).first()
    if res is None:
        res = ExperimentResult(
            experiment_id=exp_id,
            computed_at=datetime.utcnow(),
            uplift_abs=uplift_abs,
            uplift_rel=uplift_rel,
            ci_low=ci_low,
            ci_high=ci_high,
            p_value=p_value,
            treatment_size=treat_n,
            control_size=control_n,
            meta_json={
                "treatment_conversions": treat_conv,
                "control_conversions": control_conv,
                "treatment_value": treat_value,
                "control_value": control_value,
            },
        )
        db.add(res)
    else:
        res.computed_at = datetime.utcnow()
        res.uplift_abs = uplift_abs
        res.uplift_rel = uplift_rel
        res.ci_low = ci_low
        res.ci_high = ci_high
        res.p_value = p_value
        res.treatment_size = treat_n
        res.control_size = control_n
        res.meta_json = {
            "treatment_conversions": treat_conv,
            "control_conversions": control_conv,
            "treatment_value": treat_value,
            "control_value": control_value,
        }
    db.commit()

    return {
        "experiment_id": exp_id,
        "status": exp.status,
        "treatment": {
            "n": treat_n,
            "conversions": treat_conv,
            "conversion_rate": p_t,
            "total_value": treat_value,
        },
        "control": {
            "n": control_n,
            "conversions": control_conv,
            "conversion_rate": p_c,
            "total_value": control_value,
        },
        "uplift_abs": uplift_abs,
        "uplift_rel": uplift_rel,
        "ci_low": ci_low,
        "ci_high": ci_high,
        "p_value": p_value,
        "insufficient_data": False,
    }


class ExposurePayload(BaseModel):
    profile_id: str
    exposure_ts: Optional[datetime] = None
    campaign_id: Optional[str] = None
    message_id: Optional[str] = None


class ExposuresRequest(BaseModel):
    exposures: List[ExposurePayload]


@app.post("/api/experiments/{exp_id}/exposures")
def record_experiment_exposures(exp_id: int, body: ExposuresRequest, db=Depends(get_db)):
    """
    Record exposures (e.g., message sent) for an experiment.
    
    Exposures track when treatment was actually delivered, separate from assignment.
    """
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    
    if not body.exposures:
        return {"recorded": 0}
    
    exposures = [
        {
            "profile_id": e.profile_id,
            "exposure_ts": e.exposure_ts,
            "campaign_id": e.campaign_id,
            "message_id": e.message_id,
        }
        for e in body.exposures
    ]
    
    count = record_exposures_batch(db, exp_id, exposures)
    return {"recorded": count}


@app.get("/api/experiments/{exp_id}/exposures")
def get_experiment_exposures(exp_id: int, limit: int = 100, db=Depends(get_db)):
    """Get recent exposures for an experiment."""
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    
    exposures = (
        db.query(ExperimentExposure)
        .filter(ExperimentExposure.experiment_id == exp_id)
        .order_by(ExperimentExposure.exposure_ts.desc())
        .limit(limit)
        .all()
    )
    
    return [
        {
            "profile_id": e.profile_id,
            "exposure_ts": e.exposure_ts,
            "campaign_id": e.campaign_id,
            "message_id": e.message_id,
        }
        for e in exposures
    ]


@app.get("/api/experiments/{exp_id}/time-series")
def get_experiment_time_series_endpoint(exp_id: int, freq: str = "D", db=Depends(get_db)):
    """
    Get daily/weekly time series of experiment metrics.
    
    Returns cumulative metrics over time for visualization.
    """
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    
    df = get_experiment_time_series(db, exp_id, freq=freq)
    
    if df.empty:
        return {"data": []}
    
    return {"data": df.to_dict(orient="records")}


class PowerAnalysisRequest(BaseModel):
    baseline_rate: float
    mde: float
    alpha: float = 0.05
    power: float = 0.8
    treatment_rate: float = 0.5


@app.post("/api/experiments/power-analysis")
def power_analysis(body: PowerAnalysisRequest):
    """
    Estimate required sample size for an experiment.
    
    Parameters:
    - baseline_rate: control group conversion rate (e.g., 0.05 = 5%)
    - mde: minimum detectable effect (absolute, e.g., 0.01 = 1pp)
    - alpha: significance level (default 0.05)
    - power: statistical power (default 0.8)
    - treatment_rate: fraction in treatment (default 0.5)
    
    Returns total sample size needed.
    """
    if not (0 < body.baseline_rate < 1):
        raise HTTPException(status_code=400, detail="baseline_rate must be between 0 and 1")
    if not (0 < body.mde < 1):
        raise HTTPException(status_code=400, detail="mde must be between 0 and 1")
    if not (0 < body.alpha < 1):
        raise HTTPException(status_code=400, detail="alpha must be between 0 and 1")
    if not (0 < body.power < 1):
        raise HTTPException(status_code=400, detail="power must be between 0 and 1")
    if not (0 < body.treatment_rate < 1):
        raise HTTPException(status_code=400, detail="treatment_rate must be between 0 and 1")
    
    total_n = estimate_sample_size(
        baseline_rate=body.baseline_rate,
        mde=body.mde,
        alpha=body.alpha,
        power=body.power,
        treatment_rate=body.treatment_rate,
    )
    
    n_treatment = int(total_n * body.treatment_rate)
    n_control = total_n - n_treatment
    
    return {
        "total_sample_size": total_n,
        "treatment_size": n_treatment,
        "control_size": n_control,
        "baseline_rate": body.baseline_rate,
        "mde": body.mde,
        "alpha": body.alpha,
        "power": body.power,
    }


@app.post("/api/experiments/nightly-report")
def run_nightly_report_endpoint(db=Depends(get_db)):
    """
    Run nightly report for all active experiments.
    
    Computes results and generates alerts. Intended for scheduled execution.
    """
    report = run_nightly_report(db)
    return report


class ExperimentHealth(BaseModel):
    experiment_id: int
    sample: Dict[str, int]
    exposures: Dict[str, int]
    outcomes: Dict[str, int]
    balance: Dict[str, Any]
    data_completeness: Dict[str, Dict[str, str]]
    overlap_risk: Dict[str, Any]
    ready_state: Dict[str, Any]


@app.get("/api/experiments/{exp_id}/health", response_model=ExperimentHealth)
def get_experiment_health(exp_id: int, db=Depends(get_db)):
    """
    Lightweight health summary for a single experiment.

    Designed as a trust layer for the Incrementality dashboard:
    - sample sizes and conversions by group
    - basic balance check vs an assumed 50/50 split (or close)
    - data completeness for assignments, outcomes, and exposures
    - minimal contamination / overlap heuristic
    - coarse readiness classification (not_ready / early / ready)
    """
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")

    payload = compute_experiment_health(db, exp_id)
    return ExperimentHealth(**payload)


class AutoAssignRequest(BaseModel):
    channel: str
    start_date: datetime
    end_date: datetime
    treatment_rate: float = 0.5


@app.post("/api/experiments/{exp_id}/auto-assign")
def auto_assign_experiment(exp_id: int, body: AutoAssignRequest, db=Depends(get_db)):
    """
    Automatically assign profiles based on conversion paths.
    
    Finds all profiles who had touchpoints in the specified channel during
    the experiment period and assigns them deterministically.
    
    Useful for post-hoc analysis of historical data.
    """
    exp = db.get(Experiment, exp_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    
    counts = auto_assign_from_conversion_paths(
        db=db,
        experiment_id=exp_id,
        channel=body.channel,
        start_date=body.start_date,
        end_date=body.end_date,
        treatment_rate=body.treatment_rate,
    )
    
    return {
        "treatment": counts["treatment"],
        "control": counts["control"],
        "total": counts["treatment"] + counts["control"],
    }


# ==================== Dataset Routes ====================

@app.post("/api/datasets/upload")
async def upload_dataset(file: UploadFile = File(...), dataset_id: Optional[str] = None, type: str = "sales"):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")
    dataset_id = dataset_id or file.filename.replace(".csv", "")
    dest = SAMPLE_DIR / f"{dataset_id}.csv"
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    content = await file.read()
    dest.write_bytes(content)
    DATASETS[dataset_id] = {"path": dest, "type": type}
    df = pd.read_csv(dest).head(5)
    return {"dataset_id": dataset_id, "columns": list(df.columns), "preview_rows": df.to_dict(orient="records"), "path": str(dest), "type": type}

@app.get("/api/datasets")
def list_datasets():
    return [
        {"dataset_id": k, "path": str(v.get("path", "")), "type": v.get("type", "sales"), "source": v.get("source", "upload"), "metadata": v.get("metadata")}
        for k, v in DATASETS.items()
    ]

@app.get("/api/datasets/{dataset_id}")
def get_dataset(dataset_id: str, preview_only: bool = True):
    dataset_info = DATASETS.get(dataset_id)
    if not dataset_info:
        raise HTTPException(status_code=404, detail="Dataset not found")
    path = dataset_info.get("path")
    if path is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    p = Path(path) if isinstance(path, str) else path
    if not p.exists():
        raise HTTPException(status_code=404, detail="Dataset not found")
    df = pd.read_csv(p).head(5) if preview_only else pd.read_csv(p)
    out = {"dataset_id": dataset_id, "columns": list(df.columns), "preview_rows": df.to_dict(orient="records"), "type": dataset_info.get("type", "sales")}
    if dataset_info.get("metadata"):
        out["metadata"] = dataset_info["metadata"]
    return out

@app.get("/api/datasets/{dataset_id}/validate")
def validate_dataset(dataset_id: str, kpi_target: Optional[str] = Query(None, description="sales | attribution for KPI suggestion bias")):
    dataset_info = DATASETS.get(dataset_id)
    if not dataset_info:
        raise HTTPException(status_code=404, detail="Dataset not found")
    path = dataset_info.get("path")
    if path is None:
        raise HTTPException(status_code=404, detail="Dataset file not found")
    p = Path(path) if isinstance(path, str) else path
    if not p.exists():
        raise HTTPException(status_code=404, detail="Dataset file not found")
    df = pd.read_csv(p)
    columns = list(df.columns)
    n_rows = len(df)
    col_info = [{"name": col, "dtype": str(df[col].dtype), "missing": int(df[col].isna().sum()), "unique": int(df[col].nunique()), "sample_values": df[col].dropna().head(3).tolist()} for col in columns]
    # Smart mapping suggestions
    suggestions = build_smart_suggestions(df, kpi_target=kpi_target)
    date_column = suggestions.get("date_column")
    date_range = None
    if date_column and date_column in df.columns:
        try:
            parsed = pd.to_datetime(df[date_column], errors="coerce")
            date_range = {"min": str(parsed.min().date()), "max": str(parsed.max().date()), "n_periods": int(parsed.nunique())}
        except Exception:
            pass
    # Legacy suggestion keys for backward compatibility
    suggestions["spend_channels"] = suggestions.get("spend_channels") or []
    suggestions["kpi_columns"] = suggestions.get("kpi_columns") or []
    is_tall = {"channel", "campaign", "spend"}.issubset(set(columns))
    warnings = []
    if n_rows < 20:
        warnings.append(f"Only {n_rows} rows.")
    if date_column and date_range and date_range["n_periods"] < 20:
        warnings.append(f"Only {date_range['n_periods']} unique dates.")
    for ci in col_info:
        if ci["missing"] > 0 and n_rows and ci["missing"] / n_rows > 0.1:
            warnings.append(f"Column '{ci['name']}' has {ci['missing']/n_rows*100:.0f}% missing values.")
    return {"dataset_id": dataset_id, "n_rows": n_rows, "n_columns": len(columns), "columns": col_info, "date_column": date_column, "date_range": date_range, "format": "tall" if is_tall else "wide", "suggestions": suggestions, "warnings": warnings}


@app.get("/api/mmm/platform-options")
def get_mmm_platform_options():
    """Return available spend channels and optional covariates for platform-built MMM datasets."""
    channels = set()
    for exp in EXPENSES.values():
        if getattr(exp, "status", "active") == "deleted":
            continue
        ch = getattr(exp, "channel", None)
        if ch:
            channels.add(ch)
    return {
        "spend_channels": sorted(channels),
        "covariates": [],  # Platform has no covariates yet; hide in UI when empty
    }


@app.post("/api/mmm/datasets/build-from-platform")
def build_mmm_dataset_from_platform_endpoint(body: BuildFromPlatformRequest, db=Depends(get_db)):
    """
    Build an MMM weekly dataset from platform data (journeys + expenses).
    Persists CSV and registers dataset_id. Returns preview, coverage, and metadata.
    """
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db, limit=50000)
    journeys = JOURNEYS
    expenses_list = list(EXPENSES.values())
    try:
        df, coverage = build_mmm_dataset_from_platform(
            journeys=journeys,
            expenses=expenses_list,
            date_start=body.date_start,
            date_end=body.date_end,
            kpi_target=body.kpi_target,
            spend_channels=body.spend_channels,
            covariates=body.covariates or [],
            currency=body.currency,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    kpi_col = "sales" if body.kpi_target == "sales" else "conversions"
    dataset_id = f"platform-mmm-{uuid.uuid4().hex[:12]}"
    dest = MMM_PLATFORM_DIR / f"{dataset_id}.csv"
    df.to_csv(dest, index=False)
    metadata = {
        "period_start": body.date_start,
        "period_end": body.date_end,
        "kpi_target": body.kpi_target,
        "kpi_column": kpi_col,
        "spend_channels": body.spend_channels,
        "covariates": body.covariates or [],
        "currency": body.currency,
        "source": "platform",
    }
    if body.kpi_target == "attribution":
        if body.attribution_model:
            metadata["attribution_model"] = body.attribution_model
        if body.attribution_config_id:
            metadata["attribution_config_id"] = body.attribution_config_id
    DATASETS[dataset_id] = {
        "path": dest,
        "type": "sales" if body.kpi_target == "sales" else "attribution",
        "source": "platform",
        "metadata": metadata,
    }
    columns = list(df.columns)
    preview = df.head(10).to_dict(orient="records")
    return {
        "dataset_id": dataset_id,
        "columns": columns,
        "preview_rows": preview,
        "coverage": coverage,
        "metadata": metadata,
        "path": str(dest),
        "type": DATASETS[dataset_id]["type"],
    }


@app.post("/api/mmm/datasets/{dataset_id}/validate-mapping")
def validate_mapping_endpoint(dataset_id: str, body: ValidateMappingRequest):
    """
    Validate an MMM column mapping before running the model.
    Returns blocking errors and warnings (e.g. weekly sanity, non-negative spend, history length, multicollinearity, missingness).
    """
    dataset_info = DATASETS.get(dataset_id)
    if not dataset_info:
        raise HTTPException(status_code=404, detail="Dataset not found")
    path = dataset_info.get("path")
    if path is None:
        raise HTTPException(status_code=404, detail="Dataset file not found")
    p = Path(path) if isinstance(path, str) else path
    if not p.exists():
        raise HTTPException(status_code=404, detail="Dataset file not found")
    df = pd.read_csv(p)
    errors, warnings, details = validate_mapping(
        df,
        date_column=body.date_column,
        kpi=body.kpi,
        spend_channels=body.spend_channels,
        covariates=body.covariates,
    )
    from app.services_mmm_mapping import get_missingness_top_offenders
    details["missingness_top"] = get_missingness_top_offenders(details, top_n=5)
    return {"errors": errors, "warnings": warnings, "details": details, "valid": len(errors) == 0}


# ==================== MMM Model Routes ====================

@app.post("/api/models")
def run_model(cfg: ModelConfig, tasks: BackgroundTasks):
    if cfg.dataset_id not in DATASETS:
        raise HTTPException(status_code=404, detail="dataset_id not found")
    if cfg.frequency == "W" and SETTINGS.mmm.frequency != "W":
        cfg.frequency = SETTINGS.mmm.frequency
    kpi_mode = getattr(cfg, "kpi_mode", "conversions") or "conversions"
    run_id = f"mmm_{uuid.uuid4().hex[:12]}"
    now = _now_iso()
    config_dict = json.loads(cfg.model_dump_json())
    dataset_meta = (DATASETS.get(cfg.dataset_id) or {}).get("metadata") or {}
    RUNS[run_id] = {
        "status": "queued",
        "config": config_dict,
        "kpi_mode": kpi_mode,
        "created_at": now,
        "updated_at": now,
        "dataset_id": cfg.dataset_id,
    }
    if dataset_meta.get("attribution_model"):
        RUNS[run_id]["attribution_model"] = dataset_meta["attribution_model"]
    if dataset_meta.get("attribution_config_id"):
        RUNS[run_id]["attribution_config_id"] = dataset_meta["attribution_config_id"]
    _save_runs()
    tasks.add_task(_fit_model, run_id, cfg)
    return {"run_id": run_id, "status": "queued"}


@app.get("/api/models")
def list_models():
    """List all MMM runs with summary for history. Most recent first."""
    items = []
    for run_id, r in RUNS.items():
        config = r.get("config") or {}
        items.append({
            "run_id": run_id,
            "status": r.get("status", "unknown"),
            "created_at": r.get("created_at"),
            "updated_at": r.get("updated_at"),
            "dataset_id": r.get("dataset_id") or config.get("dataset_id"),
            "kpi_mode": r.get("kpi_mode"),
            "kpi": config.get("kpi"),
            "n_channels": len(config.get("spend_channels") or []),
            "n_covariates": len(config.get("covariates") or []),
            "r2": r.get("r2"),
            "engine": r.get("engine"),
        })
    items.sort(key=lambda x: (x.get("created_at") or ""), reverse=True)
    return items

@app.get("/api/models/compare")
def compare_models():
    if not RUNS:
        return []
    comparison: Dict[str, Any] = {}
    for run_id, run in RUNS.items():
        if run.get("status") != "finished":
            continue
        kpi_mode = run.get("kpi_mode", "conversions")
        for roi_entry in run.get("roi", []):
            ch = roi_entry["channel"]
            comparison.setdefault(ch, {"channel": ch, "roi": {}, "contrib": {}})
            comparison[ch]["roi"][kpi_mode] = roi_entry["roi"]
        for c_entry in run.get("contrib", []):
            ch = c_entry["channel"]
            comparison.setdefault(ch, {"channel": ch, "roi": {}, "contrib": {}})
            comparison[ch]["contrib"][kpi_mode] = c_entry["mean_share"]
    return list(comparison.values())

@app.get("/api/models/{run_id}")
def get_model(run_id: str):
    res = RUNS.get(run_id)
    if not res:
        raise HTTPException(status_code=404, detail="run_id not found")
    return res

@app.get("/api/models/{run_id}/contrib")
def channel_contrib(run_id: str):
    return RUNS.get(run_id, {}).get("contrib", [])

@app.get("/api/models/{run_id}/roi")
def roi(run_id: str):
    return RUNS.get(run_id, {}).get("roi", [])


@app.post("/api/models/{run_id}/what_if")
def what_if_scenario(run_id: str, scenario: Dict[str, float] = Body(..., embed=False)):
    """Simulate a simple what-if scenario using per-channel multipliers.

    The scenario is a mapping from channel -> multiplier, where 1.0 keeps the
    channel at baseline, 2.0 doubles its relative contribution weight, etc.

    This endpoint uses the already-fitted ROI and contribution shares and does
    not re-run the full Bayesian model, so it is fast enough for interactive UI.
    """

    run = RUNS.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Model not found")

    roi_map = {r["channel"]: r["roi"] for r in run.get("roi", [])}
    contrib_map = {c["channel"]: c["mean_share"] for c in run.get("contrib", [])}
    if not roi_map or not contrib_map:
        raise HTTPException(status_code=400, detail="ROI or contribution data not available")

    channels = sorted(roi_map.keys())

    baseline_per_channel = {}
    scenario_per_channel = {}
    baseline_total = 0.0
    scenario_total = 0.0

    for ch in channels:
        roi_val = float(roi_map.get(ch, 0.0))
        share = float(contrib_map.get(ch, 0.0))
        base = roi_val * share
        mult = float(scenario.get(ch, 1.0))
        new_val = base * mult

        baseline_per_channel[ch] = base
        scenario_per_channel[ch] = new_val
        baseline_total += base
        scenario_total += new_val

    uplift_abs = scenario_total - baseline_total
    uplift_pct = (uplift_abs / baseline_total * 100.0) if baseline_total != 0 else 0.0

    return {
        "baseline": {
            "total_kpi": baseline_total,
            "per_channel": baseline_per_channel,
        },
        "scenario": {
            "total_kpi": scenario_total,
            "per_channel": scenario_per_channel,
            "multipliers": scenario,
        },
        "lift": {
            "absolute": uplift_abs,
            "percent": uplift_pct,
        },
    }

@app.get("/api/models/{run_id}/summary/channel")
def get_channel_summary(run_id: str):
    res = RUNS.get(run_id)
    if not res:
        raise HTTPException(status_code=404, detail="Model not found")
    return res.get("channel_summary", [])

@app.get("/api/models/{run_id}/summary/campaign")
def get_campaign_summary(run_id: str):
    res = RUNS.get(run_id)
    if not res:
        raise HTTPException(status_code=404, detail="Model not found")
    return res.get("campaigns", [])

@app.get("/api/models/{run_id}/export.csv")
def export_campaign_plan(run_id: str):
    res = RUNS.get(run_id)
    if not res:
        raise HTTPException(status_code=404, detail="Model not found")
    campaigns = res.get("campaigns", [])
    if not campaigns:
        return "", 200, {"Content-Type": "text/csv"}
    out = io.StringIO()
    out.write("channel,campaign,spend,optimal_spend,roi,expected_conversions\n")
    for row in campaigns:
        spend = float(row.get("mean_spend", 0.0))
        roi_val = float(row.get("roi", 0.0))
        out.write(f"{row.get('channel')},{row.get('campaign')},{spend:.4f},{spend:.4f},{roi_val:.6f},{spend*roi_val:.4f}\n")
    return out.getvalue(), 200, {"Content-Type": "text/csv"}

@app.post("/api/models/{run_id}/optimize")
def optimize_budget(run_id: str, scenario: Dict[str, float]):
    run = RUNS.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Model not found")
    roi_map = {r["channel"]: r["roi"] for r in run.get("roi", [])}
    contrib_map = {c["channel"]: c["mean_share"] for c in run.get("contrib", [])}
    baseline = sum(roi_map.get(ch, 0) * contrib_map.get(ch, 0) for ch in roi_map)
    new_score = sum(roi_map.get(ch, 0) * contrib_map.get(ch, 0) * scenario.get(ch, 1.0) for ch in roi_map)
    uplift = ((new_score - baseline) / baseline * 100) if baseline != 0 else 0
    return {"uplift": uplift, "predicted_kpi": new_score, "baseline": baseline}

@app.post("/api/models/{run_id}/optimize/auto")
def optimize_auto(run_id: str, request: OptimizeRequest = OptimizeRequest()):
    from scipy.optimize import minimize
    import numpy as np
    run = RUNS.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Model not found")
    roi_map = {r["channel"]: r["roi"] for r in run.get("roi", [])}
    contrib_map = {c["channel"]: c["mean_share"] for c in run.get("contrib", [])}
    if not roi_map or not contrib_map:
        raise HTTPException(status_code=400, detail="ROI or contribution data not available")

    channels = list(roi_map.keys())
    n = len(channels)
    roi_values = np.maximum(np.array([roi_map.get(ch, 0) for ch in channels]), 0.01)
    contrib_raw = np.array([contrib_map.get(ch, 0) for ch in channels])
    cs = contrib_raw.sum()
    contrib_values = contrib_raw / cs if cs > 0 else contrib_raw

    # Baseline score corresponds to all multipliers = 1.0
    baseline_score = float(np.sum(roi_values * contrib_values))

    def objective(x: np.ndarray) -> float:
        # We maximize incremental score over baseline; scipy minimizes, so negate.
        return -(np.sum(roi_values * contrib_values * x) - baseline_score)

    # Sum of multipliers constrained to total_budget * n (average = total_budget)
    constraints = ({"type": "eq", "fun": lambda x: np.sum(x) - (request.total_budget * n)},)

    # Build per-channel bounds, falling back to global min/max
    bounds: list[tuple[float, float]] = []
    per_constraints = request.channel_constraints or {}
    for ch in channels:
        c = per_constraints.get(ch)
        if c and c.locked:
            bounds.append((1.0, 1.0))
        else:
            lo = c.min if c and c.min is not None else request.min_spend
            hi = c.max if c and c.max is not None else request.max_spend
            bounds.append((float(lo), float(hi)))
    try:
        # Start at all-ones (baseline) scaled by requested average multiplier
        x0 = np.ones(n) * request.total_budget
        result = minimize(
            objective,
            x0,
            method="SLSQP",
            bounds=bounds,
            constraints=constraints,
            options={"maxiter": 1000},
        )
        if not result.success:
            return {"optimal_mix": {ch: float(request.total_budget) for ch in channels}, "predicted_kpi": baseline_score, "baseline_kpi": baseline_score, "uplift": 0.0, "message": "At baseline"}
        optimal_mix = {ch: float(val) for ch, val in zip(channels, result.x)}
        predicted = float(-result.fun + baseline_score)
        uplift = ((predicted - baseline_score) / baseline_score * 100) if baseline_score > 0 else 0
        return {"optimal_mix": optimal_mix, "predicted_kpi": max(predicted, baseline_score), "baseline_kpi": baseline_score, "uplift": max(uplift, 0), "message": f"Uplift: {max(uplift,0):.1f}%"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Optimization error: {e}")

# ==================== Ad Platform Connectors ====================

@app.get("/api/connectors/status")
def connectors_status():
    sources = [(DATA_DIR / "meta_ads.csv", "Meta"), (DATA_DIR / "google_ads.csv", "Google"), (DATA_DIR / "linkedin_ads.csv", "LinkedIn"), (DATA_DIR / "meiro_cdp.csv", "Meiro CDP"), (DATA_DIR / "unified_ads.csv", "Unified")]
    stats = {}
    for path, name in sources:
        if path.exists():
            try:
                df = pd.read_csv(path)
                stats[name] = {"path": str(path), "rows": int(len(df)), "total_spend": float(df.get("spend", pd.Series([], dtype=float)).fillna(0).sum())}
            except Exception:
                stats[name] = {"path": str(path), "rows": 0}
        else:
            stats[name] = {"path": str(path), "rows": 0}
    return stats

@app.post("/api/connectors/meta")
def fetch_meta(ad_account_id: str, since: str, until: str, avg_aov: float = 0.0, access_token: Optional[str] = None):
    if not access_token:
        token_data = get_token("meta")
        if not token_data or not token_data.get("access_token"):
            db = SessionLocal()
            try:
                token_from_conn = get_access_token_for_provider(db, workspace_id="default", provider_key="meta_ads")
            finally:
                db.close()
            if not token_from_conn:
                raise HTTPException(status_code=401, detail="No Meta access token. Connect your Meta account first.")
            access_token = token_from_conn
        else:
            access_token = token_data["access_token"]
    out_path = DATA_DIR / "meta_ads.csv"
    url = f"https://graph.facebook.com/v19.0/{ad_account_id}/ads"
    params = {"access_token": access_token, "fields": "campaign_name,spend,impressions,clicks,actions,updated_time", "time_range": json.dumps({"since": since, "until": until}), "limit": 500}
    rows = []
    try:
        while True:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            for ad in data.get("data", []):
                actions = ad.get("actions", []) or []
                purchases = next((a for a in actions if a.get("action_type") == "purchase"), None)
                conv = float(purchases.get("value", 0)) if purchases else 0.0
                rows.append({"date": since, "channel": "meta_ads", "campaign": ad.get("campaign_name", "unknown"), "spend": float(ad.get("spend", 0) or 0), "impressions": int(ad.get("impressions", 0) or 0), "clicks": int(ad.get("clicks", 0) or 0), "conversions": float(conv), "revenue": float(conv * float(avg_aov))})
            next_url = data.get("paging", {}).get("next")
            if not next_url:
                break
            url = next_url
            params = {}
    except Exception:
        pass
    pd.DataFrame(rows).to_csv(out_path, index=False)
    total_spend = sum(r.get("spend", 0) for r in rows)
    if total_spend > 0:
        expense_id = f"meta_ads_{since[:7]}"
        EXPENSES[expense_id] = _with_converted_amount(
            ExpenseEntry(
                channel="meta_ads",
                cost_type="Media Spend",
                amount=total_spend,
                currency="USD",
                reporting_currency=_default_reporting_currency(),
                period=since[:7],
                service_period_start=since,
                service_period_end=until,
                notes="Auto-imported from Meta Ads API",
                source_type="import",
                source_name="meta_ads",
                actor_type="import",
                created_at=_now_iso(),
            )
        )
    now_iso = _now_iso()
    IMPORT_SYNC_STATE["meta_ads"] = {
        "last_success_at": now_iso,
        "last_attempt_at": now_iso,
        "status": "Healthy",
        "records_imported": len(rows),
        "period_start": since,
        "period_end": until,
        "platform_total": total_spend,
        "last_error": None,
        "action_hint": None,
    }
    return {"rows": len(rows), "path": str(out_path)}

@app.post("/api/connectors/google")
def fetch_google(segments_date_from: str, segments_date_to: str):
    out_path = DATA_DIR / "google_ads.csv"
    if not out_path.exists():
        pd.DataFrame([], columns=["date", "channel", "campaign", "spend", "impressions", "clicks", "conversions", "revenue"]).to_csv(out_path, index=False)
    rows = int(pd.read_csv(out_path).shape[0])
    now_iso = _now_iso()
    IMPORT_SYNC_STATE["google_ads"] = {
        "last_success_at": now_iso,
        "last_attempt_at": now_iso,
        "status": "Healthy",
        "records_imported": rows,
        "period_start": segments_date_from,
        "period_end": segments_date_to,
        "platform_total": None,
        "last_error": None,
        "action_hint": None,
    }
    return {"rows": rows, "path": str(out_path)}

@app.post("/api/connectors/linkedin")
def fetch_linkedin(since: str, until: str, access_token: Optional[str] = None):
    if not access_token:
        token_data = get_token("linkedin")
        if not token_data or not token_data.get("access_token"):
            db = SessionLocal()
            try:
                token_from_conn = get_access_token_for_provider(db, workspace_id="default", provider_key="linkedin_ads")
            finally:
                db.close()
            if not token_from_conn:
                raise HTTPException(status_code=401, detail="No LinkedIn access token. Connect your LinkedIn account first.")
            access_token = token_from_conn
        else:
            access_token = token_data["access_token"]
    out_path = DATA_DIR / "linkedin_ads.csv"
    headers = {"Authorization": f"Bearer {access_token}"}
    rows = []
    try:
        r = requests.get("https://api.linkedin.com/v2/adAnalyticsV2", headers=headers, params={"q": "analytics", "pivot": "CAMPAIGN", "timeGranularity": "DAILY"}, timeout=30)
        if r.ok:
            for el in r.json().get("elements", []):
                rows.append({"date": since, "channel": "linkedin_ads", "campaign": (el.get("campaign", {}) or {}).get("name", "unknown"), "spend": float(el.get("costInLocalCurrency", 0) or 0), "impressions": int(el.get("impressions", 0) or 0), "clicks": int(el.get("clicks", 0) or 0), "conversions": float(el.get("conversions", 0) or 0), "revenue": float(el.get("revenueValue", 0) or 0)})
    except Exception:
        pass
    pd.DataFrame(rows).to_csv(out_path, index=False)
    total_spend = sum(r.get("spend", 0) for r in rows)
    if total_spend > 0:
        expense_id = f"linkedin_ads_{since[:7]}"
        EXPENSES[expense_id] = _with_converted_amount(
            ExpenseEntry(
                channel="linkedin_ads",
                cost_type="Media Spend",
                amount=total_spend,
                currency="USD",
                reporting_currency=_default_reporting_currency(),
                period=since[:7],
                service_period_start=since,
                service_period_end=until,
                notes="Auto-imported from LinkedIn Ads API",
                source_type="import",
                source_name="linkedin_ads",
                actor_type="import",
                created_at=_now_iso(),
            )
        )
    now_iso = _now_iso()
    IMPORT_SYNC_STATE["linkedin_ads"] = {
        "last_success_at": now_iso,
        "last_attempt_at": now_iso,
        "status": "Healthy",
        "records_imported": len(rows),
        "period_start": since,
        "period_end": until,
        "platform_total": total_spend,
        "last_error": None,
        "action_hint": None,
    }
    return {"rows": len(rows), "path": str(out_path)}

@app.post("/api/connectors/merge")
def merge_ads():
    sources = [DATA_DIR / "meta_ads.csv", DATA_DIR / "google_ads.csv", DATA_DIR / "linkedin_ads.csv", DATA_DIR / "meiro_cdp.csv"]
    frames = [pd.read_csv(p) for p in sources if p.exists()]
    unified = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame([], columns=["date", "channel", "campaign", "spend", "impressions", "clicks", "conversions", "revenue"])
    for col in ["spend", "impressions", "clicks", "conversions", "revenue"]:
        unified[col] = pd.to_numeric(unified.get(col, 0), errors='coerce').fillna(0)
    unified["date"] = pd.to_datetime(unified.get("date", pd.to_datetime([])), errors='coerce').dt.date.astype(str)
    unified.drop_duplicates(subset=["date", "channel", "campaign"], keep='last', inplace=True)
    out_path = DATA_DIR / "unified_ads.csv"
    unified.to_csv(out_path, index=False)
    return {"rows": int(len(unified)), "path": str(out_path)}

# ==================== Meiro CDP Connector Routes ====================

@app.post("/api/connectors/meiro/test")
def meiro_test(req: MeiroCDPTestRequest = MeiroCDPTestRequest()):
    """Test connection without saving. Optionally save on success."""
    api_base_url = req.api_base_url
    api_key = req.api_key
    if not api_base_url or not api_key:
        cfg = meiro_cdp.get_config()
        if not cfg:
            raise HTTPException(status_code=400, detail="No saved credentials. Provide api_base_url and api_key.")
        api_base_url = cfg["api_base_url"]
        api_key = cfg["api_key"]
    result = meiro_cdp.test_connection(api_base_url, api_key)
    if result.get("ok"):
        if req.save_on_success:
            meiro_cdp.save_config(api_base_url, api_key)
        meiro_cdp.update_last_test_at()
        return {"ok": True, "message": "Connection successful"}
    raise HTTPException(status_code=400, detail=result.get("message", "Connection failed"))


@app.post("/api/connectors/meiro/connect")
def meiro_connect(req: MeiroCDPConnectRequest):
    result = meiro_cdp.test_connection(req.api_base_url, req.api_key)
    if result.get("ok"):
        meiro_cdp.save_config(req.api_base_url, req.api_key)
        meiro_cdp.update_last_test_at()
        return {"message": "Connected to Meiro CDP", "connected": True}
    raise HTTPException(status_code=400, detail=result.get("message", "Connection failed"))


@app.post("/api/connectors/meiro/save")
def meiro_save(req: MeiroCDPConnectRequest):
    """Save credentials without testing. Use with caution."""
    meiro_cdp.save_config(req.api_base_url, req.api_key)
    return {"message": "Credentials saved"}


@app.delete("/api/connectors/meiro")
def meiro_disconnect():
    if meiro_cdp.disconnect():
        return {"message": "Disconnected from Meiro CDP"}
    raise HTTPException(status_code=404, detail="No Meiro CDP connection found")

@app.get("/api/connectors/meiro/status")
def meiro_status():
    return {"connected": meiro_cdp.is_connected()}


@app.get("/api/connectors/meiro/config")
def meiro_config():
    """Full config for UI: connection status, last test, webhook URL, webhook stats. Never returns API key."""
    meta = meiro_cdp.get_connection_metadata()
    webhook_url = f"{BASE_URL}/api/connectors/meiro/profiles"
    return {
        "connected": meiro_cdp.is_connected(),
        "api_base_url": meta["api_base_url"] if meta else None,
        "last_test_at": meta["last_test_at"] if meta else get_last_test_at(),
        "has_key": meta["has_key"] if meta else False,
        "webhook_url": webhook_url,
        "webhook_last_received_at": get_webhook_last_received_at(),
        "webhook_received_count": get_webhook_received_count(),
        "webhook_has_secret": bool(get_webhook_secret() or os.getenv("MEIRO_WEBHOOK_SECRET", "").strip()),
    }

@app.get("/api/connectors/meiro/attributes")
def meiro_attributes():
    if not meiro_cdp.is_connected():
        raise HTTPException(status_code=401, detail="Meiro CDP not connected")
    return meiro_cdp.list_attributes()

@app.get("/api/connectors/meiro/events")
def meiro_events():
    if not meiro_cdp.is_connected():
        raise HTTPException(status_code=401, detail="Meiro CDP not connected")
    return meiro_cdp.list_events()

@app.get("/api/connectors/meiro/segments")
def meiro_segments():
    if not meiro_cdp.is_connected():
        raise HTTPException(status_code=401, detail="Meiro CDP not connected")
    return meiro_cdp.list_segments()

@app.post("/api/connectors/meiro/fetch")
def meiro_fetch(req: MeiroCDPExportRequest):
    if not meiro_cdp.is_connected():
        raise HTTPException(status_code=401, detail="Meiro CDP not connected")
    try:
        df = meiro_cdp.fetch_and_transform(since=req.since, until=req.until, event_types=req.event_types, attributes=req.attributes, segment_id=req.segment_id)
        out_path = DATA_DIR / "meiro_cdp.csv"
        df.to_csv(out_path, index=False)
        DATASETS["meiro-cdp-export"] = {"path": out_path, "type": "sales"}
        return {"rows": len(df), "path": str(out_path), "dataset_id": "meiro-cdp-export"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/connectors/meiro/profiles")
def meiro_profiles_status():
    """Return count of profiles stored from Meiro CDP webhook and the URL Meiro should POST to."""
    out_path = DATA_DIR / "meiro_cdp_profiles.json"
    count = 0
    if out_path.exists():
        try:
            data = json.loads(out_path.read_text())
            count = len(data) if isinstance(data, list) else 0
        except Exception:
            pass
    webhook_url = f"{BASE_URL}/api/connectors/meiro/profiles"
    return {
        "stored_count": count,
        "webhook_url": webhook_url,
        "webhook_last_received_at": get_webhook_last_received_at(),
        "webhook_received_count": get_webhook_received_count(),
    }


@app.post("/api/connectors/meiro/profiles")
async def meiro_receive_profiles(
    request: Request,
    x_meiro_webhook_secret: Optional[str] = Header(None, alias="X-Meiro-Webhook-Secret"),
):
    """
    Webhook endpoint for Meiro CDP to push customer profiles.

    Meiro CDP can POST profiles here (e.g. from a flow or export). Stored profiles
    are used when you click "Import from CDP" in Data Sources.

    Body (JSON):
      - Array of profile objects: [ { "customer_id": "...", "touchpoints": [...], ... }, ... ]
      - Or object: { "profiles": [ ... ], "replace": true }
      - Or journeys envelope: { "schema_version": "2.0", "journeys": [ ... ], "defaults": {...} }
        - replace: true (default) = replace stored profiles with this payload
        - replace: false = append to existing profiles

    Optional security:
      - Set env MEIRO_WEBHOOK_SECRET or rotate secret in UI; Meiro must send header X-Meiro-Webhook-Secret.
    """
    webhook_secret = get_webhook_secret() or os.getenv("MEIRO_WEBHOOK_SECRET", "").strip()
    if webhook_secret:
        if not x_meiro_webhook_secret or x_meiro_webhook_secret != webhook_secret:
            raise HTTPException(status_code=401, detail="Invalid or missing X-Meiro-Webhook-Secret")

    try:
        body = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    def _safe_json_excerpt(value: Any, max_chars: int = 20000) -> tuple[str, bool, int]:
        try:
            raw = json.dumps(value, ensure_ascii=False, indent=2)
        except Exception:
            raw = str(value)
        raw_bytes = len(raw.encode("utf-8", errors="ignore"))
        if len(raw) <= max_chars:
            return raw, False, raw_bytes
        suffix = "\n... [truncated]"
        return f"{raw[:max_chars]}{suffix}", True, raw_bytes

    def _extract_payload_hints(payload_profiles: list[Any]) -> tuple[list[str], list[str]]:
        conversion_names: set[str] = set()
        channels: set[str] = set()
        for profile in payload_profiles[:100]:
            if not isinstance(profile, dict):
                continue
            for conversion in (profile.get("conversions") or [])[:100]:
                if not isinstance(conversion, dict):
                    continue
                name = conversion.get("name")
                if isinstance(name, str) and name.strip():
                    conversion_names.add(name.strip())
            for touchpoint in (profile.get("touchpoints") or [])[:100]:
                if not isinstance(touchpoint, dict):
                    continue
                channel = touchpoint.get("channel")
                if isinstance(channel, str) and channel.strip():
                    channels.add(channel.strip())
        return sorted(conversion_names)[:20], sorted(channels)[:20]

    def _analyze_payload(payload_profiles: list[Any]) -> Dict[str, Any]:
        conversion_event_counts: Dict[str, int] = {}
        channel_counts: Dict[str, int] = {}
        source_counts: Dict[str, int] = {}
        medium_counts: Dict[str, int] = {}
        campaign_counts: Dict[str, int] = {}
        value_field_counts: Dict[str, int] = {}
        currency_field_counts: Dict[str, int] = {}
        dedup_key_counts: Dict[str, int] = {"conversion_id": 0, "order_id": 0, "event_id": 0}
        conversion_count = 0
        touchpoint_count = 0

        def _inc(target: Dict[str, int], key: Optional[str]) -> None:
            if not isinstance(key, str):
                return
            normalized = key.strip()
            if not normalized:
                return
            target[normalized] = target.get(normalized, 0) + 1

        for profile in payload_profiles[:200]:
            if not isinstance(profile, dict):
                continue
            conversions = profile.get("conversions") or []
            if isinstance(conversions, list):
                for conversion in conversions[:300]:
                    if not isinstance(conversion, dict):
                        continue
                    conversion_count += 1
                    _inc(conversion_event_counts, str(conversion.get("name") or "").strip().lower())
                    for dedup_key in ("conversion_id", "order_id", "event_id"):
                        if conversion.get(dedup_key):
                            dedup_key_counts[dedup_key] = dedup_key_counts.get(dedup_key, 0) + 1
                    for key, value in conversion.items():
                        if key in {"id", "name", "ts", "timestamp"}:
                            continue
                        if isinstance(value, (int, float)):
                            value_field_counts[key] = value_field_counts.get(key, 0) + 1
                        elif isinstance(value, str):
                            raw = value.strip()
                            if len(raw) == 3 and raw.upper() == raw and raw.isalpha():
                                currency_field_counts[key] = currency_field_counts.get(key, 0) + 1

            touchpoints = profile.get("touchpoints") or []
            if isinstance(touchpoints, list):
                for tp in touchpoints[:500]:
                    if not isinstance(tp, dict):
                        continue
                    touchpoint_count += 1
                    _inc(channel_counts, tp.get("channel"))
                    _inc(source_counts, tp.get("source"))
                    _inc(medium_counts, tp.get("medium"))
                    _inc(campaign_counts, tp.get("campaign"))
                    utm = tp.get("utm")
                    if isinstance(utm, dict):
                        _inc(source_counts, utm.get("source"))
                        _inc(medium_counts, utm.get("medium"))
                        _inc(campaign_counts, utm.get("campaign"))
                    source_obj = tp.get("source")
                    if isinstance(source_obj, dict):
                        _inc(source_counts, source_obj.get("platform"))
                        _inc(campaign_counts, source_obj.get("campaign_name"))
                    campaign_obj = tp.get("campaign")
                    if isinstance(campaign_obj, dict):
                        _inc(campaign_counts, campaign_obj.get("name"))

        return {
            "conversion_count": conversion_count,
            "touchpoint_count": touchpoint_count,
            "conversion_event_counts": conversion_event_counts,
            "channel_counts": channel_counts,
            "source_counts": source_counts,
            "medium_counts": medium_counts,
            "campaign_counts": campaign_counts,
            "value_field_counts": value_field_counts,
            "currency_field_counts": currency_field_counts,
            "dedup_key_counts": dedup_key_counts,
        }

    if isinstance(body, list):
        profiles = body
        replace = True
    elif isinstance(body, dict):
        profiles = body.get("profiles")
        if not isinstance(profiles, list):
            data_payload = body.get("data")
            if isinstance(data_payload, list):
                profiles = data_payload
            elif isinstance(data_payload, dict):
                nested_profiles = data_payload.get("profiles")
                profiles = nested_profiles if isinstance(nested_profiles, list) else None
        if not isinstance(profiles, list):
            journeys_payload = body.get("journeys")
            if isinstance(journeys_payload, list):
                profiles = journeys_payload
        if not isinstance(profiles, list):
            profiles = []
        replace = body.get("replace", True)
        if not isinstance(profiles, list):
            raise HTTPException(
                status_code=400,
                detail="Body must be an array, { 'profiles': [...] }, { 'data': [...] }, or { 'journeys': [...] }",
            )
    else:
        raise HTTPException(status_code=400, detail="Body must be JSON array or object with 'profiles' key")

    out_path = DATA_DIR / "meiro_cdp_profiles.json"
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if replace or not out_path.exists():
        to_store = profiles
    else:
        try:
            existing = json.loads(out_path.read_text())
            to_store = (existing if isinstance(existing, list) else []) + list(profiles)
        except Exception:
            to_store = profiles

    out_path.write_text(json.dumps(to_store, indent=2))
    now_iso = datetime.utcnow().isoformat() + "Z"
    set_webhook_received(count_delta=len(profiles), last_received_at=now_iso)
    payload_excerpt, payload_truncated, payload_bytes = _safe_json_excerpt(body)
    conversion_names, channels_detected = _extract_payload_hints(profiles)
    payload_analysis = _analyze_payload(profiles)
    append_webhook_event(
        {
            "received_at": now_iso,
            "received_count": int(len(profiles)),
            "stored_total": int(len(to_store)),
            "replace": bool(replace),
            "ip": request.client.host if request.client else None,
            "user_agent": (request.headers.get("user-agent") or "")[:256] or None,
            "payload_shape": "array" if isinstance(body, list) else "object",
            "payload_excerpt": payload_excerpt,
            "payload_truncated": payload_truncated,
            "payload_bytes": payload_bytes,
            "payload_json_valid": True,
            "conversion_event_names": conversion_names,
            "channels_detected": channels_detected,
            "payload_analysis": payload_analysis,
        },
        max_items=100,
    )
    return {"received": len(profiles), "stored_total": len(to_store), "message": "Profiles saved. Use Import from CDP in Data Sources to load into attribution."}


@app.post("/api/connectors/meiro/webhook/rotate-secret")
def meiro_webhook_rotate():
    """Rotate webhook secret. Returns new secret (show once)."""
    secret = rotate_webhook_secret()
    return {"message": "Webhook secret rotated", "secret": secret}


@app.get("/api/connectors/meiro/webhook/events")
def meiro_webhook_events(
    limit: int = Query(100, ge=1, le=500),
    include_payload: bool = Query(False, description="Include payload excerpts for debugging"),
):
    events = get_webhook_events(limit=limit)
    if not include_payload:
        events = [
            {
                k: v
                for k, v in event.items()
                if k not in {"payload_excerpt"}
            }
            for event in events
        ]
    return {"items": events, "total": len(events)}


@app.get("/api/connectors/meiro/webhook/suggestions")
def meiro_webhook_suggestions(limit: int = Query(100, ge=1, le=500)):
    events = get_webhook_events(limit=limit)
    conversion_event_counts: Dict[str, int] = {}
    channel_counts: Dict[str, int] = {}
    source_counts: Dict[str, int] = {}
    medium_counts: Dict[str, int] = {}
    value_field_counts: Dict[str, int] = {}
    dedup_key_counts: Dict[str, int] = {"conversion_id": 0, "order_id": 0, "event_id": 0}
    total_conversions = 0
    total_touchpoints = 0

    def _merge_counts(target: Dict[str, int], incoming: Dict[str, Any]) -> None:
        for k, v in (incoming or {}).items():
            try:
                iv = int(v)
            except Exception:
                continue
            if not k:
                continue
            target[k] = target.get(k, 0) + iv

    for event in events:
        analysis = event.get("payload_analysis") if isinstance(event, dict) else None
        if not isinstance(analysis, dict):
            continue
        total_conversions += int(analysis.get("conversion_count") or 0)
        total_touchpoints += int(analysis.get("touchpoint_count") or 0)
        _merge_counts(conversion_event_counts, analysis.get("conversion_event_counts") or {})
        _merge_counts(channel_counts, analysis.get("channel_counts") or {})
        _merge_counts(source_counts, analysis.get("source_counts") or {})
        _merge_counts(medium_counts, analysis.get("medium_counts") or {})
        _merge_counts(value_field_counts, analysis.get("value_field_counts") or {})
        for key in ("conversion_id", "order_id", "event_id"):
            dedup_key_counts[key] = dedup_key_counts.get(key, 0) + int((analysis.get("dedup_key_counts") or {}).get(key, 0) or 0)

    def _top_items(values: Dict[str, int], n: int = 10) -> List[Tuple[str, int]]:
        return sorted(values.items(), key=lambda kv: kv[1], reverse=True)[:n]

    top_conversion_names = _top_items(conversion_event_counts, n=10)
    top_value_fields = _top_items(value_field_counts, n=5)
    top_sources = _top_items(source_counts, n=15)
    top_mediums = _top_items(medium_counts, n=15)
    top_channels = _top_items(channel_counts, n=15)
    best_dedup_key = sorted(dedup_key_counts.items(), key=lambda kv: kv[1], reverse=True)[0][0]

    kpi_suggestions = []
    for idx, (event_name, count) in enumerate(top_conversion_names):
        if not event_name:
            continue
        event_id = re.sub(r"[^a-z0-9_]+", "_", event_name.lower()).strip("_")[:64] or f"event_{idx+1}"
        coverage = (count / total_conversions) if total_conversions > 0 else 0.0
        value_field = top_value_fields[0][0] if top_value_fields else None
        kpi_suggestions.append(
            {
                "id": event_id,
                "label": event_name.replace("_", " ").title(),
                "type": "primary" if idx == 0 else "micro",
                "event_name": event_name,
                "value_field": value_field if idx == 0 else None,
                "weight": 1.0 if idx == 0 else 0.5,
                "lookback_days": 30 if idx == 0 else 14,
                "coverage_pct": round(coverage * 100, 2),
            }
        )

    primary_kpi_id = kpi_suggestions[0]["id"] if kpi_suggestions else None

    taxonomy_rules = []
    priority = 10
    for channel, count in top_channels:
        if channel.lower() in {"unknown", "other", "(none)"}:
            continue
        taxonomy_rules.append(
            {
                "name": f"Auto: {channel}",
                "channel": channel,
                "priority": priority,
                "enabled": True,
                "source": {"operator": "any", "value": ""},
                "medium": {"operator": "any", "value": ""},
                "campaign": {"operator": "any", "value": ""},
                "observed_count": count,
            }
        )
        priority += 10
        if len(taxonomy_rules) >= 12:
            break

    source_aliases = {}
    for source, _count in top_sources:
        lower = source.lower()
        if lower in {"fb", "ig", "yt", "li"}:
            mapped = {"fb": "facebook", "ig": "instagram", "yt": "youtube", "li": "linkedin"}[lower]
            source_aliases[lower] = mapped
    medium_aliases = {}
    for medium, _count in top_mediums:
        lower = medium.lower()
        if lower in {"ppc", "paid", "paidsearch"}:
            medium_aliases[lower] = "cpc"

    kpi_apply_payload = {
        "definitions": [
            {
                "id": item["id"],
                "label": item["label"],
                "type": item["type"],
                "event_name": item["event_name"],
                "value_field": item["value_field"],
                "weight": item["weight"],
                "lookback_days": item["lookback_days"],
            }
            for item in kpi_suggestions
        ],
        "primary_kpi_id": primary_kpi_id,
    }
    taxonomy_apply_payload = {
        "channel_rules": [
            {k: v for k, v in rule.items() if k != "observed_count"}
            for rule in taxonomy_rules
        ],
        "source_aliases": source_aliases,
        "medium_aliases": medium_aliases,
    }

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "events_analyzed": len(events),
        "total_conversions_observed": total_conversions,
        "total_touchpoints_observed": total_touchpoints,
        "dedup_key_suggestion": best_dedup_key,
        "kpi_suggestions": kpi_suggestions,
        "conversion_event_suggestions": [
            {"event_name": name, "count": count}
            for name, count in top_conversion_names
        ],
        "taxonomy_suggestions": {
            "channel_rules": taxonomy_rules,
            "source_aliases": source_aliases,
            "medium_aliases": medium_aliases,
            "top_sources": [{"source": name, "count": count} for name, count in top_sources],
            "top_mediums": [{"medium": name, "count": count} for name, count in top_mediums],
        },
        "apply_payloads": {
            "kpis": kpi_apply_payload,
            "taxonomy": taxonomy_apply_payload,
        },
    }


MEIRO_MAPPING_PRESETS = {
    "web_ads_ga": {
        "name": "Web + Ads (GA-like)",
        "touchpoint_attr": "touchpoints",
        "value_attr": "conversion_value",
        "id_attr": "customer_id",
        "channel_field": "channel",
        "timestamp_field": "timestamp",
        "channel_mapping": {
            "google": "google_ads",
            "facebook": "meta_ads",
            "meta": "meta_ads",
            "linkedin": "linkedin_ads",
            "cpc": "paid_search",
            "ppc": "paid_search",
            "organic": "organic_search",
            "email": "email",
            "direct": "direct",
        },
    },
    "crm_lifecycle": {
        "name": "CRM + Lifecycle",
        "touchpoint_attr": "touchpoints",
        "value_attr": "conversion_value",
        "id_attr": "customer_id",
        "channel_field": "source",
        "timestamp_field": "event_date",
        "channel_mapping": {
            "salesforce": "crm",
            "hubspot": "crm",
            "marketo": "marketing_automation",
            "pardot": "marketing_automation",
            "newsletter": "email",
            "onboarding": "lifecycle",
            "retention": "lifecycle",
            "winback": "lifecycle",
        },
    },
}


@app.get("/api/connectors/meiro/mapping")
def meiro_get_mapping():
    return {"mapping": get_mapping(), "presets": MEIRO_MAPPING_PRESETS}


@app.post("/api/connectors/meiro/mapping")
def meiro_save_mapping(mapping: dict):
    save_mapping(mapping)
    return {"message": "Mapping saved"}


@app.get("/api/connectors/meiro/pull-config")
def meiro_get_pull_config():
    return get_pull_config()


@app.post("/api/connectors/meiro/pull-config")
def meiro_save_pull_config(config: dict):
    save_pull_config(config)
    return {"message": "Pull config saved"}


@app.post("/api/connectors/meiro/pull")
def meiro_pull(since: Optional[str] = None, until: Optional[str] = None, db=Depends(get_db)):
    """Pull mode: fetch events from Meiro API, build journeys, persist."""
    if not meiro_cdp.is_connected():
        raise HTTPException(status_code=401, detail="Meiro CDP not connected")
    from datetime import datetime, timedelta
    today = datetime.utcnow().date()
    pull_cfg = get_pull_config()
    lookback = pull_cfg.get("lookback_days", 30)
    start = (today - timedelta(days=lookback)).isoformat()
    end = today.isoformat()
    since = since or start
    until = until or end
    saved = get_mapping()
    try:
        records = meiro_cdp.fetch_raw_events(since=since, until=until)
    except Exception as e:
        _append_import_run("meiro_pull", 0, "error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    journeys = meiro_cdp.build_journeys_from_events(
        records,
        id_attr=saved.get("id_attr", "customer_id"),
        timestamp_attr=saved.get("timestamp_field", "timestamp"),
        channel_attr=saved.get("channel_field", "channel"),
        conversion_selector=pull_cfg.get("conversion_selector", "purchase"),
        session_gap_minutes=pull_cfg.get("session_gap_minutes", 30),
        dedup_interval_minutes=pull_cfg.get("dedup_interval_minutes", 5),
        channel_mapping=saved.get("channel_mapping"),
    )
    global JOURNEYS
    JOURNEYS = journeys
    persist_journeys_as_conversion_paths(db, JOURNEYS, replace=True)
    _refresh_journey_aggregates_after_import(db)
    converted = sum(1 for j in journeys if j.get("converted", True))
    ch_set = set()
    for j in journeys:
        for tp in j.get("touchpoints", []):
            ch_set.add(tp.get("channel", "unknown"))
    _append_import_run(
        "meiro_pull", len(journeys), "success",
        total=len(journeys), valid=len(journeys), converted=converted, channels_detected=sorted(ch_set),
        config_snapshot={"lookback_days": pull_cfg.get("lookback_days"), "session_gap_minutes": pull_cfg.get("session_gap_minutes"), "conversion_selector": pull_cfg.get("conversion_selector")},
        preview_rows=[{"customer_id": j.get("customer_id", "?"), "touchpoints": len(j.get("touchpoints", [])), "value": j.get("conversion_value", 0)} for j in journeys[:20]],
    )
    return {"count": len(journeys), "message": f"Pulled {len(journeys)} journeys from Meiro"}


@app.post("/api/connectors/meiro/dry-run")
def meiro_dry_run(limit: int = 100):
    """Dry run: parse first N profiles, validate, return preview and warnings. No persist."""
    cdp_json_path = DATA_DIR / "meiro_cdp_profiles.json"
    cdp_path = DATA_DIR / "meiro_cdp.csv"
    saved = get_mapping()
    mapping = AttributionMappingConfig(
        touchpoint_attr=saved.get("touchpoint_attr", "touchpoints"),
        value_attr=saved.get("value_attr", "conversion_value"),
        id_attr=saved.get("id_attr", "customer_id"),
        channel_field=saved.get("channel_field", "channel"),
        timestamp_field=saved.get("timestamp_field", "timestamp"),
    )
    if cdp_json_path.exists():
        with open(cdp_json_path) as f:
            profiles = json.load(f)
    elif cdp_path.exists():
        df = pd.read_csv(cdp_path)
        profiles = df.to_dict(orient="records")
    else:
        raise HTTPException(status_code=404, detail="No CDP data found. Fetch or push profiles first.")
    profiles = profiles[:limit] if isinstance(profiles, list) else []
    if not profiles:
        return {"count": 0, "preview": [], "warnings": ["No profiles to process"], "validation": {}}
    try:
        journeys = parse_conversion_paths(
            profiles,
            touchpoint_attr=mapping.touchpoint_attr,
            value_attr=mapping.value_attr,
            id_attr=mapping.id_attr,
            channel_field=mapping.channel_field,
            timestamp_field=mapping.timestamp_field,
        )
    except Exception as e:
        return {"count": 0, "preview": [], "warnings": [str(e)], "validation": {"error": str(e)}}
    warnings = []
    if journeys:
        sample_channels = set()
        for j in journeys:
            for tp in j.get("touchpoints", []):
                ch = tp.get("channel") if isinstance(tp, dict) else None
                if ch:
                    sample_channels.add(ch)
        if not any(c for c in sample_channels if "email" in str(c).lower() or "click" in str(c).lower()):
            warnings.append("No email click tracking detected; channel coverage may be incomplete")
    preview = [{"id": j.get("customer_id", j.get("id", "?")), "touchpoints": len(j.get("touchpoints", [])), "value": j.get("conversion_value", 0)} for j in journeys[:20]]
    return {"count": len(journeys), "preview": preview, "warnings": warnings, "validation": {"ok": len(warnings) == 0}}


# ==================== Model Fitting Background Task ====================

def _fit_model(run_id: str, cfg: ModelConfig):
    now_ts = _now_iso()
    dataset_info = DATASETS.get(cfg.dataset_id)
    if not dataset_info:
        RUNS[run_id] = {**RUNS.get(run_id, {}), "status": "error", "detail": "Dataset not found", "updated_at": now_ts}
        _save_runs()
        return
    csv_path = dataset_info.get("path")
    p = Path(csv_path) if isinstance(csv_path, str) else csv_path
    df = pd.read_csv(p, parse_dates=["date"])
    if cfg.kpi not in df.columns:
        RUNS[run_id] = {**RUNS.get(run_id, {}), "status": "error", "detail": f"Column '{cfg.kpi}' missing", "updated_at": now_ts}
        _save_runs()
        return
    is_tall = {"channel", "campaign", "spend"}.issubset(set(df.columns))
    if not is_tall:
        for c in cfg.spend_channels:
            if c not in df.columns:
                RUNS[run_id] = {**RUNS.get(run_id, {}), "status": "error", "detail": f"Column '{c}' missing", "updated_at": now_ts}
                _save_runs()
                return
    priors = cfg.priors or {}
    adstock_cfg = {"l_max": 8, "alpha_mean": priors.get("adstock", {}).get("alpha_mean", 0.5), "alpha_sd": priors.get("adstock", {}).get("alpha_sd", 0.2)}
    saturation_cfg = {"lam_mean": priors.get("saturation", {}).get("lam_mean", 0.001), "lam_sd": priors.get("saturation", {}).get("lam_sd", 0.0005)}
    mcmc_cfg = cfg.mcmc or {"draws": 1000, "tune": 1000, "chains": 4, "target_accept": 0.9}
    use_adstock = getattr(cfg, "use_adstock", True)
    use_saturation = getattr(cfg, "use_saturation", True)
    force_engine = "ridge" if (not use_adstock and not use_saturation) else None
    random_seed = getattr(cfg, "random_seed", None)
    try:
        RUNS[run_id]["status"] = "running"
        RUNS[run_id]["updated_at"] = _now_iso()
        _save_runs()
        result = mmm_fit_model(
            df=df,
            target_column=cfg.kpi,
            channel_columns=cfg.spend_channels,
            control_columns=cfg.covariates or [],
            date_column="date",
            adstock_cfg=adstock_cfg,
            saturation_cfg=saturation_cfg,
            mcmc_cfg=mcmc_cfg,
            force_engine=force_engine,
            random_seed=random_seed,
        )
        RUNS[run_id] = {
            **RUNS[run_id],
            "status": "finished",
            "r2": result["r2"],
            "contrib": result["contrib"],
            "roi": result["roi"],
            "engine": result.get("engine", "unknown"),
            "updated_at": _now_iso(),
        }
        for key in ("campaigns", "channel_summary", "adstock_params", "saturation_params", "diagnostics"):
            if key in result:
                RUNS[run_id][key] = result[key]
        _save_runs()
    except Exception as exc:
        RUNS[run_id] = {
            **RUNS.get(run_id, {}),
            "status": "error",
            "detail": str(exc),
            "config": RUNS[run_id].get("config", {}),
            "kpi_mode": getattr(cfg, "kpi_mode", "conversions"),
            "updated_at": _now_iso(),
        }
        _save_runs()
