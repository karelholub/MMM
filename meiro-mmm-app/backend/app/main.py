from fastapi import FastAPI, BackgroundTasks, UploadFile, File, Form, HTTPException, Query, Body, Request, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, Response, JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Tuple
from contextlib import contextmanager
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
from sqlalchemy import func

logger = logging.getLogger(__name__)
from app.utils.token_store import save_token, get_token, delete_token, get_all_connected_platforms
from app.utils.encrypt import encrypt, decrypt
from app.utils import datasource_config as ds_config
from app.utils.taxonomy import load_taxonomy
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
    validate_model_config,
)
from app.services_model_config_suggestions import suggest_model_config_from_journeys
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
from app.services_auth import SESSION_COOKIE_NAME, resolve_auth_context, verify_csrf
from app.core.permissions import (
    PermissionContext,
    require_any_permission,
    require_permission,
    workspace_scope_or_403,
)
from app.services_data_quality import compute_dq_snapshots, evaluate_alert_rules
from app.services_conversions import (
    apply_model_config_to_journeys,
    load_journeys_from_db,
    persist_journeys_as_conversion_paths,
)
from app.services_journey_ingestion import MEIRO_PARSER_VERSION, canonicalize_meiro_profiles, validate_and_normalize, detect_schema
from app.services_import_runs import (
    create_run as create_import_run,
    get_runs as get_import_runs,
    get_run as get_import_run,
    get_last_successful_run,
)
from app.services_journey_definitions import (
    ensure_default_journey_definition,
    get_journey_definition,
)
from app.services_journey_aggregates import run_daily_journey_aggregates
from app.services_journeys_health import (
    build_journeys_preview,
    build_journeys_summary,
)
from app.modules.journeys.router import create_router as create_journeys_router
from app.modules.performance.router import create_router as create_performance_router
from app.modules.settings.router import create_router as create_settings_router
from app.modules.admin_access.router import create_router as create_admin_access_router
from app.modules.auth_access.router import create_router as create_auth_access_router
from app.modules.alerts_funnels.router import create_router as create_alerts_funnels_router
from app.modules.attribution.router import create_router as create_attribution_router
from app.modules.attribution.schemas import LoadSampleRequest
from app.modules.admin_access.schemas import (
    AdminInvitationCreatePayload,
    AdminMembershipUpdatePayload,
    AdminRoleCreatePayload,
    AdminRoleUpdatePayload,
    AdminUserUpdatePayload,
    InvitationAcceptPayload,
)
from app.modules.settings.schemas import (
    AdsGovernanceSettings,
    AttributionSettings,
    FeatureFlags,
    KpiConfigModel,
    MMMSettings,
    NBASettings,
    RevenueConfig,
    Settings,
)
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
    get_access_token_for_provider,
)
from app.services_ads_ops import (
    ENTITY_TYPES as ADS_ENTITY_TYPES,
    PROVIDER_KEYS as ADS_PROVIDER_KEYS,
    approve_change_request as ads_approve_change_request,
    apply_change_request as ads_apply_change_request,
    create_change_request as ads_create_change_request,
    fetch_ads_state as ads_fetch_state,
    get_ads_deep_link as ads_get_deep_link,
    list_ads_audit as ads_list_audit,
    list_ads_entities as ads_list_entities,
    list_change_requests as ads_list_change_requests,
    reject_change_request as ads_reject_change_request,
)
from app.models_overview_alerts import AlertEvent, AlertRule, NotificationChannel, UserNotificationPref
from app.services_overview import get_overview_alerts
from app.services_performance_helpers import (
    build_performance_query_context as _build_performance_query_context,
    build_performance_meta as _build_performance_meta,
    build_mapping_coverage as _build_mapping_coverage,
    compute_campaign_trends as _compute_campaign_trends,
    compute_total_spend_for_period as _perf_compute_total_spend_for_period,
    compute_total_converted_value_for_period as _perf_compute_total_converted_value_for_period,
    summarize_mapped_current as _summarize_mapped_current,
)
from app.services_quality import (
    load_config_and_meta,
    get_latest_quality_for_scope,
    summarize_config_changes,
    compute_overall_quality_from_dq,
)
from app.services_paths import compute_path_archetypes, compute_path_anomalies
from app.services_revenue_config import normalize_revenue_config
from app.services_metrics import derive_efficiency, journey_revenue_value, safe_ratio
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
    append_webhook_archive_entry,
    get_last_test_at,
    get_webhook_events,
    get_webhook_archive_entries,
    get_webhook_archive_status,
    get_webhook_last_received_at,
    get_webhook_received_count,
    get_webhook_secret,
    get_mapping,
    get_mapping_state,
    save_mapping,
    get_pull_config,
    save_pull_config,
    rotate_webhook_secret,
    rebuild_profiles_from_webhook_archive,
    set_webhook_received,
    update_mapping_approval,
)
from app.attribution_engine import (
    run_attribution,
    run_all_models as run_all_attribution,
    run_attribution_campaign,
    compute_channel_performance,
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


@contextmanager
def _internal_db_session():
    provider = app.dependency_overrides.get(get_db, get_db)
    resource = provider()
    if hasattr(resource, "__next__"):
        db = next(resource)
        try:
            yield db
        finally:
            try:
                next(resource)
            except StopIteration:
                pass
        return
    try:
        yield resource
    finally:
        close = getattr(resource, "close", None)
        if callable(close):
            close()


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
        try:
            with _internal_db_session() as db:
                verify_csrf(db, request)
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    return await call_next(request)


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
    source_field: str = "source"
    medium_field: str = "medium"
    campaign_field: str = "campaign"
    currency_field: str = "currency"


class FromCDPRequest(BaseModel):
    mapping: Optional[AttributionMappingConfig] = None
    config_id: Optional[str] = None
    import_note: Optional[str] = None


class MeiroWebhookReprocessRequest(BaseModel):
    archive_limit: Optional[int] = None
    persist_to_attribution: bool = False
    config_id: Optional[str] = None
    import_note: Optional[str] = None


class MeiroMappingApprovalRequest(BaseModel):
    status: str
    note: Optional[str] = None


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
JOURNEY_SOURCE_STATE_FILE = DATA_DIR / "journey_source_state.json"
LATEST_UPLOAD_FILE = DATA_DIR / "journeys_upload_latest.json"


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


def _normalize_journey_source(value: Optional[str]) -> Optional[str]:
    raw = (value or "").strip().lower()
    if raw in {"meiro", "meiro_webhook", "meiro_pull"}:
        return "meiro"
    if raw in {"sample", "upload"}:
        return raw
    return None


def _get_journey_source_state() -> Dict[str, Any]:
    if not JOURNEY_SOURCE_STATE_FILE.exists():
        return {}
    try:
        data = json.loads(JOURNEY_SOURCE_STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def _set_active_journey_source(source: str) -> None:
    normalized = _normalize_journey_source(source)
    if not normalized:
        return
    try:
        payload = {
            "active_source": normalized,
            "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
        JOURNEY_SOURCE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        JOURNEY_SOURCE_STATE_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        pass


def _journey_source_availability() -> List[Dict[str, Any]]:
    meiro_available = (DATA_DIR / "meiro_cdp_profiles.json").exists() or (DATA_DIR / "meiro_cdp.csv").exists()
    upload_available = LATEST_UPLOAD_FILE.exists()
    sample_available = (SAMPLE_DIR / "sample-conversion-paths.json").exists()
    return [
        {"key": "sample", "label": "Sample data", "available": sample_available},
        {"key": "upload", "label": "Uploaded JSON", "available": upload_available},
        {"key": "meiro", "label": "Meiro CDP", "available": meiro_available},
    ]


# ==================== Settings ====================


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


def _replace_settings(new_settings: Settings) -> Settings:
    global SETTINGS, JOURNEYS
    SETTINGS = new_settings
    JOURNEYS = []
    _save_settings()
    return SETTINGS


def _replace_revenue_config(payload: RevenueConfig) -> Dict[str, Any]:
    global SETTINGS, JOURNEYS
    SETTINGS = Settings(
        attribution=SETTINGS.attribution,
        mmm=SETTINGS.mmm,
        nba=SETTINGS.nba,
        feature_flags=SETTINGS.feature_flags,
        ads_governance=getattr(SETTINGS, "ads_governance", AdsGovernanceSettings()),
        revenue_config=RevenueConfig(**normalize_revenue_config(payload.model_dump())),
    )
    JOURNEYS = []
    _save_settings()
    return normalize_revenue_config(SETTINGS.revenue_config.model_dump())


def _get_kpi_config_model() -> KpiConfigModel:
    cfg = KPI_CONFIG
    return KpiConfigModel(
        definitions=[KpiDefinitionModel(**d.__dict__) for d in cfg.definitions],
        primary_kpi_id=cfg.primary_kpi_id,
    )


def _replace_kpi_config(cfg: KpiConfigModel) -> KpiConfigModel:
    global KPI_CONFIG
    defs = [KpiDefinition(**d.dict()) for d in cfg.definitions]
    KPI_CONFIG = KpiConfig(definitions=defs, primary_kpi_id=cfg.primary_kpi_id)
    save_kpi_config(KPI_CONFIG)
    return _get_kpi_config_model()


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
    return _compute_campaign_trends(journeys)


def _compute_total_spend_for_period(
    *,
    expenses: Any,
    date_from: str,
    date_to: str,
    timezone_name: Optional[str],
    channels: Optional[List[str]],
) -> float:
    return _perf_compute_total_spend_for_period(
        expenses=expenses,
        date_from=date_from,
        date_to=date_to,
        timezone_name=timezone_name,
        channels=channels,
    )


def _compute_total_converted_value_for_period(
    *,
    journeys: List[Dict[str, Any]],
    date_from: str,
    date_to: str,
    timezone_name: Optional[str],
    channels: Optional[List[str]],
    conversion_key: Optional[str],
) -> float:
    return _perf_compute_total_converted_value_for_period(
        journeys=journeys,
        date_from=date_from,
        date_to=date_to,
        timezone_name=timezone_name,
        channels=channels,
        conversion_key=conversion_key,
    )


def _attach_scope_confidence(
    *,
    db: Any,
    items: List[Dict[str, Any]],
    scope_type: str,
    id_field: str,
    conversion_key: Optional[str],
) -> None:
    for item in items:
        scope_id = item.get(id_field)
        if not scope_id:
            continue
        snap = get_latest_quality_for_scope(db, scope_type, str(scope_id), conversion_key)
        if snap:
            item["confidence"] = {
                "score": snap.confidence_score,
                "label": snap.confidence_label,
                "components": snap.components_json,
            }


def _ensure_journeys_loaded(db: Any) -> List[Dict[str, Any]]:
    global JOURNEYS
    if not JOURNEYS:
        JOURNEYS = load_journeys_from_db(db)
    return JOURNEYS or []


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


class ModelConfigSuggestPayload(BaseModel):
    strategy: str = "balanced"


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


class AdsChangeRequestCreatePayload(BaseModel):
    provider: str
    account_id: str
    entity_type: str
    entity_id: str
    action_type: str
    action_payload: Dict[str, Any] = Field(default_factory=dict)


class AdsChangeRequestRejectPayload(BaseModel):
    reason: Optional[str] = None


class AdsChangeRequestApplyPayload(BaseModel):
    admin_override: bool = False


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

    journeys = _ensure_journeys_loaded(db)
    if not journeys:
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
        journeys,
        baseline_json or {},
    )
    draft_journeys = apply_model_config_to_journeys(journeys, cfg_json)

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


@app.post("/api/model-configs/{cfg_id}/suggest")
def suggest_model_config_route(
    cfg_id: str,
    payload: ModelConfigSuggestPayload,
    db=Depends(get_db),
):
    cfg = db.get(ORMModelConfig, cfg_id)
    if not cfg:
        raise HTTPException(status_code=404, detail="Config not found")
    journeys = _ensure_journeys_loaded(db)
    return suggest_model_config_from_journeys(
        journeys,
        kpi_definitions=[d.__dict__ for d in KPI_CONFIG.definitions],
        strategy=payload.strategy,
    )


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


def _ads_governance_settings() -> AdsGovernanceSettings:
    cfg = getattr(SETTINGS, "ads_governance", None)
    if isinstance(cfg, AdsGovernanceSettings):
        return cfg
    if isinstance(cfg, dict):
        try:
            return AdsGovernanceSettings(**cfg)
        except Exception:
            return AdsGovernanceSettings()
    return AdsGovernanceSettings()


@app.get("/api/ads/entities")
def get_ads_entities(
    provider: Optional[str] = Query(default=None),
    entity_type: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("ads.view")),
):
    workspace_id = workspace_scope_or_403(ctx, None)
    try:
        if provider and provider not in ADS_PROVIDER_KEYS:
            raise ValueError("Unsupported provider")
        if entity_type and entity_type not in ADS_ENTITY_TYPES:
            raise ValueError("Unsupported entity_type")
        return ads_list_entities(
            db,
            workspace_id=workspace_id,
            provider=provider,
            entity_type=entity_type,
            search=search,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/ads/deeplink")
def get_ads_deeplink(
    provider: str = Query(...),
    account_id: Optional[str] = Query(default=None),
    entity_type: str = Query(...),
    entity_id: str = Query(..., min_length=1),
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("ads.view")),
):
    workspace_id = workspace_scope_or_403(ctx, None)
    try:
        url = ads_get_deep_link(
            db,
            workspace_id=workspace_id,
            provider=provider,
            account_id=account_id,
            entity_type=entity_type,
            entity_id=entity_id,
        )
        return {"provider": provider, "entity_type": entity_type, "entity_id": entity_id, "url": url}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/ads/state")
def get_ads_state(
    provider: str = Query(...),
    account_id: str = Query(..., min_length=1),
    entity_type: str = Query(...),
    entity_id: str = Query(..., min_length=1),
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("ads.view")),
):
    workspace_id = workspace_scope_or_403(ctx, None)
    try:
        return ads_fetch_state(
            db,
            workspace_id=workspace_id,
            provider=provider,
            account_id=account_id,
            entity_type=entity_type,
            entity_id=entity_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/ads/change-requests")
def create_ads_change_request(
    payload: AdsChangeRequestCreatePayload,
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("ads.propose")),
):
    workspace_id = workspace_scope_or_403(ctx, None)
    governance = _ads_governance_settings()
    try:
        return ads_create_change_request(
            db,
            workspace_id=workspace_id,
            requested_by_user_id=ctx.user_id,
            provider=payload.provider,
            account_id=payload.account_id,
            entity_type=payload.entity_type,
            entity_id=payload.entity_id,
            action_type=payload.action_type,
            action_payload=payload.action_payload,
            approval_required=bool(governance.require_approval),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/ads/change-requests")
def get_ads_change_requests(
    status: Optional[str] = Query(default=None),
    provider: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("ads.view")),
):
    workspace_id = workspace_scope_or_403(ctx, None)
    try:
        if status and status not in {"draft", "pending_approval", "approved", "rejected", "applied", "failed", "cancelled"}:
            raise ValueError("Unsupported status")
        if provider and provider not in ADS_PROVIDER_KEYS:
            raise ValueError("Unsupported provider")
        return ads_list_change_requests(
            db,
            workspace_id=workspace_id,
            status=status,
            provider=provider,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/ads/change-requests/{request_id}/approve")
def approve_ads_change_request(
    request_id: str,
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("ads.apply")),
):
    workspace_id = workspace_scope_or_403(ctx, None)
    try:
        return ads_approve_change_request(
            db,
            workspace_id=workspace_id,
            request_id=request_id,
            actor_user_id=ctx.user_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/ads/change-requests/{request_id}/reject")
def reject_ads_change_request(
    request_id: str,
    payload: AdsChangeRequestRejectPayload,
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("ads.apply")),
):
    workspace_id = workspace_scope_or_403(ctx, None)
    try:
        return ads_reject_change_request(
            db,
            workspace_id=workspace_id,
            request_id=request_id,
            actor_user_id=ctx.user_id,
            reason=payload.reason,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/ads/change-requests/{request_id}/apply")
def apply_ads_change_request(
    request_id: str,
    payload: AdsChangeRequestApplyPayload,
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("ads.apply")),
):
    workspace_id = workspace_scope_or_403(ctx, None)
    governance = _ads_governance_settings()
    try:
        return ads_apply_change_request(
            db,
            workspace_id=workspace_id,
            request_id=request_id,
            actor_user_id=ctx.user_id,
            require_approval=bool(governance.require_approval),
            budget_change_limit_pct=max(0.0, float(governance.max_budget_change_pct)),
            admin_override=bool(payload.admin_override),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/ads/audit")
def get_ads_audit(
    provider: Optional[str] = Query(default=None),
    entity_id: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    db=Depends(get_db),
    ctx: PermissionContext = Depends(require_permission("ads.view")),
):
    workspace_id = workspace_scope_or_403(ctx, None)
    try:
        if provider and provider not in ADS_PROVIDER_KEYS:
            raise ValueError("Unsupported provider")
        return ads_list_audit(
            db,
            workspace_id=workspace_id,
            provider=provider,
            entity_id=entity_id,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _current_user_id(request: Request) -> str:
    """Resolve current user for notification prefs; session first, then legacy fallback."""
    raw_session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if raw_session_id:
        with _internal_db_session() as db:
            ctx = resolve_auth_context(db, raw_session_id=raw_session_id)
            if ctx:
                return ctx.user.id
    return request.headers.get("X-User-Id") or request.query_params.get("user_id") or "default"


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


# ==================== Health ====================

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        **engine_info(),
        "journeys_loaded": len(JOURNEYS),
        "attribution_models": ATTRIBUTION_MODELS,
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
        source_field=saved.get("source_field", "source"),
        medium_field=saved.get("medium_field", "medium"),
        campaign_field=saved.get("campaign_field", "campaign"),
        currency_field=saved.get("currency_field", "currency"),
    )
    mapping = base
    if req.mapping:
        mapping = AttributionMappingConfig(
            touchpoint_attr=req.mapping.touchpoint_attr or base.touchpoint_attr,
            value_attr=req.mapping.value_attr or base.value_attr,
            id_attr=req.mapping.id_attr or base.id_attr,
            channel_field=req.mapping.channel_field or base.channel_field,
            timestamp_field=req.mapping.timestamp_field or base.timestamp_field,
            source_field=req.mapping.source_field or base.source_field,
            medium_field=req.mapping.medium_field or base.medium_field,
            campaign_field=req.mapping.campaign_field or base.campaign_field,
            currency_field=req.mapping.currency_field or base.currency_field,
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
        pull_cfg = get_pull_config()
        result = canonicalize_meiro_profiles(
            profiles,
            mapping=mapping.model_dump(),
            revenue_config=(SETTINGS.revenue_config.model_dump() if hasattr(SETTINGS.revenue_config, "model_dump") else SETTINGS.revenue_config),
            dedup_config=pull_cfg,
        )
        journeys = result["valid_journeys"]
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
        source_label, len(journeys), "success",
        total=len(journeys), valid=len(journeys), converted=converted, channels_detected=sorted(ch_set),
        config_snapshot={
            "mapping_preset": "saved",
            "schema_version": "1.0",
            "dedup_interval_minutes": pull_cfg.get("dedup_interval_minutes"),
            "dedup_mode": pull_cfg.get("dedup_mode"),
            "primary_dedup_key": pull_cfg.get("primary_dedup_key"),
            "fallback_dedup_keys": pull_cfg.get("fallback_dedup_keys"),
        },
        preview_rows=[
            {
                "customer_id": j.get("customer_id", "?"),
                "touchpoints": len(j.get("touchpoints", [])),
                "value": journey_revenue_value(j),
            }
            for j in journeys[:20]
        ],
        import_note=req.import_note,
    )
    _set_active_journey_source("meiro")
    return {"count": len(journeys), "message": f"Parsed {len(journeys)} journeys from CDP data"}

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
    _set_active_journey_source("sample")
    return {
        "count": len(valid),
        "message": f"Loaded {len(valid)} valid journeys",
        "import_summary": summary,
        "validation_items": result["validation_items"],
    }


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
    "defaulted_conversion_value_pct": {
        "definition": "Share of conversion entries where the configured default value was applied because the raw value was missing or invalid.",
        "recommended_actions": [
            "Review Revenue KPI defaults and per-conversion overrides in Settings",
            "Confirm the correct conversion value field is mapped from Meiro payloads",
            "Inspect webhook suggestions for missing value-field paths",
        ],
    },
    "raw_zero_conversion_value_pct": {
        "definition": "Share of conversion entries that arrived with a raw numeric value of zero before any fallback logic.",
        "recommended_actions": [
            "Check whether zero is a valid business value for this conversion type",
            "Use per-conversion defaults only where zero should mean missing revenue",
            "Validate source event schemas and test payloads from Meiro",
        ],
    },
    "unresolved_source_medium_touchpoint_pct": {
        "definition": "Share of touchpoints whose source/medium pair does not match any current taxonomy rule after alias normalization.",
        "recommended_actions": [
            "Apply draft taxonomy suggestions from the Meiro payload analysis card",
            "Add missing source aliases and medium aliases for new traffic values",
            "Create channel rules for repeated unresolved source/medium pairs",
        ],
    },
    "inferred_mapping_journey_pct": {
        "definition": "Share of journeys where the parser had to fall back to inferred field mappings instead of only using explicitly configured paths.",
        "recommended_actions": [
            "Review and apply Meiro mapping suggestions for touchpoints, IDs, and conversion fields",
            "Confirm webhook payloads use stable field paths for source, medium, campaign, value, and timestamp",
            "Audit journey parser metadata to identify the fields relying on fallback extraction",
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


def _validate_conversion_kpi_id(conversion_kpi_id: Optional[str]) -> Optional[str]:
    value = (conversion_kpi_id or "").strip()
    if not value:
        return None
    available = {d.id for d in KPI_CONFIG.definitions}
    if value not in available:
        raise HTTPException(status_code=400, detail=f"conversion_kpi_id '{value}' is not defined in KPI config")
    return value


app.include_router(
    create_journeys_router(
        get_db_dependency=get_db,
        require_permission_dependency=require_permission,
        clamp_int_fn=clamp_int,
        resolve_per_page_fn=resolve_per_page,
        resolve_sort_dir_fn=resolve_sort_dir,
        validate_conversion_kpi_id_fn=_validate_conversion_kpi_id,
        get_settings_obj=lambda: SETTINGS,
    )
)

app.include_router(
    create_performance_router(
        get_db_dependency=get_db,
        require_permission_dependency=require_permission,
        ensure_journeys_loaded_fn=_ensure_journeys_loaded,
        build_query_context_fn=_build_performance_query_context,
        build_meta_fn=_build_performance_meta,
        attach_scope_confidence_fn=_attach_scope_confidence,
        build_mapping_coverage_fn=_build_mapping_coverage,
        summarize_mapped_current_fn=_summarize_mapped_current,
        expenses_obj=EXPENSES,
    )
)

app.include_router(
    create_settings_router(
        get_db_dependency=get_db,
        require_permission_dependency=require_permission,
        get_settings_obj=lambda: SETTINGS,
        replace_settings_fn=_replace_settings,
        replace_revenue_config_fn=_replace_revenue_config,
        get_kpi_config_model_fn=_get_kpi_config_model,
        replace_kpi_config_fn=_replace_kpi_config,
        ensure_journeys_loaded_fn=_ensure_journeys_loaded,
        resolve_current_user_id_fn=_current_user_id,
    )
)

app.include_router(
    create_admin_access_router(
        get_db_dependency=get_db,
        require_permission_dependency=require_permission,
        require_any_permission_dependency=require_any_permission,
        workspace_scope_or_403_fn=workspace_scope_or_403,
        get_settings_obj=lambda: SETTINGS,
        write_security_audit_fn=_write_security_audit,
        invite_token_fn=_invite_token,
        hash_invite_token_fn=_hash_invite_token,
    )
)

app.include_router(
    create_auth_access_router(
        get_db_dependency=get_db,
        require_permission_dependency=require_permission,
        get_base_url_fn=lambda: BASE_URL,
        get_frontend_url_fn=lambda: FRONTEND_URL,
        get_connected_platforms_fn=get_all_connected_platforms,
        meiro_connected_fn=meiro_cdp.is_connected,
        delete_token_fn=delete_token,
        datasource_config_obj=ds_config,
        session_cookie_secure=SESSION_COOKIE_SECURE,
        session_cookie_samesite=SESSION_COOKIE_SAMESITE,
        session_cookie_max_age=SESSION_COOKIE_MAX_AGE,
    )
)
app.include_router(
    create_alerts_funnels_router(
        get_db_dependency=get_db,
        require_permission_dependency=require_permission,
        clamp_int_fn=clamp_int,
        resolve_per_page_fn=resolve_per_page,
        get_journey_definition_fn=get_journey_definition,
    )
)

app.include_router(
    create_attribution_router(
        get_db_dependency=get_db,
        require_permission_dependency=require_permission,
        attribution_models_obj=ATTRIBUTION_MODELS,
        get_journeys_fn=_ensure_journeys_loaded,
        get_import_runs_fn=get_import_runs,
        get_import_run_fn=get_import_run,
        get_last_successful_run_fn=get_last_successful_run,
        build_journeys_summary_fn=build_journeys_summary,
        build_journeys_preview_fn=build_journeys_preview,
        get_kpi_config_fn=lambda: KPI_CONFIG,
        get_journey_source_state_fn=_get_journey_source_state,
        normalize_journey_source_fn=_normalize_journey_source,
        set_active_journey_source_fn=_set_active_journey_source,
        journey_source_availability_fn=_journey_source_availability,
        latest_upload_file_obj=LATEST_UPLOAD_FILE,
        validate_and_normalize_fn=validate_and_normalize,
        persist_journeys_fn=persist_journeys_as_conversion_paths,
        refresh_journey_aggregates_fn=_refresh_journey_aggregates_after_import,
        save_last_import_result_fn=_save_last_import_result,
        load_last_import_result_fn=_load_last_import_result,
        append_import_run_fn=_append_import_run,
        load_sample_journeys_fn=lambda *args, **kwargs: load_sample_journeys(*args, **kwargs),
        import_journeys_from_cdp_fn=lambda *args, **kwargs: import_journeys_from_cdp(*args, **kwargs),
        from_cdp_request_factory=FromCDPRequest,
        run_attribution_fn=run_attribution,
        load_config_and_meta_fn=load_config_and_meta,
        apply_model_config_fn=apply_model_config_to_journeys,
        get_settings_obj=lambda: SETTINGS,
        get_attribution_results_obj=lambda: ATTRIBUTION_RESULTS,
        get_data_dir_obj=lambda: DATA_DIR,
        get_mapping_fn=get_mapping,
        attribution_mapping_config_cls=AttributionMappingConfig,
        canonicalize_meiro_profiles_fn=canonicalize_meiro_profiles,
        get_pull_config_fn=get_pull_config,
        get_default_config_id_fn=get_default_config_id,
        get_model_config_fn=lambda db, cfg_id: db.get(ORMModelConfig, cfg_id),
        journey_revenue_value_fn=journey_revenue_value,
        analyze_paths_fn=analyze_paths,
        compute_next_best_action_fn=compute_next_best_action,
        has_any_campaign_fn=has_any_campaign,
        filter_nba_recommendations_fn=_filter_nba_recommendations,
        step_string_fn=_step_string,
        get_latest_quality_for_scope_fn=get_latest_quality_for_scope,
        build_conversion_paths_analysis_from_daily_fn=build_conversion_paths_analysis_from_daily,
        build_conversion_path_details_from_daily_fn=build_conversion_path_details_from_daily,
        path_archetypes_cache_obj=PATH_ARCHETYPES_CACHE,
        compute_path_archetypes_fn=compute_path_archetypes,
        compute_path_anomalies_fn=compute_path_anomalies,
        run_attribution_campaign_fn=run_attribution_campaign,
        expenses_obj=EXPENSES,
        compute_channel_performance_fn=compute_channel_performance,
        derive_efficiency_fn=derive_efficiency,
        compute_campaign_uplift_fn=compute_campaign_uplift,
        compute_campaign_trends_fn=compute_campaign_trends,
    )
)


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
    journeys = _ensure_journeys_loaded(db)
    if not journeys:
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

                curr_roas = safe_ratio(curr_val_ch, curr_spend_ch)
                prev_roas = safe_ratio(prev_val_ch, prev_spend_ch)
                curr_cpa = safe_ratio(curr_spend_ch, curr_conv_ch)
                prev_cpa = safe_ratio(prev_spend_ch, prev_conv_ch)

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
    dedupe_seen_timeline: set[str] = set()
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
        daily_values[day_str] = daily_values.get(day_str, 0.0) + journey_revenue_value(
            j,
            dedupe_seen=dedupe_seen_timeline,
        )
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
        source_medium_pair_counts: Dict[str, int] = {}
        value_field_counts: Dict[str, int] = {}
        currency_field_counts: Dict[str, int] = {}
        touchpoint_attr_counts: Dict[str, int] = {}
        channel_field_path_counts: Dict[str, int] = {}
        source_field_path_counts: Dict[str, int] = {}
        medium_field_path_counts: Dict[str, int] = {}
        campaign_field_path_counts: Dict[str, int] = {}
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

        def _normalized_text(value: Any) -> Optional[str]:
            if isinstance(value, dict):
                value = value.get("name") or value.get("platform") or value.get("campaign_name")
            if not isinstance(value, str):
                return None
            normalized = value.strip().lower()
            return normalized or None

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
                touchpoint_attr_counts["touchpoints"] = touchpoint_attr_counts.get("touchpoints", 0) + 1
                for tp in touchpoints[:500]:
                    if not isinstance(tp, dict):
                        continue
                    touchpoint_count += 1
                    raw_source = _normalized_text(tp.get("source"))
                    raw_medium = _normalized_text(tp.get("medium"))
                    raw_campaign = _normalized_text(tp.get("campaign"))
                    if tp.get("channel"):
                        channel_field_path_counts["channel"] = channel_field_path_counts.get("channel", 0) + 1
                    if tp.get("source"):
                        source_field_path_counts["source"] = source_field_path_counts.get("source", 0) + 1
                    if tp.get("medium"):
                        medium_field_path_counts["medium"] = medium_field_path_counts.get("medium", 0) + 1
                    if tp.get("campaign"):
                        campaign_field_path_counts["campaign"] = campaign_field_path_counts.get("campaign", 0) + 1
                    _inc(channel_counts, tp.get("channel"))
                    utm = tp.get("utm")
                    if isinstance(utm, dict):
                        if utm.get("source"):
                            source_field_path_counts["utm.source"] = source_field_path_counts.get("utm.source", 0) + 1
                        if utm.get("medium"):
                            medium_field_path_counts["utm.medium"] = medium_field_path_counts.get("utm.medium", 0) + 1
                        if utm.get("campaign"):
                            campaign_field_path_counts["utm.campaign"] = campaign_field_path_counts.get("utm.campaign", 0) + 1
                        raw_source = raw_source or _normalized_text(utm.get("source"))
                        raw_medium = raw_medium or _normalized_text(utm.get("medium"))
                        raw_campaign = raw_campaign or _normalized_text(utm.get("campaign"))
                    source_obj = tp.get("source")
                    if isinstance(source_obj, dict):
                        if source_obj.get("platform"):
                            source_field_path_counts["source.platform"] = source_field_path_counts.get("source.platform", 0) + 1
                        if source_obj.get("campaign_name"):
                            campaign_field_path_counts["source.campaign_name"] = campaign_field_path_counts.get("source.campaign_name", 0) + 1
                        raw_source = raw_source or _normalized_text(source_obj.get("platform"))
                        raw_campaign = raw_campaign or _normalized_text(source_obj.get("campaign_name"))
                    campaign_obj = tp.get("campaign")
                    if isinstance(campaign_obj, dict):
                        if campaign_obj.get("name"):
                            campaign_field_path_counts["campaign.name"] = campaign_field_path_counts.get("campaign.name", 0) + 1
                        raw_campaign = raw_campaign or _normalized_text(campaign_obj.get("name"))
                    _inc(source_counts, raw_source)
                    _inc(medium_counts, raw_medium)
                    _inc(campaign_counts, raw_campaign)
                    if raw_source or raw_medium:
                        pair_key = f"{raw_source or ''}||{raw_medium or ''}"
                        source_medium_pair_counts[pair_key] = source_medium_pair_counts.get(pair_key, 0) + 1

        return {
            "conversion_count": conversion_count,
            "touchpoint_count": touchpoint_count,
            "conversion_event_counts": conversion_event_counts,
            "channel_counts": channel_counts,
            "source_counts": source_counts,
            "medium_counts": medium_counts,
            "campaign_counts": campaign_counts,
            "source_medium_pair_counts": source_medium_pair_counts,
            "value_field_counts": value_field_counts,
            "currency_field_counts": currency_field_counts,
            "touchpoint_attr_counts": touchpoint_attr_counts,
            "channel_field_path_counts": channel_field_path_counts,
            "source_field_path_counts": source_field_path_counts,
            "medium_field_path_counts": medium_field_path_counts,
            "campaign_field_path_counts": campaign_field_path_counts,
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
    append_webhook_archive_entry(
        {
            "received_at": now_iso,
            "parser_version": MEIRO_PARSER_VERSION,
            "replace": bool(replace),
            "payload_shape": "array" if isinstance(body, list) else "object",
            "received_count": int(len(profiles)),
            "profiles": profiles,
        }
    )
    append_webhook_event(
        {
            "received_at": now_iso,
            "received_count": int(len(profiles)),
            "stored_total": int(len(to_store)),
            "replace": bool(replace),
            "parser_version": MEIRO_PARSER_VERSION,
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


@app.get("/api/connectors/meiro/webhook/archive-status")
def meiro_webhook_archive_status():
    return get_webhook_archive_status()


@app.get("/api/connectors/meiro/webhook/archive")
def meiro_webhook_archive(
    limit: int = Query(25, ge=1, le=500),
):
    items = get_webhook_archive_entries(limit=limit)
    return {"items": items, "total": len(items)}


@app.post("/api/connectors/meiro/webhook/reprocess")
def meiro_webhook_reprocess(
    payload: MeiroWebhookReprocessRequest = Body(default=MeiroWebhookReprocessRequest()),
    db=Depends(get_db),
):
    archive_entries = get_webhook_archive_entries(limit=payload.archive_limit or 5000)
    archived_profiles = rebuild_profiles_from_webhook_archive(limit=payload.archive_limit)
    if not archived_profiles:
        raise HTTPException(status_code=404, detail="No archived webhook payloads found to reprocess.")
    mapping_state = get_mapping_state()

    out_path = DATA_DIR / "meiro_cdp_profiles.json"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(archived_profiles, indent=2))

    result: Dict[str, Any] = {
        "reprocessed_profiles": len(archived_profiles),
        "archive_entries_used": len(archive_entries),
        "parser_version": MEIRO_PARSER_VERSION,
        "mapping_version": mapping_state.get("version") or 0,
        "mapping_approval_status": ((mapping_state.get("approval") or {}).get("status") or "unreviewed"),
        "persisted_to_attribution": False,
    }
    if payload.persist_to_attribution:
        import_result = import_journeys_from_cdp(
            req=FromCDPRequest(config_id=payload.config_id, import_note=payload.import_note or "Reprocessed from webhook archive"),
            db=db,
        )
        result["persisted_to_attribution"] = True
        result["import_result"] = import_result
    return result


@app.get("/api/connectors/meiro/webhook/suggestions")
def meiro_webhook_suggestions(limit: int = Query(100, ge=1, le=500)):
    events = get_webhook_events(limit=limit)
    conversion_event_counts: Dict[str, int] = {}
    channel_counts: Dict[str, int] = {}
    source_counts: Dict[str, int] = {}
    medium_counts: Dict[str, int] = {}
    campaign_counts: Dict[str, int] = {}
    source_medium_pair_counts: Dict[str, int] = {}
    value_field_counts: Dict[str, int] = {}
    currency_field_counts: Dict[str, int] = {}
    touchpoint_attr_counts: Dict[str, int] = {}
    channel_field_path_counts: Dict[str, int] = {}
    source_field_path_counts: Dict[str, int] = {}
    medium_field_path_counts: Dict[str, int] = {}
    campaign_field_path_counts: Dict[str, int] = {}
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
        _merge_counts(campaign_counts, analysis.get("campaign_counts") or {})
        _merge_counts(source_medium_pair_counts, analysis.get("source_medium_pair_counts") or {})
        _merge_counts(value_field_counts, analysis.get("value_field_counts") or {})
        _merge_counts(currency_field_counts, analysis.get("currency_field_counts") or {})
        _merge_counts(touchpoint_attr_counts, analysis.get("touchpoint_attr_counts") or {})
        _merge_counts(channel_field_path_counts, analysis.get("channel_field_path_counts") or {})
        _merge_counts(source_field_path_counts, analysis.get("source_field_path_counts") or {})
        _merge_counts(medium_field_path_counts, analysis.get("medium_field_path_counts") or {})
        _merge_counts(campaign_field_path_counts, analysis.get("campaign_field_path_counts") or {})
        for key in ("conversion_id", "order_id", "event_id"):
            dedup_key_counts[key] = dedup_key_counts.get(key, 0) + int((analysis.get("dedup_key_counts") or {}).get(key, 0) or 0)

    def _top_items(values: Dict[str, int], n: int = 10) -> List[Tuple[str, int]]:
        return sorted(values.items(), key=lambda kv: kv[1], reverse=True)[:n]

    top_conversion_names = _top_items(conversion_event_counts, n=10)
    top_value_fields = _top_items(value_field_counts, n=5)
    top_currency_fields = _top_items(currency_field_counts, n=5)
    top_sources = _top_items(source_counts, n=15)
    top_mediums = _top_items(medium_counts, n=15)
    top_campaigns = _top_items(campaign_counts, n=15)
    top_channels = _top_items(channel_counts, n=15)
    top_source_medium_pairs = _top_items(source_medium_pair_counts, n=20)
    top_touchpoint_attrs = _top_items(touchpoint_attr_counts, n=5)
    top_channel_field_paths = _top_items(channel_field_path_counts, n=5)
    top_source_field_paths = _top_items(source_field_path_counts, n=5)
    top_medium_field_paths = _top_items(medium_field_path_counts, n=5)
    top_campaign_field_paths = _top_items(campaign_field_path_counts, n=5)
    best_dedup_key = sorted(dedup_key_counts.items(), key=lambda kv: kv[1], reverse=True)[0][0]
    dedup_key_candidates = []
    for key, count in sorted(dedup_key_counts.items(), key=lambda kv: kv[1], reverse=True):
        coverage = (count / total_conversions) if total_conversions > 0 else 0.0
        dedup_key_candidates.append(
            {
                "key": key,
                "count": count,
                "coverage_pct": round(coverage * 100, 2),
                "recommended": key == best_dedup_key,
            }
        )

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

    current_taxonomy = load_taxonomy()

    def _normalized_label(value: Optional[str]) -> str:
        return str(value or "").strip().lower()

    def _suggest_source_alias(raw_source: str) -> Optional[str]:
        known = {
            "fb": "facebook",
            "ig": "instagram",
            "yt": "youtube",
            "li": "linkedin",
            "tt": "tiktok",
            "gads": "google",
            "adwords": "google",
        }
        return known.get(_normalized_label(raw_source))

    def _suggest_medium_alias(raw_medium: str) -> Optional[str]:
        known = {
            "ppc": "cpc",
            "paid": "cpc",
            "paidsearch": "cpc",
            "paid_search": "cpc",
            "paidsocial": "paid_social",
            "paid-social": "paid_social",
            "social_paid": "paid_social",
        }
        return known.get(_normalized_label(raw_medium))

    def _classify_channel(source: str, medium: str) -> Optional[str]:
        src = _normalized_label(source)
        med = _normalized_label(medium)
        if med in {"email", "e-mail"} or "email" in med:
            return "email"
        if med in {"(none)", "none", "direct", ""} and src in {"", "direct"}:
            return "direct"
        if med in {"cpc", "ppc", "paid_search"} and re.search(r"google|bing|baidu|adwords", src):
            return "paid_search"
        if med in {"paid_social", "social", "social_paid", "paid"} and re.search(r"facebook|meta|instagram|linkedin|twitter|x|tiktok", src):
            return "paid_social"
        if src in {"newsletter", "mailchimp", "klaviyo", "braze", "customerio"}:
            return "email"
        return None

    def _matches_existing_rule(source: str, medium: str, campaign: str = "") -> bool:
        source_norm = current_taxonomy.source_aliases.get(source, source)
        medium_norm = current_taxonomy.medium_aliases.get(medium, medium)
        for rule in current_taxonomy.channel_rules:
            if rule.matches(source_norm, medium_norm, campaign):
                return True
        return False

    source_aliases = dict(current_taxonomy.source_aliases)
    suggested_source_aliases: Dict[str, str] = {}
    for source, count in top_sources:
        lower = _normalized_label(source)
        if not lower or lower in source_aliases:
            continue
        mapped = _suggest_source_alias(lower)
        if mapped and mapped != lower:
            source_aliases[lower] = mapped
            suggested_source_aliases[lower] = mapped

    medium_aliases = dict(current_taxonomy.medium_aliases)
    suggested_medium_aliases: Dict[str, str] = {}
    for medium, count in top_mediums:
        lower = _normalized_label(medium)
        if not lower or lower in medium_aliases:
            continue
        mapped = _suggest_medium_alias(lower)
        if mapped and mapped != lower:
            medium_aliases[lower] = mapped
            suggested_medium_aliases[lower] = mapped

    taxonomy_rules = []
    unresolved_pairs = []
    seen_rule_keys = {
        (
            rule.channel,
            rule.source.normalize_operator(),
            rule.source.value,
            rule.medium.normalize_operator(),
            rule.medium.value,
            rule.campaign.normalize_operator(),
            rule.campaign.value,
        )
        for rule in current_taxonomy.channel_rules
    }
    priority = max([rule.priority for rule in current_taxonomy.channel_rules] or [0]) + 10
    for pair_key, count in top_source_medium_pairs:
        raw_source, raw_medium = (pair_key.split("||", 1) + [""])[:2]
        source_norm = source_aliases.get(raw_source, raw_source)
        medium_norm = medium_aliases.get(raw_medium, raw_medium)
        if not source_norm and not medium_norm:
            continue
        if _matches_existing_rule(source_norm, medium_norm):
            continue
        suggested_channel = _classify_channel(source_norm, medium_norm)
        if suggested_channel:
            active_conditions = int(bool(source_norm)) + int(bool(medium_norm))
            if active_conditions == 0:
                continue
            rule_key = (
                suggested_channel,
                "equals",
                source_norm,
                "equals",
                medium_norm,
                "any",
                "",
            )
            if rule_key in seen_rule_keys:
                continue
            taxonomy_rules.append(
                {
                    "name": f"Auto: {suggested_channel} from {source_norm or 'source'} / {medium_norm or 'medium'}",
                    "channel": suggested_channel,
                    "priority": priority,
                    "enabled": True,
                    "source": {"operator": "equals" if source_norm else "any", "value": source_norm},
                    "medium": {"operator": "equals" if medium_norm else "any", "value": medium_norm},
                    "campaign": {"operator": "any", "value": ""},
                    "observed_count": count,
                    "match_source": source_norm,
                    "match_medium": medium_norm,
                }
            )
            seen_rule_keys.add(rule_key)
            priority += 10
        else:
            unresolved_pairs.append(
                {
                    "source": source_norm,
                    "medium": medium_norm,
                    "count": count,
                }
            )
        if len(taxonomy_rules) >= 12:
            break

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
            {
                "name": r.name,
                "channel": r.channel,
                "priority": r.priority,
                "enabled": r.enabled,
                "source": {"operator": r.source.normalize_operator(), "value": r.source.value or ""},
                "medium": {"operator": r.medium.normalize_operator(), "value": r.medium.value or ""},
                "campaign": {"operator": r.campaign.normalize_operator(), "value": r.campaign.value or ""},
            }
            for r in current_taxonomy.channel_rules
        ]
        + [{k: v for k, v in rule.items() if k not in {"observed_count", "match_source", "match_medium"}} for rule in taxonomy_rules],
        "source_aliases": source_aliases,
        "medium_aliases": medium_aliases,
    }
    mapping_apply_payload = {
        "touchpoint_attr": top_touchpoint_attrs[0][0] if top_touchpoint_attrs else "touchpoints",
        "value_attr": top_value_fields[0][0] if top_value_fields else "conversion_value",
        "id_attr": "customer_id",
        "channel_field": top_channel_field_paths[0][0] if top_channel_field_paths else "channel",
        "timestamp_field": "timestamp",
        "source_field": top_source_field_paths[0][0] if top_source_field_paths else "source",
        "medium_field": top_medium_field_paths[0][0] if top_medium_field_paths else "medium",
        "campaign_field": top_campaign_field_paths[0][0] if top_campaign_field_paths else "campaign",
        "currency_field": top_currency_fields[0][0] if top_currency_fields else "currency",
    }

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "events_analyzed": len(events),
        "total_conversions_observed": total_conversions,
        "total_touchpoints_observed": total_touchpoints,
        "dedup_key_suggestion": best_dedup_key,
        "dedup_key_candidates": dedup_key_candidates,
        "kpi_suggestions": kpi_suggestions,
        "conversion_event_suggestions": [
            {"event_name": name, "count": count}
            for name, count in top_conversion_names
        ],
        "taxonomy_suggestions": {
            "channel_rules": taxonomy_rules,
            "source_aliases": suggested_source_aliases,
            "medium_aliases": suggested_medium_aliases,
            "top_sources": [{"source": name, "count": count} for name, count in top_sources],
            "top_mediums": [{"medium": name, "count": count} for name, count in top_mediums],
            "top_campaigns": [{"campaign": name, "count": count} for name, count in top_campaigns],
            "observed_pairs": [
                {
                    "source": (name.split("||", 1) + [""])[0],
                    "medium": (name.split("||", 1) + [""])[1],
                    "count": count,
                }
                for name, count in top_source_medium_pairs
            ],
            "unresolved_pairs": unresolved_pairs[:10],
        },
        "mapping_suggestions": {
            "touchpoint_attr_candidates": [{"path": name, "count": count} for name, count in top_touchpoint_attrs],
            "value_field_candidates": [{"path": name, "count": count} for name, count in top_value_fields],
            "currency_field_candidates": [{"path": name, "count": count} for name, count in top_currency_fields],
            "channel_field_candidates": [{"path": name, "count": count} for name, count in top_channel_field_paths],
            "source_field_candidates": [{"path": name, "count": count} for name, count in top_source_field_paths],
            "medium_field_candidates": [{"path": name, "count": count} for name, count in top_medium_field_paths],
            "campaign_field_candidates": [{"path": name, "count": count} for name, count in top_campaign_field_paths],
        },
        "apply_payloads": {
            "kpis": kpi_apply_payload,
            "taxonomy": taxonomy_apply_payload,
            "mapping": mapping_apply_payload,
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
    return {**get_mapping_state(), "presets": MEIRO_MAPPING_PRESETS}


@app.post("/api/connectors/meiro/mapping")
def meiro_save_mapping(mapping: dict):
    save_mapping(mapping)
    return {"message": "Mapping saved", **get_mapping_state()}


@app.post("/api/connectors/meiro/mapping/approval")
def meiro_update_mapping_approval(payload: MeiroMappingApprovalRequest):
    state = update_mapping_approval(payload.status, payload.note)
    return {"message": "Mapping approval updated", **state}


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
        dedup_mode=pull_cfg.get("dedup_mode", "balanced"),
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
        config_snapshot={
            "lookback_days": pull_cfg.get("lookback_days"),
            "session_gap_minutes": pull_cfg.get("session_gap_minutes"),
            "conversion_selector": pull_cfg.get("conversion_selector"),
            "dedup_interval_minutes": pull_cfg.get("dedup_interval_minutes"),
            "dedup_mode": pull_cfg.get("dedup_mode"),
            "primary_dedup_key": pull_cfg.get("primary_dedup_key"),
            "fallback_dedup_keys": pull_cfg.get("fallback_dedup_keys"),
        },
        preview_rows=[{"customer_id": j.get("customer_id", "?"), "touchpoints": len(j.get("touchpoints", [])), "value": journey_revenue_value(j)} for j in journeys[:20]],
    )
    _set_active_journey_source("meiro")
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
        source_field=saved.get("source_field", "source"),
        medium_field=saved.get("medium_field", "medium"),
        campaign_field=saved.get("campaign_field", "campaign"),
        currency_field=saved.get("currency_field", "currency"),
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
        pull_cfg = get_pull_config()
        result = canonicalize_meiro_profiles(
            profiles,
            mapping=mapping.model_dump(),
            revenue_config=(SETTINGS.revenue_config.model_dump() if hasattr(SETTINGS.revenue_config, "model_dump") else SETTINGS.revenue_config),
            dedup_config=pull_cfg,
        )
        journeys = result["valid_journeys"]
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
    preview = [{"id": j.get("customer_id", j.get("id", "?")), "touchpoints": len(j.get("touchpoints", [])), "value": journey_revenue_value(j)} for j in journeys[:20]]
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
