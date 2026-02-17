"""Versioned settings service for Journeys/Funnels/Diagnostics behavior."""

from __future__ import annotations

import copy
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
from sqlalchemy import func
from sqlalchemy.orm import Session

from .models_config_dq import (
    JourneyPathDaily,
    JourneySettingsStatus,
    JourneySettingsVersion,
    JourneyTransitionDaily,
    WorkspaceSettings,
)

DEFAULT_WORKSPACE_ID = "default"
SCHEMA_VERSION = "1.0"
ACTIVE_SETTINGS_CACHE_TTL_SECONDS = 60

_ACTIVE_SETTINGS_CACHE: Dict[str, Any] = {
    "workspace_id": None,
    "version_id": None,
    "settings_json": None,
    "expires_at": None,
}


def _new_id() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.utcnow()


def _deepcopy(data: Any) -> Any:
    return copy.deepcopy(data)


class SessionizationSettings(BaseModel):
    session_timeout_minutes: int = Field(default=30, ge=1, le=1440)
    start_new_session_on_events: List[str] = Field(default_factory=list)
    lookback_window_days: int = Field(default=30, ge=1, le=365)
    conversion_journeys_only: bool = True
    allow_all_journeys: bool = False
    max_steps_per_journey: int = Field(default=20, ge=2, le=50)
    dedup_consecutive_identical_steps: bool = True
    timezone_handling: str = "platform_default"


class StepMappingRule(BaseModel):
    step_name: str = Field(min_length=1, max_length=128)
    priority: int = Field(default=100, ge=1, le=10000)
    enabled: bool = True
    event_name_equals: List[str] = Field(default_factory=list)
    url_contains: List[str] = Field(default_factory=list)
    url_regex: List[str] = Field(default_factory=list)
    channel_group_equals: List[str] = Field(default_factory=list)
    referrer_contains: List[str] = Field(default_factory=list)
    custom_predicate: Optional[Dict[str, Any]] = None


class StepCanonicalizationSettings(BaseModel):
    first_match_wins: bool = True
    fallback_step: str = Field(default="Other", min_length=1, max_length=128)
    collapse_rare_steps_into_other_default: bool = True
    rules: List[StepMappingRule] = Field(default_factory=list)


class PathsExplorerDefaults(BaseModel):
    default_sort: str = Field(default="conversions_desc")
    top_paths_limit: int = Field(default=50, ge=10, le=500)
    group_low_frequency_paths_into_other: bool = True
    trend_metric: str = Field(default="conversion_rate")
    comparison_window: str = Field(default="previous_period")


class FlowDefaults(BaseModel):
    max_depth: int = Field(default=4, ge=2, le=6)
    min_volume_threshold: int = Field(default=20, ge=1, le=1000000)
    rare_event_threshold: int = Field(default=10, ge=1, le=1000000)
    collapse_rare_into_other: bool = True
    max_nodes: int = Field(default=30, ge=10, le=150)
    always_show_conversion_terminal_node: bool = True


class FunnelsDefaults(BaseModel):
    default_counting_method: str = Field(default="uniques")
    default_conversion_window_seconds: int = Field(default=604800, ge=1, le=31622400)
    step_to_step_max_time_enabled: bool = False
    step_to_step_max_time_seconds: Optional[int] = Field(default=None, ge=1, le=31622400)
    attribution_model_default: str = Field(default="data_driven")
    breakdown_top_n: int = Field(default=5, ge=1, le=25)

    @model_validator(mode="after")
    def _validate_step_window(self) -> "FunnelsDefaults":
        if self.step_to_step_max_time_enabled and not self.step_to_step_max_time_seconds:
            raise ValueError("step_to_step_max_time_seconds is required when step_to_step_max_time_enabled=true")
        return self


class DiagnosticsDefaults(BaseModel):
    enabled: bool = False
    baseline_mode: str = Field(default="previous_period")
    sensitivity: str = Field(default="medium")
    signals: Dict[str, bool] = Field(
        default_factory=lambda: {
            "time_to_next_step_spike": True,
            "device_skew_change": True,
            "geo_skew_change": True,
            "consent_opt_out_spike": False,
            "error_event_rate_spike": False,
            "landing_page_group_change": False,
        }
    )
    output_policy: str = Field(default="hypotheses_only")
    require_evidence_for_every_claim: bool = True
    confidence_thresholds: Dict[str, int] = Field(
        default_factory=lambda: {"low_max": 39, "medium_max": 69, "high_min": 70}
    )

    @field_validator("require_evidence_for_every_claim")
    @classmethod
    def _require_evidence(cls, value: bool) -> bool:
        if value is not True:
            raise ValueError("require_evidence_for_every_claim must remain true")
        return value


class GuardrailDefaults(BaseModel):
    aggregation_reprocess_window_days: int = Field(default=3, ge=1, le=30)
    journey_instance_sampling_retention_days: Optional[int] = Field(
        default=None, ge=1, le=7
    )
    sampling_rate_example_journeys: float = Field(default=0.0, ge=0.0, le=1.0)
    max_flow_query_lookback_window_days: int = Field(default=90, ge=7, le=365)
    max_flow_date_range_days_before_weekly: int = Field(default=45, ge=7, le=365)


class JourneySettingsSchema(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    sessionization: SessionizationSettings = SessionizationSettings()
    step_canonicalization: StepCanonicalizationSettings = StepCanonicalizationSettings()
    paths_explorer_defaults: PathsExplorerDefaults = PathsExplorerDefaults()
    flow_defaults: FlowDefaults = FlowDefaults()
    funnels_defaults: FunnelsDefaults = FunnelsDefaults()
    diagnostics_defaults: DiagnosticsDefaults = DiagnosticsDefaults()
    performance_guardrails: GuardrailDefaults = GuardrailDefaults()

    @field_validator("schema_version")
    @classmethod
    def _schema_version(cls, value: str) -> str:
        if value != SCHEMA_VERSION:
            raise ValueError(f"schema_version must be '{SCHEMA_VERSION}'")
        return value


def default_journey_settings() -> Dict[str, Any]:
    return JourneySettingsSchema().model_dump()


def _normalize_and_validate(settings_json: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, str]], List[Dict[str, str]]]:
    errors: List[Dict[str, str]] = []
    warnings: List[Dict[str, str]] = []
    try:
        normalized_model = JourneySettingsSchema.model_validate(settings_json or {})
        normalized = normalized_model.model_dump()
    except ValidationError as exc:
        for issue in exc.errors():
            path = ".".join(str(p) for p in issue.get("loc", []))
            errors.append(
                {
                    "path": path or "settings_json",
                    "message": issue.get("msg", "Invalid value"),
                    "code": str(issue.get("type", "validation_error")),
                }
            )
        return None, errors, warnings

    rules = normalized["step_canonicalization"]["rules"]
    seen_names: set[str] = set()
    for idx, rule in enumerate(rules):
        name = (rule.get("step_name") or "").strip().lower()
        if name in seen_names:
            errors.append(
                {
                    "path": f"step_canonicalization.rules[{idx}].step_name",
                    "message": "Duplicate step_name found; use unique step names per rule.",
                    "code": "duplicate_step_name",
                }
            )
        seen_names.add(name)

    signature_to_index: Dict[str, int] = {}
    for idx, rule in enumerate(rules):
        signature = str(
            {
                "event_name_equals": sorted(rule.get("event_name_equals") or []),
                "url_contains": sorted(rule.get("url_contains") or []),
                "url_regex": sorted(rule.get("url_regex") or []),
                "channel_group_equals": sorted(rule.get("channel_group_equals") or []),
                "referrer_contains": sorted(rule.get("referrer_contains") or []),
            }
        )
        if signature in signature_to_index:
            first = signature_to_index[signature]
            warnings.append(
                {
                    "path": f"step_canonicalization.rules[{idx}]",
                    "message": f"Rule overlaps with rules[{first}] (same matching clauses). First-match priority may hide this rule.",
                    "code": "overlapping_rule",
                }
            )
        else:
            signature_to_index[signature] = idx

    if (
        normalized["sessionization"]["conversion_journeys_only"] is False
        and normalized["sessionization"]["allow_all_journeys"] is False
    ):
        errors.append(
            {
                "path": "sessionization",
                "message": "At least one journey mode must be enabled (conversion_journeys_only or allow_all_journeys).",
                "code": "invalid_mode",
            }
        )

    if normalized["flow_defaults"]["min_volume_threshold"] <= 2:
        warnings.append(
            {
                "path": "flow_defaults.min_volume_threshold",
                "message": "Very low threshold may produce noisy Sankey charts.",
                "code": "noise_risk",
            }
        )

    if normalized["sessionization"]["lookback_window_days"] > 90:
        warnings.append(
            {
                "path": "sessionization.lookback_window_days",
                "message": "Lookback > 90 days increases compute cost for journey aggregation and flow queries.",
                "code": "cost_risk",
            }
        )

    return normalized, errors, warnings


def validate_journey_settings(settings_json: Dict[str, Any]) -> Dict[str, Any]:
    normalized, errors, warnings = _normalize_and_validate(settings_json)
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "normalized": normalized,
    }


def _top_level_diff(old: Optional[Dict[str, Any]], new: Dict[str, Any]) -> Dict[str, Any]:
    old_map = old if isinstance(old, dict) else {}
    changed_keys = sorted(
        key for key in set(old_map.keys()) | set(new.keys()) if old_map.get(key) != new.get(key)
    )
    return {"changed_keys": changed_keys, "old": old_map, "new": new}


def _next_version_label(db: Session) -> str:
    existing = db.query(func.count(JourneySettingsVersion.id)).scalar() or 0
    return f"v{int(existing) + 1}"


def _get_or_create_workspace_settings(db: Session, workspace_id: str = DEFAULT_WORKSPACE_ID) -> WorkspaceSettings:
    row = db.get(WorkspaceSettings, workspace_id)
    if row:
        return row
    row = WorkspaceSettings(
        workspace_id=workspace_id,
        active_journey_settings_version_id=None,
        updated_at=_now(),
        updated_by="system",
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def list_journey_settings_versions(db: Session) -> List[JourneySettingsVersion]:
    return (
        db.query(JourneySettingsVersion)
        .order_by(JourneySettingsVersion.created_at.desc())
        .all()
    )


def get_journey_settings_version(db: Session, version_id: str) -> Optional[JourneySettingsVersion]:
    return db.get(JourneySettingsVersion, version_id)


def create_journey_settings_draft(
    db: Session,
    *,
    created_by: str,
    version_label: Optional[str] = None,
    description: Optional[str] = None,
    settings_json: Optional[Dict[str, Any]] = None,
) -> JourneySettingsVersion:
    payload = _deepcopy(settings_json) if isinstance(settings_json, dict) else default_journey_settings()
    validation = validate_journey_settings(payload)
    normalized = validation.get("normalized") or payload
    item = JourneySettingsVersion(
        id=_new_id(),
        status=JourneySettingsStatus.DRAFT,
        version_label=(version_label or _next_version_label(db)).strip() or _next_version_label(db),
        description=description,
        created_at=_now(),
        updated_at=_now(),
        created_by=created_by,
        settings_json=normalized,
        validation_json={k: v for k, v in validation.items() if k != "normalized"},
        diff_json=_top_level_diff(None, normalized),
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


def update_journey_settings_draft(
    db: Session,
    *,
    version_id: str,
    actor: str,
    settings_json: Dict[str, Any],
    description: Optional[str] = None,
) -> JourneySettingsVersion:
    item = db.get(JourneySettingsVersion, version_id)
    if not item:
        raise ValueError("Journey settings version not found")
    if item.status != JourneySettingsStatus.DRAFT:
        raise ValueError("Only draft versions can be updated")

    validation = validate_journey_settings(settings_json or {})
    normalized = validation.get("normalized")
    if normalized is None:
        raise ValueError("Validation failed for draft update")

    old_settings = _deepcopy(item.settings_json or {})
    item.settings_json = normalized
    item.validation_json = {k: v for k, v in validation.items() if k != "normalized"}
    item.diff_json = _top_level_diff(old_settings, normalized)
    if description is not None:
        item.description = description
    item.updated_at = _now()
    item.created_by = actor or item.created_by
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


def archive_journey_settings_version(
    db: Session,
    *,
    version_id: str,
    actor: str,
) -> JourneySettingsVersion:
    item = db.get(JourneySettingsVersion, version_id)
    if not item:
        raise ValueError("Journey settings version not found")
    if item.status == JourneySettingsStatus.ACTIVE:
        raise ValueError("Active journey settings cannot be archived directly")
    item.status = JourneySettingsStatus.ARCHIVED
    item.updated_at = _now()
    item.activated_by = actor or item.activated_by
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


def _active_version_for_workspace(db: Session, workspace_id: str = DEFAULT_WORKSPACE_ID) -> Optional[JourneySettingsVersion]:
    ws = _get_or_create_workspace_settings(db, workspace_id)
    if ws.active_journey_settings_version_id:
        linked = db.get(JourneySettingsVersion, ws.active_journey_settings_version_id)
        if linked and linked.status == JourneySettingsStatus.ACTIVE:
            return linked

    fallback = (
        db.query(JourneySettingsVersion)
        .filter(JourneySettingsVersion.status == JourneySettingsStatus.ACTIVE)
        .order_by(JourneySettingsVersion.activated_at.desc(), JourneySettingsVersion.updated_at.desc())
        .first()
    )
    if fallback and ws.active_journey_settings_version_id != fallback.id:
        ws.active_journey_settings_version_id = fallback.id
        ws.updated_at = _now()
        ws.updated_by = "system"
        db.add(ws)
        db.commit()
    return fallback


def ensure_active_journey_settings(
    db: Session,
    *,
    actor: str = "system",
    workspace_id: str = DEFAULT_WORKSPACE_ID,
) -> JourneySettingsVersion:
    active = _active_version_for_workspace(db, workspace_id=workspace_id)
    if active:
        return active

    draft = create_journey_settings_draft(
        db,
        created_by=actor,
        version_label="v1",
        description="Initial default journeys settings",
        settings_json=default_journey_settings(),
    )
    return activate_journey_settings_version(
        db,
        version_id=draft.id,
        actor=actor,
        workspace_id=workspace_id,
    )


def get_active_journey_settings(
    db: Session,
    *,
    workspace_id: str = DEFAULT_WORKSPACE_ID,
    use_cache: bool = True,
) -> Dict[str, Any]:
    if use_cache:
        exp: Optional[datetime] = _ACTIVE_SETTINGS_CACHE.get("expires_at")
        if (
            exp
            and exp > _now()
            and _ACTIVE_SETTINGS_CACHE.get("workspace_id") == workspace_id
            and isinstance(_ACTIVE_SETTINGS_CACHE.get("settings_json"), dict)
        ):
            return {
                "version_id": _ACTIVE_SETTINGS_CACHE.get("version_id"),
                "settings_json": _deepcopy(_ACTIVE_SETTINGS_CACHE.get("settings_json")),
            }

    active = ensure_active_journey_settings(db, workspace_id=workspace_id)
    payload = _deepcopy(active.settings_json or default_journey_settings())
    _ACTIVE_SETTINGS_CACHE.update(
        {
            "workspace_id": workspace_id,
            "version_id": active.id,
            "settings_json": payload,
            "expires_at": _now() + timedelta(seconds=ACTIVE_SETTINGS_CACHE_TTL_SECONDS),
        }
    )
    return {"version_id": active.id, "settings_json": payload}


def invalidate_active_journey_settings_cache() -> None:
    _ACTIVE_SETTINGS_CACHE.update(
        {
            "workspace_id": None,
            "version_id": None,
            "settings_json": None,
            "expires_at": None,
        }
    )


def build_journey_settings_impact_preview(
    db: Session,
    *,
    draft_settings_json: Dict[str, Any],
    workspace_id: str = DEFAULT_WORKSPACE_ID,
) -> Dict[str, Any]:
    validation = validate_journey_settings(draft_settings_json)
    normalized = validation.get("normalized")
    if normalized is None:
        return {
            "preview_available": False,
            "reason": "Draft settings validation failed",
            "validation": {k: v for k, v in validation.items() if k != "normalized"},
        }

    active = ensure_active_journey_settings(db, workspace_id=workspace_id)
    baseline = active.settings_json or default_journey_settings()
    diff = _top_level_diff(baseline, normalized)

    end_day = datetime.utcnow().date()
    start_day = end_day - timedelta(days=7)
    recent_paths = (
        db.query(func.count(JourneyPathDaily.id))
        .filter(JourneyPathDaily.date >= start_day, JourneyPathDaily.date <= end_day)
        .scalar()
        or 0
    )
    recent_transitions = (
        db.query(func.count(JourneyTransitionDaily.id))
        .filter(
            JourneyTransitionDaily.date >= start_day,
            JourneyTransitionDaily.date <= end_day,
        )
        .scalar()
        or 0
    )
    distinct_steps = (
        db.query(func.count(func.distinct(JourneyTransitionDaily.from_step)))
        .filter(
            JourneyTransitionDaily.date >= start_day,
            JourneyTransitionDaily.date <= end_day,
        )
        .scalar()
        or 0
    )

    baseline_rules = len((baseline.get("step_canonicalization") or {}).get("rules") or [])
    draft_rules = len((normalized.get("step_canonicalization") or {}).get("rules") or [])

    warnings: List[str] = []
    baseline_lb = int((baseline.get("sessionization") or {}).get("lookback_window_days") or 30)
    draft_lb = int((normalized.get("sessionization") or {}).get("lookback_window_days") or 30)
    if draft_lb > baseline_lb:
        warnings.append("Lookback window increased: higher compute and slower queries are expected.")

    baseline_threshold = int((baseline.get("flow_defaults") or {}).get("min_volume_threshold") or 20)
    draft_threshold = int((normalized.get("flow_defaults") or {}).get("min_volume_threshold") or 20)
    if draft_threshold < baseline_threshold:
        warnings.append("Flow min volume threshold lowered: charts may become noisier.")

    overlap_warnings = [
        w["message"]
        for w in validation.get("warnings", [])
        if w.get("code") == "overlapping_rule"
    ]
    warnings.extend(overlap_warnings[:3])

    baseline_metrics = {
        "step_rules": baseline_rules,
        "flow_min_volume_threshold": baseline_threshold,
        "lookback_window_days": baseline_lb,
        "recent_paths_7d": int(recent_paths),
        "recent_transitions_7d": int(recent_transitions),
        "distinct_steps_7d": int(distinct_steps),
    }
    draft_metrics = {
        "step_rules": draft_rules,
        "flow_min_volume_threshold": draft_threshold,
        "lookback_window_days": draft_lb,
        "recent_paths_7d": int(recent_paths),
        "recent_transitions_7d": int(recent_transitions),
        "distinct_steps_7d": int(distinct_steps),
    }
    deltas = {
        k: float(draft_metrics.get(k, 0) - baseline_metrics.get(k, 0))
        for k in ("step_rules", "flow_min_volume_threshold", "lookback_window_days")
    }
    estimated_paths_returned = min(
        int((normalized.get("paths_explorer_defaults") or {}).get("top_paths_limit") or 50),
        int(recent_paths),
    )
    return {
        "preview_available": True,
        "changed_keys": diff.get("changed_keys", []),
        "baseline": baseline_metrics,
        "draft": draft_metrics,
        "deltas": deltas,
        "estimated_paths_returned": estimated_paths_returned,
        "warnings": warnings,
        "validation": {k: v for k, v in validation.items() if k != "normalized"},
    }


def activate_journey_settings_version(
    db: Session,
    *,
    version_id: str,
    actor: str,
    workspace_id: str = DEFAULT_WORKSPACE_ID,
    activation_note: Optional[str] = None,
) -> JourneySettingsVersion:
    item = db.get(JourneySettingsVersion, version_id)
    if not item:
        raise ValueError("Journey settings version not found")
    if item.status == JourneySettingsStatus.ARCHIVED:
        raise ValueError("Archived versions cannot be activated")

    validation = validate_journey_settings(item.settings_json or {})
    normalized = validation.get("normalized")
    if normalized is None:
        raise ValueError("Cannot activate invalid settings")

    active = _active_version_for_workspace(db, workspace_id=workspace_id)
    baseline = _deepcopy(active.settings_json) if active else None
    if active and active.id != item.id:
        active.status = JourneySettingsStatus.ARCHIVED
        active.updated_at = _now()
        db.add(active)

    item.settings_json = normalized
    item.validation_json = {k: v for k, v in validation.items() if k != "normalized"}
    item.status = JourneySettingsStatus.ACTIVE
    item.activated_at = _now()
    item.activated_by = actor
    item.updated_at = _now()
    item.diff_json = _top_level_diff(baseline, normalized)
    if activation_note:
        meta = _deepcopy(item.diff_json or {})
        meta["activation_note"] = activation_note
        item.diff_json = meta
    db.add(item)

    ws = _get_or_create_workspace_settings(db, workspace_id=workspace_id)
    ws.active_journey_settings_version_id = item.id
    ws.updated_at = _now()
    ws.updated_by = actor
    db.add(ws)

    db.commit()
    db.refresh(item)
    invalidate_active_journey_settings_cache()
    return item

