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
    JourneyInstanceFact,
    JourneySettingsStatus,
    JourneySettingsVersion,
    JourneyStepFact,
    SilverTouchpointFact,
    WorkspaceSettings,
)
from .services_journey_path_outputs import count_recent_path_outputs
from .services_journey_transition_outputs import count_recent_transition_from_steps, count_recent_transition_outputs

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


def _top_string_counts(db: Session, column: Any, *, limit: int = 12) -> List[Dict[str, Any]]:
    rows = (
        db.query(column, func.count().label("count"))
        .filter(column.isnot(None))
        .filter(column != "")
        .group_by(column)
        .order_by(func.count().desc(), column.asc())
        .limit(max(1, min(limit, 50)))
        .all()
    )
    return [{"value": str(value), "count": int(count or 0)} for value, count in rows if str(value or "").strip()]


def _suggest_step_rules(
    *,
    channels: List[Dict[str, Any]],
    event_names: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    channel_values = {str(item.get("value") or "").strip().lower() for item in channels}
    event_values = {str(item.get("value") or "").strip().lower() for item in event_names}
    rules: List[Dict[str, Any]] = []
    priority = 10

    if any(value in channel_values for value in ("paid_search", "paid_social", "google_ads", "meta_ads")):
        rules.append(
            {
                "step_name": "Paid Landing",
                "priority": priority,
                "enabled": True,
                "channel_group_equals": sorted(
                    [value for value in channel_values if value in {"paid_search", "paid_social", "google_ads", "meta_ads"}]
                ),
            }
        )
        priority += 10
    if any(value in channel_values for value in ("organic_search", "referral", "direct")):
        rules.append(
            {
                "step_name": "Organic / Direct Landing",
                "priority": priority,
                "enabled": True,
                "channel_group_equals": sorted(
                    [value for value in channel_values if value in {"organic_search", "referral", "direct"}]
                ),
            }
        )
        priority += 10

    event_groups = [
        ("Product / Content View", {"product_view", "content_view", "page_view", "view_item"}),
        ("Checkout / Intent", {"add_to_cart", "begin_checkout", "checkout", "form_start", "form_submit"}),
        ("Conversion", {"purchase", "lead", "lead_won", "sign_up", "subscribe"}),
    ]
    for step_name, candidates in event_groups:
        matched = sorted([value for value in event_values if value in candidates])
        if matched:
            rules.append(
                {
                    "step_name": step_name,
                    "priority": priority,
                    "enabled": True,
                    "event_name_equals": matched,
                }
            )
            priority += 10
    return rules


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


def _build_step_rule_evidence(
    db: Session,
    *,
    normalized_settings_json: Dict[str, Any],
) -> Dict[str, Any]:
    rules = list((normalized_settings_json.get("step_canonicalization") or {}).get("rules") or [])
    total_touchpoints = int(db.query(func.count(SilverTouchpointFact.id)).scalar() or 0)
    observed_channels = {
        str(value).strip().lower()
        for value, in db.query(SilverTouchpointFact.channel)
        .filter(SilverTouchpointFact.channel.isnot(None))
        .filter(SilverTouchpointFact.channel != "")
        .distinct()
        .all()
        if str(value or "").strip()
    }
    observed_events = {
        str(value).strip().lower()
        for value, in db.query(SilverTouchpointFact.event_name)
        .filter(SilverTouchpointFact.event_name.isnot(None))
        .filter(SilverTouchpointFact.event_name != "")
        .distinct()
        .all()
        if str(value or "").strip()
    }

    rule_rows: List[Dict[str, Any]] = []
    for idx, rule in enumerate(rules):
        channels = sorted({str(value).strip().lower() for value in (rule.get("channel_group_equals") or []) if str(value or "").strip()})
        events = sorted({str(value).strip().lower() for value in (rule.get("event_name_equals") or []) if str(value or "").strip()})
        unsupported_clauses = [
            key
            for key in ("url_contains", "url_regex", "referrer_contains", "custom_predicate")
            if rule.get(key)
        ]

        query = db.query(func.count(SilverTouchpointFact.id))
        if channels:
            query = query.filter(func.lower(SilverTouchpointFact.channel).in_(channels))
        if events:
            query = query.filter(func.lower(SilverTouchpointFact.event_name).in_(events))
        matched_touchpoints = int(query.scalar() or 0) if (channels or events) else 0

        warnings: List[str] = []
        if not channels and not events and not unsupported_clauses:
            warnings.append("No supported evidence clauses found for preview matching.")
        if unsupported_clauses:
            warnings.append(
                "Preview evidence ignores clauses not stored in current silver facts: "
                + ", ".join(sorted(unsupported_clauses))
                + "."
            )
        unknown_channels = [value for value in channels if value not in observed_channels]
        if unknown_channels:
            warnings.append("Channels not observed in workspace facts: " + ", ".join(unknown_channels) + ".")
        unknown_events = [value for value in events if value not in observed_events]
        if unknown_events:
            warnings.append("Event names not observed in workspace facts: " + ", ".join(unknown_events) + ".")
        if (channels or events) and matched_touchpoints == 0:
            warnings.append("Rule matches zero observed touchpoints in current workspace facts.")

        rule_rows.append(
            {
                "index": idx,
                "step_name": rule.get("step_name"),
                "enabled": bool(rule.get("enabled", True)),
                "priority": int(rule.get("priority") or 0),
                "matched_touchpoints": matched_touchpoints,
                "match_share": (matched_touchpoints / total_touchpoints) if total_touchpoints else 0.0,
                "channels": channels,
                "events": events,
                "unsupported_clauses": sorted(unsupported_clauses),
                "warnings": warnings,
            }
        )

    return {
        "summary": {
            "rule_count": len(rules),
            "total_touchpoints": total_touchpoints,
            "rules_with_matches": sum(1 for row in rule_rows if row["matched_touchpoints"] > 0),
        },
        "rules": rule_rows,
    }


def build_journey_settings_validation_report(
    db: Session,
    *,
    settings_json: Dict[str, Any],
) -> Dict[str, Any]:
    validation = validate_journey_settings(settings_json)
    normalized = validation.get("normalized")
    rule_evidence = _build_step_rule_evidence(db, normalized_settings_json=normalized) if normalized else None
    return {
        "valid": validation["valid"],
        "errors": validation["errors"],
        "warnings": validation["warnings"],
        "normalized": normalized,
        "rule_evidence": rule_evidence,
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
    validation = build_journey_settings_validation_report(db, settings_json=draft_settings_json)
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
    recent_paths = count_recent_path_outputs(db, date_from=start_day, date_to=end_day)
    recent_transitions = count_recent_transition_outputs(db, date_from=start_day, date_to=end_day)
    distinct_steps = count_recent_transition_from_steps(db, date_from=start_day, date_to=end_day)

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
        "rule_evidence": validation.get("rule_evidence"),
    }


def build_journey_settings_context(
    db: Session,
    *,
    kpi_config: Optional[Any] = None,
    workspace_id: str = DEFAULT_WORKSPACE_ID,
) -> Dict[str, Any]:
    active_payload = get_active_journey_settings(db, workspace_id=workspace_id, use_cache=False)
    active_settings_json = _deepcopy(active_payload.get("settings_json") or default_journey_settings())
    schema_defaults = default_journey_settings()

    observed_channels = _top_string_counts(db, JourneyInstanceFact.channel_group, limit=12)
    observed_event_names = _top_string_counts(db, SilverTouchpointFact.event_name, limit=20)
    observed_steps = _top_string_counts(db, JourneyStepFact.step_name, limit=15)
    observed_conversion_keys = _top_string_counts(db, JourneyInstanceFact.conversion_key, limit=12)
    journeys_loaded = int(db.query(func.count(JourneyInstanceFact.id)).scalar() or 0)

    recommended_min_volume_threshold = 10 if journeys_loaded < 500 else 20 if journeys_loaded < 5_000 else 30
    recommended_top_paths_limit = 50 if journeys_loaded < 2_000 else 75 if journeys_loaded < 10_000 else 100
    recommended_max_nodes = 20 if journeys_loaded < 1_000 else 30 if journeys_loaded < 10_000 else 40

    scaffold_settings_json = _deepcopy(active_settings_json)
    active_rules = list((scaffold_settings_json.get("step_canonicalization") or {}).get("rules") or [])
    default_rules = list((schema_defaults.get("step_canonicalization") or {}).get("rules") or [])
    if not active_rules or active_rules == default_rules:
        scaffold_settings_json["step_canonicalization"]["rules"] = _suggest_step_rules(
            channels=observed_channels,
            event_names=observed_event_names,
        )
    if (scaffold_settings_json.get("flow_defaults") or {}).get("min_volume_threshold") == (
        (schema_defaults.get("flow_defaults") or {}).get("min_volume_threshold")
    ):
        scaffold_settings_json["flow_defaults"]["min_volume_threshold"] = recommended_min_volume_threshold
    if (scaffold_settings_json.get("flow_defaults") or {}).get("max_nodes") == (
        (schema_defaults.get("flow_defaults") or {}).get("max_nodes")
    ):
        scaffold_settings_json["flow_defaults"]["max_nodes"] = recommended_max_nodes
    if (scaffold_settings_json.get("paths_explorer_defaults") or {}).get("top_paths_limit") == (
        (schema_defaults.get("paths_explorer_defaults") or {}).get("top_paths_limit")
    ):
        scaffold_settings_json["paths_explorer_defaults"]["top_paths_limit"] = recommended_top_paths_limit

    observed_kpis: List[Dict[str, Any]] = []
    definitions = list(getattr(kpi_config, "definitions", []) or [])
    primary_kpi_id = getattr(kpi_config, "primary_kpi_id", None)
    observed_key_counts = {str(item["value"]): int(item["count"]) for item in observed_conversion_keys}
    for definition in definitions:
        definition_id = str(getattr(definition, "id", "") or "").strip()
        if not definition_id:
            continue
        observed_kpis.append(
            {
                "id": definition_id,
                "label": getattr(definition, "label", definition_id),
                "observed_count": observed_key_counts.get(definition_id, 0),
                "is_primary": definition_id == primary_kpi_id,
            }
        )

    notes: List[str] = []
    if observed_channels:
        notes.append("Channel groups are derived from observed journey instances in this workspace.")
    if observed_event_names:
        notes.append("Step-rule suggestions are seeded from recent observed event names.")
    if not observed_event_names:
        notes.append("No recent event names were found, so step-rule suggestions remain conservative.")

    return {
        "active_version_id": active_payload.get("version_id"),
        "workspace_summary": {
            "journeys_loaded": journeys_loaded,
            "observed_channels": len(observed_channels),
            "observed_event_names": len(observed_event_names),
            "observed_steps": len(observed_steps),
            "observed_conversion_keys": len(observed_conversion_keys),
        },
        "observed_channels": observed_channels,
        "observed_event_names": observed_event_names,
        "observed_steps": observed_steps,
        "observed_conversion_keys": observed_conversion_keys,
        "observed_kpis": observed_kpis,
        "recommendations": {
            "flow_defaults": {
                "min_volume_threshold": recommended_min_volume_threshold,
                "max_nodes": recommended_max_nodes,
            },
            "paths_explorer_defaults": {
                "top_paths_limit": recommended_top_paths_limit,
            },
            "notes": notes,
        },
        "scaffold_settings_json": scaffold_settings_json,
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
