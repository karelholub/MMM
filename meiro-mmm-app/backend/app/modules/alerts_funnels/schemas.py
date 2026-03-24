from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


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
