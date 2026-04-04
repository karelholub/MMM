from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


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


class JourneySavedViewPayload(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    journey_definition_id: Optional[str] = Field(None, max_length=36)
    state: Dict[str, Any] = Field(default_factory=dict)


class JourneyHypothesisPayload(BaseModel):
    journey_definition_id: str = Field(..., min_length=1, max_length=36)
    title: str = Field(..., min_length=1, max_length=255)
    target_kpi: Optional[str] = Field(None, max_length=64)
    hypothesis_text: str = Field(..., min_length=1, max_length=5000)
    trigger: Dict[str, Any] = Field(default_factory=dict)
    segment: Dict[str, Any] = Field(default_factory=dict)
    current_action: Dict[str, Any] = Field(default_factory=dict)
    proposed_action: Dict[str, Any] = Field(default_factory=dict)
    support_count: int = Field(0, ge=0)
    baseline_rate: Optional[float] = Field(None, ge=0)
    sample_size_target: Optional[int] = Field(None, ge=0)
    status: str = Field("draft", max_length=32)
    linked_experiment_id: Optional[int] = Field(None, ge=1)
    result: Dict[str, Any] = Field(default_factory=dict)


class JourneyExperimentCreatePayload(BaseModel):
    start_at: datetime
    end_at: datetime
    name: Optional[str] = Field(None, max_length=255)
    channel: Optional[str] = Field(None, max_length=64)
    notes: Optional[str] = Field(None, max_length=5000)
    experiment_type: str = Field("holdout", max_length=32)
    proposed_step: Optional[str] = Field(None, max_length=255)
    guardrails: Dict[str, Any] = Field(default_factory=dict)


class JourneyPolicySimulationPayload(BaseModel):
    proposed_step: Optional[str] = Field(None, max_length=255)
