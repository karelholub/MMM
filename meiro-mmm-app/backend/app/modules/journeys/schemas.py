from typing import Optional

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
