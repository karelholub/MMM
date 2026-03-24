from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


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
