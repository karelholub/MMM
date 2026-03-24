from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


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
