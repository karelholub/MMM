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


class BudgetRecommendationTargetPayload(BaseModel):
    channel: str
    provider: str
    entity_id: str
    entity_name: Optional[str] = None
    account_id: Optional[str] = None
    delta_pct: float
    reason: Optional[str] = None


class BudgetRecommendationBulkCreatePayload(BaseModel):
    run_id: str
    scenario_id: str
    recommendation_id: Optional[str] = None
    currency: str = "USD"
    targets: list[BudgetRecommendationTargetPayload] = Field(default_factory=list)
