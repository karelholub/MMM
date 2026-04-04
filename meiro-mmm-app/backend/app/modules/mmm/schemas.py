from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


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
    channel_display_names: Optional[Dict[str, str]] = None

    def __init__(self, **data):
        if "priors" not in data or data["priors"] is None:
            data["priors"] = {
                "adstock": {"alpha_mean": 0.5, "alpha_sd": 0.2},
                "saturation": {"lam_mean": 0.001, "lam_sd": 0.0005},
            }
        if "mcmc" not in data or data["mcmc"] is None:
            data["mcmc"] = {"draws": 1000, "tune": 1000, "chains": 4, "target_accept": 0.9}
        if "kpi" not in data or data["kpi"] is None:
            if "kpi_mode" in data:
                data["kpi"] = {"conversions": "conversions", "aov": "aov", "profit": "profit"}.get(data["kpi_mode"], "conversions")
        super().__init__(**data)


class ChannelConstraint(BaseModel):
    min: float | None = None
    max: float | None = None
    locked: bool = False


class OptimizeRequest(BaseModel):
    total_budget: float = 1.0
    min_spend: float = 0.5
    max_spend: float = 2.0
    channel_constraints: Dict[str, ChannelConstraint] | None = None


class BudgetRecommendationQuery(BaseModel):
    objective: str = "protect_efficiency"
    total_budget_change_pct: float = 0.0


class BudgetScenarioCreateRequest(BaseModel):
    objective: str = "protect_efficiency"
    total_budget_change_pct: float = 0.0
    multipliers: Dict[str, float] = Field(default_factory=dict)
    recommendations: List[Dict[str, Any]] = Field(default_factory=list)


class BuildFromPlatformRequest(BaseModel):
    date_start: str
    date_end: str
    kpi_target: str = "sales"
    spend_channels: List[str]
    covariates: Optional[List[str]] = None
    currency: str = "USD"
    attribution_model: Optional[str] = None
    attribution_config_id: Optional[str] = None


class ValidateMappingRequest(BaseModel):
    date_column: str
    kpi: str
    spend_channels: List[str]
    covariates: Optional[List[str]] = None
    kpi_target: Optional[str] = None
