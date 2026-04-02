from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class AttributionSettings(BaseModel):
    lookback_window_days: int = 30
    use_converted_flag: bool = True
    conversion_value_mode: str = "gross_only"
    min_journey_quality_score: int = Field(default=0, ge=0, le=100)
    min_conversion_value: float = 0.0
    time_decay_half_life_days: float = 7.0
    position_first_pct: float = 0.4
    position_last_pct: float = 0.4
    markov_min_paths: int = 5


class MMMSettings(BaseModel):
    frequency: str = "W"


class NBASettings(BaseModel):
    min_prefix_support: int = 5
    min_conversion_rate: float = 0.01
    max_prefix_depth: int = 5
    min_next_support: int = 5
    max_suggestions_per_prefix: int = 3
    min_uplift_pct: Optional[float] = None
    excluded_channels: List[str] = Field(default_factory=lambda: ["direct"])


class FeatureFlags(BaseModel):
    mmm_enabled: bool = False
    journeys_enabled: bool = False
    journey_examples_enabled: bool = False
    funnel_builder_enabled: bool = False
    funnel_diagnostics_enabled: bool = False
    access_control_enabled: bool = False
    custom_roles_enabled: bool = False
    audit_log_enabled: bool = False
    scim_enabled: bool = False
    sso_enabled: bool = False


class AdsGovernanceSettings(BaseModel):
    require_approval: bool = True
    max_budget_change_pct: float = 30.0


class RevenueConfig(BaseModel):
    conversion_names: List[str] = Field(default_factory=lambda: ["purchase"])
    value_field_path: str = "value"
    currency_field_path: str = "currency"
    dedup_key: str = "conversion_id"
    default_value: float = 0.0
    default_value_mode: str = "missing_only"
    per_conversion_overrides: List[Dict[str, Any]] = Field(default_factory=list)
    base_currency: str = "EUR"
    fx_enabled: bool = False
    fx_mode: str = "none"
    fx_rates_json: Dict[str, float] = Field(default_factory=dict)
    source_type: str = "conversion_event"


class Settings(BaseModel):
    attribution: AttributionSettings = AttributionSettings()
    mmm: MMMSettings = MMMSettings()
    nba: NBASettings = NBASettings()
    feature_flags: FeatureFlags = FeatureFlags()
    ads_governance: AdsGovernanceSettings = AdsGovernanceSettings()
    revenue_config: RevenueConfig = RevenueConfig()


class KpiDefinitionModel(BaseModel):
    id: str
    label: str
    type: str
    event_name: str
    value_field: Optional[str] = None
    weight: float = 1.0
    lookback_days: Optional[int] = None


class KpiConfigModel(BaseModel):
    definitions: List[KpiDefinitionModel]
    primary_kpi_id: Optional[str] = None


class KpiTestPayload(BaseModel):
    definition: KpiDefinitionModel


class NotificationChannelCreate(BaseModel):
    type: str
    config: Dict[str, Any] = Field(default_factory=dict)
    slack_webhook_url: Optional[str] = None


class NotificationChannelUpdate(BaseModel):
    config: Optional[Dict[str, Any]] = None
    slack_webhook_url: Optional[str] = None


class NotificationPrefUpdate(BaseModel):
    severities: Optional[List[str]] = None
    digest_mode: Optional[str] = None
    quiet_hours: Optional[Dict[str, Any]] = None
    is_enabled: Optional[bool] = None


class NotificationPrefUpsert(BaseModel):
    channel_id: int
    severities: List[str] = Field(default_factory=list)
    digest_mode: str = "realtime"
    quiet_hours: Optional[Dict[str, Any]] = None
    is_enabled: bool = False
