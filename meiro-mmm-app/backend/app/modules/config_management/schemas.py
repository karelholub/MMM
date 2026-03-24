from typing import Any, Dict, Optional

from pydantic import BaseModel


class ModelConfigPayload(BaseModel):
    name: str
    config_json: Dict[str, Any]
    change_note: Optional[str] = None
    created_by: str = "system"


class ModelConfigUpdatePayload(BaseModel):
    config_json: Dict[str, Any]
    change_note: Optional[str] = None
    actor: str = "system"


class ModelConfigActivatePayload(BaseModel):
    actor: str = "system"
    set_as_default: bool = True
    activation_note: Optional[str] = None


class ModelConfigValidatePayload(BaseModel):
    config_json: Optional[Dict[str, Any]] = None


class ModelConfigPreviewPayload(BaseModel):
    config_json: Optional[Dict[str, Any]] = None


class ModelConfigSuggestPayload(BaseModel):
    strategy: str = "balanced"


class JourneySettingsVersionCreatePayload(BaseModel):
    version_label: Optional[str] = None
    description: Optional[str] = None
    settings_json: Optional[Dict[str, Any]] = None
    created_by: str = "system"


class JourneySettingsVersionUpdatePayload(BaseModel):
    settings_json: Dict[str, Any]
    description: Optional[str] = None
    actor: str = "system"


class JourneySettingsValidatePayload(BaseModel):
    settings_json: Optional[Dict[str, Any]] = None
    version_id: Optional[str] = None


class JourneySettingsPreviewPayload(BaseModel):
    settings_json: Optional[Dict[str, Any]] = None
    version_id: Optional[str] = None


class JourneySettingsActivatePayload(BaseModel):
    version_id: str
    actor: str = "system"
    activation_note: Optional[str] = None
    confirm: bool = False
