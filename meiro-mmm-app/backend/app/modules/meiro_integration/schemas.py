from typing import List, Optional

from pydantic import BaseModel, Field


class MeiroCDPConnectRequest(BaseModel):
    api_base_url: str
    api_key: str


class MeiroCDPTestRequest(BaseModel):
    api_base_url: Optional[str] = None
    api_key: Optional[str] = None
    save_on_success: bool = False


class MeiroCDPExportRequest(BaseModel):
    since: str
    until: str
    event_types: Optional[List[str]] = None
    attributes: Optional[List[str]] = None
    segment_id: Optional[str] = None


class MeiroWebhookReprocessRequest(BaseModel):
    archive_limit: Optional[int] = None
    replay_mode: Optional[str] = "last_n"
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    persist_to_attribution: bool = False
    config_id: Optional[str] = None
    import_note: Optional[str] = None


class MeiroMappingApprovalRequest(BaseModel):
    status: str
    note: Optional[str] = None
