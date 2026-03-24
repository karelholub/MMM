from typing import List, Optional

from pydantic import BaseModel, Field


class JourneySourceActivatePayload(BaseModel):
    source: str = Field(..., pattern="^(sample|upload|meiro)$")
    import_note: Optional[str] = None


class ImportPreCheckResponse(BaseModel):
    would_overwrite: bool
    current_count: int
    current_converted: int
    current_channels: List[str]
    incoming_count: int
    incoming_converted: int
    incoming_channels: List[str]
    converted_drop_pct: Optional[float] = None
    warning: Optional[str] = None


class ImportConfirmRequest(BaseModel):
    confirmed: bool = True
    import_note: Optional[str] = None


class LoadSampleRequest(BaseModel):
    import_note: Optional[str] = None
