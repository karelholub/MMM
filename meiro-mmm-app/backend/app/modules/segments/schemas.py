from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class LocalSegmentPayload(BaseModel):
    name: str
    description: Optional[str] = None
    definition: Dict[str, Any] = Field(default_factory=dict)
