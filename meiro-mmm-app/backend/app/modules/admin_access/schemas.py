from typing import List, Optional

from pydantic import BaseModel, Field


class AdminUserUpdatePayload(BaseModel):
    status: str = Field(..., pattern="^(active|disabled)$")


class AdminMembershipUpdatePayload(BaseModel):
    role_id: str


class AdminInvitationCreatePayload(BaseModel):
    email: str
    role_id: str
    workspace_id: Optional[str] = None
    expires_in_days: int = Field(default=7, ge=1, le=30)


class AdminRoleCreatePayload(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    description: Optional[str] = None
    permission_keys: List[str] = Field(default_factory=list)
    workspace_id: Optional[str] = None


class AdminRoleUpdatePayload(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=128)
    description: Optional[str] = None
    permission_keys: Optional[List[str]] = None


class InvitationAcceptPayload(BaseModel):
    token: str
    name: Optional[str] = None
