from typing import List, Optional

from pydantic import BaseModel, Field


class AuthLoginPayload(BaseModel):
    email: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    provider: str = "bootstrap"
    name: Optional[str] = None
    workspace_id: str = "default"


class AuthWorkspaceSwitchPayload(BaseModel):
    workspace_id: str


class OAuthStartPayload(BaseModel):
    return_url: Optional[str] = None


class OAuthSelectAccountsPayload(BaseModel):
    account_ids: List[str] = Field(default_factory=list)


class DatasourceCredentialUpdate(BaseModel):
    """Update OAuth credentials for a platform. Only provided fields are updated."""

    platform: str
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    developer_token: Optional[str] = None
    app_id: Optional[str] = None
    app_secret: Optional[str] = None
