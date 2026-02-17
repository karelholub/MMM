from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol


@dataclass
class OAuthError:
    code: str
    message: str
    retryable: bool = False
    needs_reauth: bool = False
    configuration: bool = False


class OAuthProvider(Protocol):
    key: str

    def scopes(self) -> List[str]:
        ...

    def build_auth_url(
        self,
        *,
        client_id: str,
        redirect_uri: str,
        state: str,
        code_challenge: str,
    ) -> str:
        ...

    def exchange_code_for_token(
        self,
        *,
        code: str,
        code_verifier: str,
        redirect_uri: str,
        client_id: str,
        client_secret: str,
    ) -> Dict[str, Any]:
        ...

    def refresh_access_token(
        self,
        *,
        refresh_token: str,
        client_id: str,
        client_secret: str,
    ) -> Dict[str, Any]:
        ...

    def fetch_accounts(self, *, access_token: str, credentials: Dict[str, str]) -> List[Dict[str, Any]]:
        ...

    def normalize_error(self, exc: Exception) -> OAuthError:
        ...
