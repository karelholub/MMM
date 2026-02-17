from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urlencode

import requests

from app.connectors.oauth.base import OAuthError


class MetaAdsOAuthProvider:
    key = "meta_ads"

    def scopes(self) -> List[str]:
        return ["ads_read", "ads_management", "business_management"]

    def build_auth_url(
        self,
        *,
        client_id: str,
        redirect_uri: str,
        state: str,
        code_challenge: str,
    ) -> str:
        params = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": ",".join(self.scopes()),
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        return f"https://www.facebook.com/v19.0/dialog/oauth?{urlencode(params)}"

    def exchange_code_for_token(
        self,
        *,
        code: str,
        code_verifier: str,
        redirect_uri: str,
        client_id: str,
        client_secret: str,
    ) -> Dict[str, Any]:
        r = requests.get(
            "https://graph.facebook.com/v19.0/oauth/access_token",
            params={
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": redirect_uri,
                "code": code,
                "code_verifier": code_verifier,
            },
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def refresh_access_token(
        self,
        *,
        refresh_token: str,
        client_id: str,
        client_secret: str,
    ) -> Dict[str, Any]:
        # Meta long-lived token refresh pattern.
        r = requests.get(
            "https://graph.facebook.com/v19.0/oauth/access_token",
            params={
                "grant_type": "fb_exchange_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "fb_exchange_token": refresh_token,
            },
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def fetch_accounts(self, *, access_token: str, credentials: Dict[str, str]) -> List[Dict[str, Any]]:
        r = requests.get(
            "https://graph.facebook.com/v19.0/me/adaccounts",
            params={
                "fields": "id,name,account_status",
                "limit": 200,
                "access_token": access_token,
            },
            timeout=30,
        )
        r.raise_for_status()
        rows = r.json().get("data") or []
        return [
            {
                "id": row.get("id"),
                "name": row.get("name") or row.get("id"),
                "status": row.get("account_status"),
                "provider": self.key,
            }
            for row in rows
            if row.get("id")
        ]

    def normalize_error(self, exc: Exception) -> OAuthError:
        text = str(exc)
        lower = text.lower()
        if "invalid_grant" in lower or "invalid oauth access token" in lower or "session has expired" in lower:
            return OAuthError(code="invalid_grant", message="Meta token is invalid or expired", needs_reauth=True)
        if "redirect_uri" in lower:
            return OAuthError(
                code="redirect_uri_mismatch",
                message="Redirect URL mismatch. Ensure callback URL is in Meta Valid OAuth Redirect URIs.",
                configuration=True,
            )
        if "rate limit" in lower or "timeout" in lower:
            return OAuthError(code="network", message="Temporary Meta API issue", retryable=True)
        return OAuthError(code="provider_error", message=text or "Meta OAuth failed")
