from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urlencode

import requests

from app.connectors.oauth.base import OAuthError


class GoogleAdsOAuthProvider:
    key = "google_ads"

    def scopes(self) -> List[str]:
        return ["https://www.googleapis.com/auth/adwords"]

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
            "scope": " ".join(self.scopes()),
            "access_type": "offline",
            "prompt": "consent",
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        return f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"

    def exchange_code_for_token(
        self,
        *,
        code: str,
        code_verifier: str,
        redirect_uri: str,
        client_id: str,
        client_secret: str,
    ) -> Dict[str, Any]:
        r = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": client_id,
                "client_secret": client_secret,
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
        r = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def fetch_accounts(self, *, access_token: str, credentials: Dict[str, str]) -> List[Dict[str, Any]]:
        developer_token = (credentials.get("developer_token") or "").strip()
        if not developer_token:
            raise RuntimeError("Google Ads developer token is required to list accessible accounts")

        r = requests.get(
            "https://googleads.googleapis.com/v16/customers:listAccessibleCustomers",
            headers={
                "Authorization": f"Bearer {access_token}",
                "developer-token": developer_token,
            },
            timeout=30,
        )
        r.raise_for_status()
        names = r.json().get("resourceNames") or []
        out: List[Dict[str, Any]] = []
        for name in names:
            customer_id = str(name).split("/")[-1]
            out.append({"id": customer_id, "name": customer_id, "provider": self.key})
        return out

    def normalize_error(self, exc: Exception) -> OAuthError:
        text = str(exc)
        lower = text.lower()
        if "invalid_grant" in lower or "token has been expired" in lower or "revoked" in lower:
            return OAuthError(code="invalid_grant", message="Google token is invalid or revoked", needs_reauth=True)
        if "redirect_uri_mismatch" in lower:
            return OAuthError(
                code="redirect_uri_mismatch",
                message="Redirect URL mismatch. Check Google OAuth redirect URI settings.",
                configuration=True,
            )
        if "timeout" in lower or "temporarily unavailable" in lower:
            return OAuthError(code="network", message="Temporary network/provider issue", retryable=True)
        return OAuthError(code="provider_error", message=text or "Google Ads OAuth failed")
