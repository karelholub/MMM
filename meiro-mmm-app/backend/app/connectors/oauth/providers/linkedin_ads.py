from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urlencode

import requests

from app.connectors.oauth.base import OAuthError


class LinkedInAdsOAuthProvider:
    key = "linkedin_ads"

    def scopes(self) -> List[str]:
        return ["r_ads", "r_ads_reporting", "rw_organization_admin"]

    def build_auth_url(
        self,
        *,
        client_id: str,
        redirect_uri: str,
        state: str,
        code_challenge: str,
    ) -> str:
        params = {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": " ".join(self.scopes()),
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        return f"https://www.linkedin.com/oauth/v2/authorization?{urlencode(params)}"

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
            "https://www.linkedin.com/oauth/v2/accessToken",
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
            "https://www.linkedin.com/oauth/v2/accessToken",
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
        r = requests.get(
            "https://api.linkedin.com/v2/adAccountsV2",
            params={"q": "search", "search.status.values[0]": "ACTIVE", "count": 100},
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30,
        )
        r.raise_for_status()
        elements = r.json().get("elements") or []
        out: List[Dict[str, Any]] = []
        for row in elements:
            account_id = row.get("id")
            if not account_id:
                continue
            out.append({
                "id": str(account_id),
                "name": row.get("name") or str(account_id),
                "status": row.get("status"),
                "provider": self.key,
            })
        return out

    def normalize_error(self, exc: Exception) -> OAuthError:
        text = str(exc)
        lower = text.lower()
        if "invalid_grant" in lower or "expired" in lower or "revoked" in lower:
            return OAuthError(code="invalid_grant", message="LinkedIn token is invalid or expired", needs_reauth=True)
        if "redirect_uri" in lower:
            return OAuthError(
                code="redirect_uri_mismatch",
                message="Redirect URL mismatch. LinkedIn requires exact redirect_uri match.",
                configuration=True,
            )
        if "rate" in lower or "timeout" in lower:
            return OAuthError(code="network", message="Temporary LinkedIn API issue", retryable=True)
        return OAuthError(code="provider_error", message=text or "LinkedIn OAuth failed")
