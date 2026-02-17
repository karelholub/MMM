from __future__ import annotations

from typing import Dict, List

from app.connectors.oauth.base import OAuthProvider
from app.connectors.oauth.providers.google_ads import GoogleAdsOAuthProvider
from app.connectors.oauth.providers.meta_ads import MetaAdsOAuthProvider
from app.connectors.oauth.providers.linkedin_ads import LinkedInAdsOAuthProvider


_PROVIDER_REGISTRY: Dict[str, OAuthProvider] = {
    "google_ads": GoogleAdsOAuthProvider(),
    "meta_ads": MetaAdsOAuthProvider(),
    "linkedin_ads": LinkedInAdsOAuthProvider(),
}


def get_oauth_provider(provider_key: str) -> OAuthProvider:
    key = (provider_key or "").strip().lower()
    if key not in _PROVIDER_REGISTRY:
        raise ValueError(f"Unsupported OAuth provider: {provider_key}")
    return _PROVIDER_REGISTRY[key]


def list_oauth_provider_keys() -> List[str]:
    return sorted(_PROVIDER_REGISTRY.keys())
