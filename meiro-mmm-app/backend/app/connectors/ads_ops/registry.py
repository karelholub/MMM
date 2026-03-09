from __future__ import annotations

from typing import Dict, List

from app.connectors.ads_ops.base import AdsProviderAdapter
from app.connectors.ads_ops.providers.google_ads import GoogleAdsAdapter
from app.connectors.ads_ops.providers.meta_ads import MetaAdsAdapter
from app.connectors.ads_ops.providers.linkedin_ads import LinkedInAdsAdapter


_REGISTRY: Dict[str, AdsProviderAdapter] = {
    "google_ads": GoogleAdsAdapter(),
    "meta_ads": MetaAdsAdapter(),
    "linkedin_ads": LinkedInAdsAdapter(),
}


def get_ads_adapter(provider_key: str) -> AdsProviderAdapter:
    key = (provider_key or "").strip().lower()
    if key not in _REGISTRY:
        raise ValueError(f"Unsupported ads provider: {provider_key}")
    return _REGISTRY[key]


def list_ads_adapter_keys() -> List[str]:
    return sorted(_REGISTRY.keys())
