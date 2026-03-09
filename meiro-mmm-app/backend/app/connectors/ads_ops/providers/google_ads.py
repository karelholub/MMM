from __future__ import annotations

import os
from typing import Any, Dict

from app.connectors.ads_ops.base import AdsApplyResult, AdsProviderError


class GoogleAdsAdapter:
    key = "google_ads"

    def build_deep_link(self, *, account_id: str, entity_type: str, entity_id: str) -> str:
        cid = (account_id or "").replace("-", "")
        if entity_type == "campaign":
            return f"https://ads.google.com/aw/campaigns?ocid={cid}&campaignId={entity_id}"
        if entity_type == "adgroup":
            return f"https://ads.google.com/aw/adgroups?ocid={cid}&adGroupId={entity_id}"
        return f"https://ads.google.com/aw/overview?ocid={cid}"

    def fetch_entity_state(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str) -> Dict[str, Any]:
        # Lightweight MVP: keep non-destructive and avoid expensive GAQL graph in this pass.
        return {"status": "unknown", "budget": None, "currency": None, "name": entity_id}

    def pause_entity(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str, idempotency_key: str) -> AdsApplyResult:
        if os.getenv("ADS_ACTIONS_DRY_RUN", "1") != "0":
            return AdsApplyResult(ok=True, message="dry_run: google pause accepted", meta={"idempotency_key": idempotency_key})
        raise RuntimeError("Google Ads mutate endpoint not enabled in this environment")

    def enable_entity(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str, idempotency_key: str) -> AdsApplyResult:
        if os.getenv("ADS_ACTIONS_DRY_RUN", "1") != "0":
            return AdsApplyResult(ok=True, message="dry_run: google enable accepted", meta={"idempotency_key": idempotency_key})
        raise RuntimeError("Google Ads mutate endpoint not enabled in this environment")

    def update_budget(
        self,
        *,
        access_token: str,
        account_id: str,
        entity_type: str,
        entity_id: str,
        daily_budget: float,
        currency: str | None,
        idempotency_key: str,
    ) -> AdsApplyResult:
        if os.getenv("ADS_ACTIONS_DRY_RUN", "1") != "0":
            return AdsApplyResult(ok=True, message="dry_run: google budget accepted", meta={"daily_budget": daily_budget, "currency": currency, "idempotency_key": idempotency_key})
        raise RuntimeError("Google Ads budget mutate endpoint not enabled in this environment")

    def normalize_error(self, exc: Exception) -> AdsProviderError:
        msg = str(exc)
        lower = msg.lower()
        if "invalid_grant" in lower or "unauthorized" in lower:
            return AdsProviderError(code="invalid_token", message="Google Ads authorization expired", needs_reauth=True)
        return AdsProviderError(code="provider_error", message=msg or "Google Ads provider error")

    def supports(self, action_type: str, entity_type: str) -> bool:
        if action_type in {"pause", "enable", "update_budget"} and entity_type in {"campaign", "adgroup"}:
            return True
        return False
