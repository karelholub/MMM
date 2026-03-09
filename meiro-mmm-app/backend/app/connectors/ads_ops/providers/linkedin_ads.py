from __future__ import annotations

import os
from typing import Any, Dict

from app.connectors.ads_ops.base import AdsApplyResult, AdsProviderError


class LinkedInAdsAdapter:
    key = "linkedin_ads"

    def build_deep_link(self, *, account_id: str, entity_type: str, entity_id: str) -> str:
        aid = str(account_id or "").strip()
        if entity_type == "campaign":
            return f"https://www.linkedin.com/campaignmanager/accounts/{aid}/campaigns/{entity_id}"
        if entity_type == "adgroup":
            return f"https://www.linkedin.com/campaignmanager/accounts/{aid}/campaigns?search={entity_id}"
        return f"https://www.linkedin.com/campaignmanager/accounts/{aid}/campaigns"

    def fetch_entity_state(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str) -> Dict[str, Any]:
        return {"status": "unknown", "budget": None, "currency": None, "name": entity_id}

    def pause_entity(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str, idempotency_key: str) -> AdsApplyResult:
        if os.getenv("ADS_ACTIONS_DRY_RUN", "1") != "0":
            return AdsApplyResult(ok=True, message="dry_run: linkedin pause accepted", meta={"idempotency_key": idempotency_key})
        raise RuntimeError("LinkedIn mutate endpoint not enabled in this environment")

    def enable_entity(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str, idempotency_key: str) -> AdsApplyResult:
        if os.getenv("ADS_ACTIONS_DRY_RUN", "1") != "0":
            return AdsApplyResult(ok=True, message="dry_run: linkedin enable accepted", meta={"idempotency_key": idempotency_key})
        raise RuntimeError("LinkedIn mutate endpoint not enabled in this environment")

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
            return AdsApplyResult(ok=True, message="dry_run: linkedin budget accepted", meta={"daily_budget": daily_budget, "currency": currency, "idempotency_key": idempotency_key})
        raise RuntimeError("LinkedIn budget mutate endpoint not enabled in this environment")

    def normalize_error(self, exc: Exception) -> AdsProviderError:
        msg = str(exc)
        lower = msg.lower()
        if "invalid_grant" in lower or "expired" in lower:
            return AdsProviderError(code="invalid_token", message="LinkedIn authorization expired", needs_reauth=True)
        return AdsProviderError(code="provider_error", message=msg or "LinkedIn provider error")

    def supports(self, action_type: str, entity_type: str) -> bool:
        if action_type in {"pause", "enable", "update_budget"} and entity_type in {"campaign", "adgroup"}:
            return True
        return False
