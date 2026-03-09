from __future__ import annotations

import os
from typing import Any, Dict

from app.connectors.ads_ops.base import AdsApplyResult, AdsProviderError


class MetaAdsAdapter:
    key = "meta_ads"

    def build_deep_link(self, *, account_id: str, entity_type: str, entity_id: str) -> str:
        act = account_id if str(account_id).startswith("act_") else f"act_{account_id}"
        if entity_type == "campaign":
            return f"https://adsmanager.facebook.com/adsmanager/manage/campaigns?act={act}&selected_campaign_ids={entity_id}"
        if entity_type == "adset":
            return f"https://adsmanager.facebook.com/adsmanager/manage/adsets?act={act}&selected_adset_ids={entity_id}"
        return f"https://adsmanager.facebook.com/adsmanager/manage/campaigns?act={act}"

    def fetch_entity_state(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str) -> Dict[str, Any]:
        return {"status": "unknown", "budget": None, "currency": None, "name": entity_id}

    def pause_entity(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str, idempotency_key: str) -> AdsApplyResult:
        if os.getenv("ADS_ACTIONS_DRY_RUN", "1") != "0":
            return AdsApplyResult(ok=True, message="dry_run: meta pause accepted", meta={"idempotency_key": idempotency_key})
        raise RuntimeError("Meta mutate endpoint not enabled in this environment")

    def enable_entity(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str, idempotency_key: str) -> AdsApplyResult:
        if os.getenv("ADS_ACTIONS_DRY_RUN", "1") != "0":
            return AdsApplyResult(ok=True, message="dry_run: meta enable accepted", meta={"idempotency_key": idempotency_key})
        raise RuntimeError("Meta mutate endpoint not enabled in this environment")

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
            return AdsApplyResult(ok=True, message="dry_run: meta budget accepted", meta={"daily_budget": daily_budget, "currency": currency, "idempotency_key": idempotency_key})
        raise RuntimeError("Meta budget mutate endpoint not enabled in this environment")

    def normalize_error(self, exc: Exception) -> AdsProviderError:
        msg = str(exc)
        lower = msg.lower()
        if "invalid oauth" in lower or "invalid_grant" in lower:
            return AdsProviderError(code="invalid_token", message="Meta authorization expired", needs_reauth=True)
        return AdsProviderError(code="provider_error", message=msg or "Meta provider error")

    def supports(self, action_type: str, entity_type: str) -> bool:
        if action_type in {"pause", "enable"} and entity_type in {"campaign", "adset"}:
            return True
        if action_type == "update_budget" and entity_type == "adset":
            return True
        return False
