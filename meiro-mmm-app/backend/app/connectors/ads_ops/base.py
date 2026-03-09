from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Protocol


@dataclass
class AdsApplyResult:
    ok: bool
    provider_request_id: str | None = None
    message: str | None = None
    meta: Dict[str, Any] | None = None


@dataclass
class AdsProviderError:
    code: str
    message: str
    retryable: bool = False
    needs_reauth: bool = False


class AdsProviderAdapter(Protocol):
    key: str

    def build_deep_link(self, *, account_id: str, entity_type: str, entity_id: str) -> str:
        ...

    def fetch_entity_state(
        self,
        *,
        access_token: str,
        account_id: str,
        entity_type: str,
        entity_id: str,
    ) -> Dict[str, Any]:
        ...

    def pause_entity(
        self,
        *,
        access_token: str,
        account_id: str,
        entity_type: str,
        entity_id: str,
        idempotency_key: str,
    ) -> AdsApplyResult:
        ...

    def enable_entity(
        self,
        *,
        access_token: str,
        account_id: str,
        entity_type: str,
        entity_id: str,
        idempotency_key: str,
    ) -> AdsApplyResult:
        ...

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
        ...

    def normalize_error(self, exc: Exception) -> AdsProviderError:
        ...

    def supports(self, action_type: str, entity_type: str) -> bool:
        ...
