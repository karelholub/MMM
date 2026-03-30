"""Centralized in-process cache for resolved journey payloads.

This keeps the old "warm dataset" behavior, but moves it behind a lock and
explicit invalidation API instead of exposing a mutable module-level list.
"""

from __future__ import annotations

import threading
from typing import Any, Callable, Dict, List


JourneyLoader = Callable[..., List[Dict[str, Any]]]

_CACHE_LOCK = threading.RLock()
_CACHE: Dict[str, Any] = {
    "journeys": [],
    "limit": 0,
}


def load_cached_journeys(
    db: Any,
    *,
    loader_fn: JourneyLoader,
    limit: int = 50000,
) -> List[Dict[str, Any]]:
    """Load journeys through a lock-aware process cache.

    The cache is keyed by the largest requested limit. Smaller reads reuse the
    already-loaded dataset and return a shallow list copy so callers cannot
    append/pop against shared state.
    """
    normalized_limit = max(1, int(limit or 1))
    with _CACHE_LOCK:
        journeys = _CACHE.get("journeys") or []
        cached_limit = int(_CACHE.get("limit") or 0)
        if journeys and cached_limit >= normalized_limit:
            return list(journeys[:normalized_limit])

        loaded = loader_fn(db, limit=normalized_limit)
        _CACHE["journeys"] = list(loaded or [])
        _CACHE["limit"] = normalized_limit
        return list(_CACHE["journeys"])


def invalidate_journey_cache() -> None:
    with _CACHE_LOCK:
        _CACHE["journeys"] = []
        _CACHE["limit"] = 0


def get_journey_cache_status() -> Dict[str, Any]:
    with _CACHE_LOCK:
        journeys = _CACHE.get("journeys") or []
        cached_limit = int(_CACHE.get("limit") or 0)
        return {
            "cached_count": len(journeys),
            "cached_limit": cached_limit,
            "initialized": bool(journeys),
        }
