from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services_journey_cache import (
    get_journey_cache_status,
    invalidate_journey_cache,
    load_cached_journeys,
)


def test_load_cached_journeys_reuses_cached_result_for_smaller_limits():
    invalidate_journey_cache()
    calls = []
    dataset = [{"id": str(idx)} for idx in range(5)]

    def loader(_db, *, limit: int):
        calls.append(limit)
        return dataset[:limit]

    first = load_cached_journeys(object(), loader_fn=loader, limit=5)
    second = load_cached_journeys(object(), loader_fn=loader, limit=3)

    assert calls == [5]
    assert first == dataset
    assert second == dataset[:3]


def test_load_cached_journeys_reloads_when_requesting_larger_limit():
    invalidate_journey_cache()
    calls = []
    dataset = [{"id": str(idx)} for idx in range(8)]

    def loader(_db, *, limit: int):
        calls.append(limit)
        return dataset[:limit]

    smaller = load_cached_journeys(object(), loader_fn=loader, limit=3)
    larger = load_cached_journeys(object(), loader_fn=loader, limit=8)

    assert calls == [3, 8]
    assert smaller == dataset[:3]
    assert larger == dataset


def test_invalidate_journey_cache_clears_cached_state():
    invalidate_journey_cache()

    def loader(_db, *, limit: int):
        return [{"id": "j1"}][:limit]

    load_cached_journeys(object(), loader_fn=loader, limit=1)
    assert get_journey_cache_status() == {
        "cached_count": 1,
        "cached_limit": 1,
        "initialized": True,
    }

    invalidate_journey_cache()

    assert get_journey_cache_status() == {
        "cached_count": 0,
        "cached_limit": 0,
        "initialized": False,
    }
