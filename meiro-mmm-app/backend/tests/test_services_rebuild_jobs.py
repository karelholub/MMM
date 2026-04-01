from pathlib import Path
import sys
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services_rebuild_jobs import (
    purge_journey_definition_outputs,
    rebuild_journey_definition_outputs,
    rebuild_journey_aggregate_outputs,
    rebuild_taxonomy_dq_outputs,
)


def test_rebuild_taxonomy_dq_outputs_prefers_db_facts(monkeypatch):
    db = object()
    backfill_calls = []
    current_calls = []

    monkeypatch.setattr(
        "app.services_rebuild_jobs.backfill_taxonomy_dq_snapshots_from_db",
        lambda db, taxonomy=None: backfill_calls.append((db, taxonomy)) or {"buckets_processed": 2},
    )
    monkeypatch.setattr(
        "app.services_rebuild_jobs.persist_taxonomy_dq_snapshots_from_db",
        lambda db, taxonomy=None: current_calls.append((db, taxonomy)) or ["snap-1", "snap-2"],
    )

    out = rebuild_taxonomy_dq_outputs(db, taxonomy="taxonomy")

    assert backfill_calls == [(db, "taxonomy")]
    assert current_calls == [(db, "taxonomy")]
    assert out["source"] == "db_touchpoint_facts"
    assert out["snapshots"] == ["snap-1", "snap-2"]


def test_rebuild_taxonomy_dq_outputs_falls_back_to_journeys(monkeypatch):
    monkeypatch.setattr(
        "app.services_rebuild_jobs.backfill_taxonomy_dq_snapshots_from_db",
        lambda db, taxonomy=None: {"buckets_processed": 0},
    )
    monkeypatch.setattr(
        "app.services_rebuild_jobs.persist_taxonomy_dq_snapshots_from_db",
        lambda db, taxonomy=None: None,
    )
    monkeypatch.setattr(
        "app.services_conversions.load_journeys_from_db",
        lambda db, limit=10000: [{"id": "j1"}],
    )
    monkeypatch.setattr(
        "app.services_rebuild_jobs.persist_taxonomy_dq_snapshots",
        lambda db, journeys, taxonomy=None: ["fallback-snapshot", journeys, taxonomy],
    )

    out = rebuild_taxonomy_dq_outputs(object(), taxonomy="taxonomy")

    assert out["source"] == "journeys_fallback"
    assert out["snapshots"][0] == "fallback-snapshot"
    assert out["snapshots"][1] == [{"id": "j1"}]
    assert out["snapshots"][2] == "taxonomy"


class _FakeScalarQuery:
    def __init__(self, bounds):
        self._bounds = bounds

    def one_or_none(self):
        return self._bounds


class _FakeDb:
    def __init__(self, bounds):
        self._bounds = bounds

    def query(self, *_args, **_kwargs):
        return _FakeScalarQuery(self._bounds)


def test_rebuild_journey_aggregate_outputs_expands_reprocess_window(monkeypatch):
    fake_db = _FakeDb((datetime(2026, 3, 1), datetime(2026, 3, 4)))
    monkeypatch.setattr(
        "app.services_rebuild_jobs.get_active_journey_settings",
        lambda db, use_cache=True: {"settings_json": {"performance_guardrails": {"aggregation_reprocess_window_days": 2}}},
    )
    monkeypatch.setattr(
        "app.services_rebuild_jobs.run_daily_journey_aggregates",
        lambda db, reprocess_days=0: {"definitions": 1, "days_processed": 4, "source_rows_processed": 20, "reprocess_days": reprocess_days},
    )

    out = rebuild_journey_aggregate_outputs(fake_db, reprocess_days=1)

    assert out["effective_reprocess_days"] == 4
    assert out["days_processed"] == 4


def test_rebuild_journey_definition_outputs_uses_guardrail_default(monkeypatch):
    db = object()
    monkeypatch.setattr(
        "app.services_rebuild_jobs.get_active_journey_settings",
        lambda db, use_cache=True: {"settings_json": {"performance_guardrails": {"aggregation_reprocess_window_days": 5}}},
    )
    monkeypatch.setattr(
        "app.services_rebuild_jobs.rebuild_journey_definition_outputs_from_daily",
        lambda db, definition_id=None, reprocess_days=0: {
            "definition_id": definition_id,
            "days_processed": 3,
            "reprocess_days": reprocess_days,
        },
    )

    out = rebuild_journey_definition_outputs(db, definition_id="def-1")

    assert out["definition_id"] == "def-1"
    assert out["days_processed"] == 3
    assert out["effective_reprocess_days"] == 5


def test_purge_journey_definition_outputs_delegates(monkeypatch):
    db = object()
    monkeypatch.setattr(
        "app.services_rebuild_jobs.purge_journey_definition_outputs_from_daily",
        lambda db, definition_id=None: {"definition_id": definition_id, "path_rows_deleted": 2},
    )

    out = purge_journey_definition_outputs(db, definition_id="def-2")

    assert out == {"definition_id": "def-2", "path_rows_deleted": 2}
