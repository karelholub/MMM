from pathlib import Path
import sys
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services_rebuild_jobs import (
    purge_journey_definition_outputs,
    rebuild_multiple_journey_definition_outputs,
    rebuild_outputs_for_kpi_config_change,
    rebuild_outputs_for_taxonomy_change,
    rebuild_journey_definition_outputs,
    rebuild_journey_aggregate_outputs,
    rebuild_taxonomy_dq_outputs,
)
from app.modules.settings.schemas import KpiConfigModel, KpiDefinitionModel


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
        "app.services_rebuild_jobs.resolve_canonical_history_bounds",
        lambda db: (None, None),
    )
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


def test_rebuild_journey_aggregate_outputs_prefers_canonical_history_bounds(monkeypatch):
    db = object()
    monkeypatch.setattr(
        "app.services_rebuild_jobs.resolve_canonical_history_bounds",
        lambda db: (datetime(2026, 2, 1), datetime(2026, 2, 6)),
    )
    monkeypatch.setattr(
        "app.services_rebuild_jobs.get_active_journey_settings",
        lambda db, use_cache=True: {"settings_json": {"performance_guardrails": {"aggregation_reprocess_window_days": 2}}},
    )
    monkeypatch.setattr(
        "app.services_rebuild_jobs.run_daily_journey_aggregates",
        lambda db, reprocess_days=0: {"definitions": 1, "days_processed": reprocess_days, "source_rows_processed": 10},
    )

    out = rebuild_journey_aggregate_outputs(db, reprocess_days=1)

    assert out["effective_reprocess_days"] == 6
    assert out["days_processed"] == 6


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


def test_rebuild_multiple_journey_definition_outputs_summarizes_results(monkeypatch):
    db = object()
    monkeypatch.setattr(
        "app.services_rebuild_jobs.list_active_journey_definitions",
        lambda db: [type("D", (), {"id": "def-1"})(), type("D", (), {"id": "def-2"})()],
    )
    monkeypatch.setattr(
        "app.services_rebuild_jobs.rebuild_journey_definition_outputs_from_daily",
        lambda db, definition_id=None, reprocess_days=0: {
            "definition_id": definition_id,
            "days_processed": 1,
            "source_rows_processed": 2,
            "path_rows_written": 3,
            "transition_rows_written": 4,
            "example_rows_written": 5,
            "definition_rows_written": 6,
            "obsolete_days_removed": 7,
        },
    )

    out = rebuild_multiple_journey_definition_outputs(db, reprocess_days=9)

    assert out["definitions_rebuilt"] == 2
    assert out["definition_ids"] == ["def-1", "def-2"]
    assert out["effective_reprocess_days"] == 9
    assert out["days_processed"] == 2
    assert out["definition_rows_written"] == 12


def test_rebuild_outputs_for_taxonomy_change_runs_taxonomy_and_definition_jobs(monkeypatch):
    db = object()
    monkeypatch.setattr(
        "app.services_rebuild_jobs.rebuild_taxonomy_dq_outputs",
        lambda db, taxonomy=None: {"source": "db_touchpoint_facts", "taxonomy": taxonomy},
    )
    monkeypatch.setattr(
        "app.services_rebuild_jobs.rebuild_journey_aggregate_outputs",
        lambda db, reprocess_days=None: {"days_processed": reprocess_days or 3},
    )
    monkeypatch.setattr(
        "app.services_rebuild_jobs.rebuild_multiple_journey_definition_outputs",
        lambda db, reprocess_days=None: {"definitions_rebuilt": 3, "effective_reprocess_days": reprocess_days or 3},
    )

    out = rebuild_outputs_for_taxonomy_change(db, taxonomy="taxonomy", reprocess_days=5)

    assert out["taxonomy"]["taxonomy"] == "taxonomy"
    assert out["aggregate_outputs"]["days_processed"] == 5
    assert out["journey_outputs"]["definitions_rebuilt"] == 3
    assert out["journey_outputs"]["effective_reprocess_days"] == 5


def test_rebuild_outputs_for_kpi_config_change_targets_impacted_definitions(monkeypatch):
    db = object()
    previous_cfg = KpiConfigModel(
        definitions=[
            KpiDefinitionModel(id="purchase", label="Purchase", type="conversion", event_name="purchase"),
            KpiDefinitionModel(id="lead", label="Lead", type="conversion", event_name="lead_submit"),
        ],
        primary_kpi_id="purchase",
    )
    current_cfg = KpiConfigModel(
        definitions=[
            KpiDefinitionModel(id="purchase", label="Purchase", type="conversion", event_name="purchase_confirmed"),
            KpiDefinitionModel(id="lead", label="Lead", type="conversion", event_name="lead_submit"),
        ],
        primary_kpi_id="purchase",
    )
    monkeypatch.setattr(
        "app.services_rebuild_jobs.list_active_journey_definitions",
        lambda db, conversion_kpi_ids=None: [type("D", (), {"id": "def-1", "conversion_kpi_id": "purchase"})()],
    )
    monkeypatch.setattr(
        "app.services_rebuild_jobs.rebuild_multiple_journey_definition_outputs",
        lambda db, definition_ids=None, reprocess_days=None: {"definition_ids": definition_ids or [], "definitions_rebuilt": len(definition_ids or [])},
    )
    monkeypatch.setattr(
        "app.services_rebuild_jobs.rebuild_journey_aggregate_outputs",
        lambda db, reprocess_days=None: {"days_processed": reprocess_days or 3},
    )

    out = rebuild_outputs_for_kpi_config_change(db, previous_cfg=previous_cfg, current_cfg=current_cfg)

    assert out["impacted_kpi_ids"] == ["purchase"]
    assert out["affected_definition_ids"] == ["def-1"]
    assert out["aggregate_outputs"]["days_processed"] == 3
    assert out["rebuild"]["definitions_rebuilt"] == 1
