from app.db import SessionLocal
from app.models_config_dq import MeiroRawBatch, MeiroReplayRun, MeiroReplaySnapshot
from app.services_meiro_raw_batches import record_meiro_raw_batch
from app.services_meiro_replay_runs import record_meiro_replay_run
from app.services_meiro_replay_snapshots import create_meiro_replay_snapshot


def _clear_meiro_persistence_tables() -> None:
    db = SessionLocal()
    try:
        db.query(MeiroReplaySnapshot).delete()
        db.query(MeiroReplayRun).delete()
        db.query(MeiroRawBatch).delete()
        db.commit()
    finally:
        db.close()


def test_record_meiro_raw_batch_returns_accessible_detached_row() -> None:
    _clear_meiro_persistence_tables()
    db = SessionLocal()
    try:
        row = record_meiro_raw_batch(
            db,
            source_kind="events",
            ingestion_channel="webhook",
            payload_json={"received_at": "2026-03-31T06:00:00Z", "events": [{"event_id": "evt-1"}]},
            received_at="2026-03-31T06:00:00Z",
            parser_version="v-test",
            payload_shape="object",
            replace=False,
            records_count=1,
            metadata_json={"ip": "127.0.0.1"},
        )
    finally:
        db.close()

    assert row.id is not None
    assert row.source_kind == "events"
    assert row.ingestion_channel == "webhook"
    assert row.records_count == 1
    assert row.payload_json["events"][0]["event_id"] == "evt-1"

    verify_db = SessionLocal()
    try:
        stored = verify_db.query(MeiroRawBatch).filter(MeiroRawBatch.id == row.id).first()
        assert stored is not None
        assert stored.batch_id == row.batch_id
    finally:
        verify_db.close()


def test_record_meiro_replay_run_persists_without_refresh() -> None:
    _clear_meiro_persistence_tables()
    db = SessionLocal()
    try:
        payload = record_meiro_replay_run(
            db,
            scope="auto",
            status="success",
            trigger="interval",
            archive_source="events",
            replay_mode="incremental",
            latest_event_batch_db_id=12,
            archive_entries_seen=4,
            archive_entries_used=2,
            profiles_reconstructed=3,
            quarantine_count=1,
            persisted_count=2,
            result_json={"ok": True},
        )
    finally:
        db.close()

    assert payload["status"] == "success"
    assert payload["latest_event_batch_db_id"] == 12
    assert payload["result_json"] == {"ok": True}

    verify_db = SessionLocal()
    try:
        stored = verify_db.query(MeiroReplayRun).filter(MeiroReplayRun.run_id == payload["run_id"]).first()
        assert stored is not None
        assert stored.latest_event_batch_db_id == 12
        assert stored.result_json == {"ok": True}
    finally:
        verify_db.close()


def test_create_meiro_replay_snapshot_persists_without_refresh() -> None:
    _clear_meiro_persistence_tables()
    db = SessionLocal()
    try:
        payload = create_meiro_replay_snapshot(
            db,
            source_kind="events",
            replay_mode="incremental",
            latest_event_batch_db_id=22,
            archive_entries_used=5,
            profiles_json=[{"customer_id": "cust-1"}],
            context_json={"replace_profile_ids": ["cust-1"]},
        )
    finally:
        db.close()

    assert payload["source_kind"] == "events"
    assert payload["profiles_count"] == 1
    assert payload["context_json"]["replace_profile_ids"] == ["cust-1"]

    verify_db = SessionLocal()
    try:
        stored = (
            verify_db.query(MeiroReplaySnapshot)
            .filter(MeiroReplaySnapshot.snapshot_id == payload["snapshot_id"])
            .first()
        )
        assert stored is not None
        assert stored.latest_event_batch_db_id == 22
        assert stored.profiles_count == 1
    finally:
        verify_db.close()
