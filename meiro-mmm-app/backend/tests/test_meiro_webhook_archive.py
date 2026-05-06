from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from app.db import SessionLocal
from app.main import app
from app.modules.meiro_integration import router as meiro_router
from app.models_config_dq import ConversionPath, MeiroEventFact, MeiroEventProfileState, MeiroProfileFact, MeiroRawBatch, MeiroReplayRun
from app.services_conversions import persist_journeys_as_conversion_paths
from app.utils import meiro_config


def _clear_meiro_raw_batches() -> None:
    db = SessionLocal()
    try:
        db.query(MeiroRawBatch).delete()
        db.query(MeiroEventFact).delete()
        db.query(MeiroEventProfileState).delete()
        db.query(MeiroProfileFact).delete()
        db.commit()
    finally:
        db.close()


def _latest_meiro_raw_batch(source_kind: str) -> MeiroRawBatch | None:
    db = SessionLocal()
    try:
        return (
            db.query(MeiroRawBatch)
            .filter(MeiroRawBatch.source_kind == source_kind)
            .order_by(MeiroRawBatch.id.desc())
            .first()
        )
    finally:
        db.close()


def _clear_meiro_replay_runs() -> None:
    db = SessionLocal()
    try:
        db.query(MeiroReplayRun).delete()
        db.commit()
    finally:
        db.close()


def _clear_conversion_paths() -> None:
    db = SessionLocal()
    try:
        db.query(ConversionPath).delete()
        db.commit()
    finally:
        db.close()


def _clear_meiro_event_profile_state() -> None:
    db = SessionLocal()
    try:
        db.query(MeiroEventProfileState).delete()
        db.commit()
    finally:
        db.close()


def _clear_meiro_event_facts() -> None:
    db = SessionLocal()
    try:
        db.query(MeiroEventFact).delete()
        db.commit()
    finally:
        db.close()


def _clear_meiro_profile_facts() -> None:
    db = SessionLocal()
    try:
        db.query(MeiroProfileFact).delete()
        db.commit()
    finally:
        db.close()


def test_webhook_archive_rebuilds_profiles_in_replace_append_order(monkeypatch, tmp_path):
    archive_path = tmp_path / "meiro_webhook_archive.jsonl"
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", archive_path)
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)

    meiro_config.append_webhook_archive_entry(
        {
            "received_at": "2026-03-20T10:00:00Z",
            "parser_version": "v1",
            "replace": True,
            "profiles": [{"customer_id": "a"}],
        }
    )
    meiro_config.append_webhook_archive_entry(
        {
            "received_at": "2026-03-20T10:05:00Z",
            "parser_version": "v1",
            "replace": False,
            "profiles": [{"customer_id": "b"}],
        }
    )
    meiro_config.append_webhook_archive_entry(
        {
            "received_at": "2026-03-20T10:10:00Z",
            "parser_version": "v2",
            "replace": True,
            "profiles": [{"customer_id": "c"}],
        }
    )

    rebuilt = meiro_config.rebuild_profiles_from_webhook_archive()
    assert [item["customer_id"] for item in rebuilt] == ["c"]

    latest_two = meiro_config.rebuild_profiles_from_webhook_archive(limit=2)
    assert [item["customer_id"] for item in latest_two] == ["c"]

    status = meiro_config.get_webhook_archive_status()
    assert status["available"] is True
    assert status["entries"] == 3
    assert status["last_received_at"] == "2026-03-20T10:10:00Z"
    assert status["parser_versions"] == ["v1", "v2"]

    items = meiro_config.get_webhook_archive_entries(limit=2)
    assert len(items) == 2
    assert items[0]["received_at"] == "2026-03-20T10:10:00Z"
    assert items[1]["received_at"] == "2026-03-20T10:05:00Z"


def test_event_archive_status_counts_events(monkeypatch, tmp_path):
    event_archive_path = tmp_path / "meiro_event_archive.jsonl"
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", event_archive_path)
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)

    meiro_config.append_event_archive_entry(
        {
            "received_at": "2026-03-26T10:00:00Z",
            "parser_version": "v3",
            "replace": False,
            "events": [{"event_id": "evt-1"}, {"event_id": "evt-2"}],
            "received_count": 2,
        }
    )
    meiro_config.append_event_archive_entry(
        {
            "received_at": "2026-03-26T10:05:00Z",
            "parser_version": "v3",
            "replace": False,
            "events": [{"event_id": "evt-3"}],
            "received_count": 1,
        }
    )

    status = meiro_config.get_event_archive_status()
    assert status["available"] is True
    assert status["entries"] == 2
    assert status["events_received"] == 3
    assert status["last_received_at"] == "2026-03-26T10:05:00Z"
    assert status["parser_versions"] == ["v3"]

    items = meiro_config.get_event_archive_entries(limit=2)
    assert len(items) == 2
    assert items[0]["received_at"] == "2026-03-26T10:05:00Z"


def test_event_archive_status_uses_persisted_cache(monkeypatch, tmp_path):
    event_archive_path = tmp_path / "meiro_event_archive.jsonl"
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", event_archive_path)
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    meiro_config._ARCHIVE_STATUS_CACHE.clear()

    meiro_config.append_event_archive_entry(
        {
            "received_at": "2026-03-26T10:00:00Z",
            "parser_version": "v3",
            "replace": False,
            "events": [{"event_id": "evt-1"}],
            "received_count": 1,
        }
    )

    first = meiro_config.get_event_archive_status()
    assert first["events_received"] == 1

    meiro_config._ARCHIVE_STATUS_CACHE.clear()
    original_open = type(meiro_config.EVENT_ARCHIVE_PATH).open

    def fail_if_archive_scanned(self, *args, **kwargs):
        mode = args[0] if args else kwargs.get("mode")
        if self == meiro_config.EVENT_ARCHIVE_PATH and (mode is None or mode.startswith("r")):
            raise AssertionError("archive file should not be rescanned when persisted status cache matches")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(type(meiro_config.EVENT_ARCHIVE_PATH), "open", fail_if_archive_scanned)
    second = meiro_config.get_event_archive_status()
    assert second["events_received"] == 1
    assert second["entries"] == 1


def test_event_contract_readiness_reports_target_site_coverage(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    meiro_config.append_event_archive_entry(
        {
            "received_at": "2026-05-04T10:00:00Z",
            "parser_version": "v1",
            "replace": False,
            "events": [
                {
                    "event_id": "evt-meiro-click",
                    "customer_id": "profile-1",
                    "timestamp": "2026-05-04T09:59:00Z",
                    "event_name": "campaign_click",
                    "site": "meiro.io",
                    "source": "google",
                    "medium": "cpc",
                    "campaign": "brand",
                    "segments": [{"id": "high_intent", "name": "High intent"}],
                    "activationMeasurement": {
                        "activation_campaign_id": "brand",
                        "creative_asset_id": "hero",
                    },
                },
                {
                    "event_id": "evt-store-purchase",
                    "customer_id": "profile-1",
                    "timestamp": "2026-05-04T10:02:00Z",
                    "event_name": "purchase",
                    "page_url": "https://meir.store/checkout/success",
                    "order_id": "order-1",
                    "value": 99,
                    "currency": "EUR",
                    "utm_source": "google",
                    "utm_medium": "cpc",
                    "utm_campaign": "brand",
                },
            ],
            "received_count": 2,
        }
    )

    client = TestClient(app)
    response = client.get("/api/connectors/meiro/events/contract-readiness")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ready"
    assert payload["target_events"] == 2
    assert payload["site_counts"]["meiro.io"] == 1
    assert payload["site_counts"]["meir.store"] == 1
    assert payload["coverage"]["identity"] == 1.0
    assert payload["coverage"]["attribution"] == 1.0
    assert payload["coverage"]["conversion_linkage"] == 0.5
    assert payload["coverage"]["activation_metadata"] == 0.5


def test_event_contract_readiness_blocks_when_target_sites_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    meiro_config.append_event_archive_entry(
        {
            "received_at": "2026-05-04T10:00:00Z",
            "parser_version": "v1",
            "replace": False,
            "events": [{"event_id": "evt-other", "event_name": "page_view", "site": "example.com"}],
            "received_count": 1,
        }
    )

    client = TestClient(app)
    response = client.get("/api/connectors/meiro/events/contract-readiness")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "blocked"
    assert payload["target_events"] == 0
    assert payload["blockers"]


def test_event_contract_sample_sender_makes_readiness_ready(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")
    _clear_meiro_raw_batches()
    _clear_meiro_event_profile_state()
    _clear_meiro_event_facts()

    client = TestClient(app)
    response = client.post("/api/connectors/meiro/events/contract-sample")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["received"] == 2
    assert payload["readiness"]["status"] == "ready"
    assert payload["readiness"]["site_counts"]["meiro.io"] == 1
    assert payload["readiness"]["site_counts"]["meir.store"] == 1
    assert payload["readiness"]["coverage"]["identity"] == 1.0
    assert payload["readiness"]["coverage"]["activation_metadata"] == 1.0

    status = client.get("/api/connectors/meiro/events/archive-status")
    assert status.status_code == 200
    assert status.json()["events_received"] == 2


def test_event_contract_sample_can_replay_latest_raw_batch_to_attribution(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")
    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    _clear_conversion_paths()
    _clear_meiro_event_profile_state()
    _clear_meiro_event_facts()

    client = TestClient(app)
    sample = client.post("/api/connectors/meiro/events/contract-sample")
    assert sample.status_code == 200

    replay = client.post(
        "/api/connectors/meiro/webhook/reprocess",
        json={
            "archive_source": "events",
            "replay_mode": "last_n",
            "archive_limit": 1,
            "persist_to_attribution": True,
            "import_note": "Test contract sample replay",
        },
    )

    assert replay.status_code == 200
    payload = replay.json()
    assert payload["archive_source"] == "events"
    assert payload["persisted_to_attribution"] is True
    assert payload["archive_entries_used"] == 1
    assert payload["event_reconstruction_diagnostics"]["touchpoints_reconstructed"] >= 1
    assert payload["event_reconstruction_diagnostics"]["conversions_reconstructed"] >= 1
    assert payload["import_result"]["count"] >= 1


def test_webhook_suggestions_include_raw_event_ingests(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    client = TestClient(app)

    response = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-page",
                    "event_payload": {
                        "event_id": "evt-page",
                        "customer_id": "cust-1",
                        "timestamp": "2026-03-27T10:00:00Z",
                        "event_name": "page_view",
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "brand",
                        "click_id": "gclid-1",
                    },
                },
                {
                    "event_id": "evt-conv",
                    "event_payload": {
                        "event_id": "evt-conv",
                        "customer_id": "cust-1",
                        "timestamp": "2026-03-27T10:05:00Z",
                        "event_name": "purchase",
                        "conversion_id": "conv-1",
                        "value": 120.0,
                        "currency": "EUR",
                    },
                },
            ],
            "replace": False,
        },
    )
    assert response.status_code == 200

    suggestions = client.get("/api/connectors/meiro/webhook/suggestions")
    assert suggestions.status_code == 200
    payload = suggestions.json()
    assert payload["events_analyzed"] >= 1
    assert payload["total_touchpoints_observed"] >= 1
    assert payload["total_conversions_observed"] >= 1
    assert any(item["event_name"] == "purchase" for item in payload["conversion_event_suggestions"])


def test_meiro_dry_run_prefers_event_archive_replay_source(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")
    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    _clear_conversion_paths()
    meiro_config.save_pull_config(
        {
            "primary_ingest_source": "events",
            "replay_archive_source": "events",
            "replay_mode": "last_n",
            "replay_archive_limit": 5000,
        }
    )

    client = TestClient(app)
    response = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_payload": {
                        "event_id": "evt-page",
                        "customer_id": "cust-1",
                        "timestamp": "2026-03-27T10:00:00Z",
                        "event_name": "page_view",
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "brand",
                        "click_id": "gclid-1",
                        "session_id": "sess-1",
                    },
                },
                {
                    "event_payload": {
                        "event_id": "evt-conv",
                        "customer_id": "cust-1",
                        "timestamp": "2026-03-27T10:05:00Z",
                        "event_name": "purchase",
                        "conversion_id": "conv-1",
                        "value": 120.0,
                        "currency": "EUR",
                        "session_id": "sess-1",
                    },
                },
            ],
            "replace": False,
        },
    )
    assert response.status_code == 200

    dry_run = client.post("/api/connectors/meiro/dry-run")
    assert dry_run.status_code == 200
    payload = dry_run.json()
    assert payload["archive_source"] == "events"
    assert payload["archive_entries_used"] >= 1
    assert payload["count"] >= 1

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    _clear_conversion_paths()


def test_profiles_webhook_persists_db_raw_batch(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    client = TestClient(app)

    response = client.post(
        "/api/connectors/meiro/profiles",
        json={
            "profiles": [
                {
                    "customer_id": "cust-profile-1",
                    "touchpoints": [
                        {
                            "timestamp": "2026-03-28T09:00:00Z",
                            "channel": "google_ads",
                        }
                    ],
                    "conversions": [
                        {
                            "timestamp": "2026-03-28T09:10:00Z",
                            "name": "purchase",
                        }
                    ],
                }
            ],
            "replace": False,
        },
    )

    assert response.status_code == 200
    batch = _latest_meiro_raw_batch("profiles")
    assert batch is not None
    assert batch.ingestion_channel == "webhook"
    assert batch.records_count == 1
    assert batch.payload_json["profiles"][0]["customer_id"] == "cust-profile-1"


def test_profiles_webhook_updates_canonical_profile_facts_incrementally(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    _clear_meiro_profile_facts()
    client = TestClient(app)

    first = client.post(
        "/api/connectors/meiro/profiles",
        json={
            "profiles": [
                {
                    "customer_id": "cust-profile-state-1",
                    "touchpoints": [{"timestamp": "2026-03-28T09:00:00Z", "channel": "google_ads"}],
                }
            ],
            "replace": False,
        },
    )
    second = client.post(
        "/api/connectors/meiro/profiles",
        json={
            "profiles": [
                {
                    "customer_id": "cust-profile-state-2",
                    "touchpoints": [{"timestamp": "2026-03-28T09:05:00Z", "channel": "email"}],
                }
            ],
            "replace": False,
        },
    )
    replaced = client.post(
        "/api/connectors/meiro/profiles",
        json={
            "profiles": [
                {
                    "customer_id": "cust-profile-state-3",
                    "touchpoints": [{"timestamp": "2026-03-28T09:10:00Z", "channel": "direct"}],
                }
            ],
            "replace": True,
        },
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert replaced.status_code == 200

    db = SessionLocal()
    try:
        facts = db.query(MeiroProfileFact).order_by(MeiroProfileFact.profile_id.asc()).all()
        assert [fact.profile_id for fact in facts] == ["cust-profile-state-3"]
        assert facts[0].raw_batch_db_id is not None
        assert facts[0].profile_json["customer_id"] == "cust-profile-state-3"
    finally:
        db.close()


def test_events_webhook_persists_db_raw_batch(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    client = TestClient(app)

    response = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-db-1",
                    "event_payload": {
                        "event_id": "evt-db-1",
                        "customer_id": "cust-event-1",
                        "timestamp": "2026-03-28T10:00:00Z",
                        "event_name": "page_view",
                    },
                }
            ]
        },
    )

    assert response.status_code == 200
    batch = _latest_meiro_raw_batch("events")
    assert batch is not None
    assert batch.ingestion_channel == "webhook"
    assert batch.records_count == 1
    assert batch.payload_json["events"][0]["event_id"] == "evt-db-1"


def test_events_webhook_updates_event_profile_state_incrementally(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    _clear_meiro_event_profile_state()
    client = TestClient(app)

    first = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-state-touch",
                    "event_payload": {
                        "event_id": "evt-state-touch",
                        "customer_id": "cust-state-1",
                        "timestamp": "2026-03-28T10:00:00Z",
                        "event_name": "page_view",
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "brand",
                        "segments": [{"id": "vip", "name": "VIP buyers"}],
                    },
                }
            ]
        },
    )
    second = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-state-conv",
                    "event_payload": {
                        "event_id": "evt-state-conv",
                        "customer_id": "cust-state-1",
                        "timestamp": "2026-03-28T10:05:00Z",
                        "event_name": "purchase",
                        "conversion_id": "conv-state-1",
                        "value": 50.0,
                        "currency": "EUR",
                    },
                }
            ]
        },
    )

    assert first.status_code == 200
    assert second.status_code == 200

    db = SessionLocal()
    try:
        state = db.query(MeiroEventProfileState).filter(MeiroEventProfileState.profile_id == "cust-state-1").one()
        assert len(state.profile_json["touchpoints"]) == 1
        assert len(state.profile_json["conversions"]) == 1
        assert state.profile_json["segments"] == [{"id": "vip", "name": "VIP buyers"}]
        assert state.latest_event_batch_db_id is not None
    finally:
        db.close()


def test_events_webhook_updates_canonical_event_facts_incrementally(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    _clear_meiro_event_facts()
    client = TestClient(app)

    first = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-fact-touch",
                    "event_payload": {
                        "event_id": "evt-fact-touch",
                        "customer_id": "cust-fact-1",
                        "timestamp": "2026-03-28T10:00:00Z",
                        "event_name": "page_view",
                        "source": "google",
                    },
                }
            ]
        },
    )
    second = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-fact-conv",
                    "event_payload": {
                        "event_id": "evt-fact-conv",
                        "customer_id": "cust-fact-1",
                        "timestamp": "2026-03-28T10:05:00Z",
                        "event_name": "purchase",
                        "conversion_id": "conv-fact-1",
                        "value": 50.0,
                        "currency": "EUR",
                    },
                }
            ]
        },
    )
    replaced = client.post(
        "/api/connectors/meiro/events",
        json={
            "replace": True,
            "events": [
                {
                    "event_id": "evt-fact-reset",
                    "event_payload": {
                        "event_id": "evt-fact-reset",
                        "customer_id": "cust-fact-2",
                        "timestamp": "2026-03-28T11:00:00Z",
                        "event_name": "page_view",
                        "source": "meta",
                    },
                }
            ]
        },
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert replaced.status_code == 200

    db = SessionLocal()
    try:
        facts = db.query(MeiroEventFact).order_by(MeiroEventFact.event_uid.asc()).all()
        assert len(facts) == 1
        assert facts[0].profile_id == "cust-fact-2"
        assert facts[0].event_uid == "evt-fact-reset"
        assert facts[0].raw_batch_db_id is not None
    finally:
        db.close()


def test_event_archive_endpoints_prefer_db_batches_when_json_archive_is_unavailable(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")
    monkeypatch.setattr(meiro_router, "append_event_archive_entry", lambda entry: None)

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    client = TestClient(app)

    response = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-db-status-1",
                    "event_payload": {
                        "event_id": "evt-db-status-1",
                        "customer_id": "cust-event-status-1",
                        "timestamp": "2026-03-28T10:00:00Z",
                        "event_name": "page_view",
                    },
                }
            ]
        },
    )

    assert response.status_code == 200
    status = client.get("/api/connectors/meiro/events/archive-status")
    assert status.status_code == 200
    payload = status.json()
    assert payload["available"] is True
    assert payload["entries"] == 1
    assert payload["events_received"] == 1
    assert isinstance(payload["latest_batch_db_id"], int)

    archive = client.get("/api/connectors/meiro/events/archive")
    assert archive.status_code == 200
    items = archive.json()["items"]
    assert len(items) == 1
    assert items[0]["db_row_id"] == payload["latest_batch_db_id"]
    assert items[0]["events"][0]["event_id"] == "evt-db-status-1"


def test_event_archive_status_prefers_file_archive_totals_when_both_sources_exist(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()

    meiro_config.append_event_archive_entry(
        {
            "received_at": "2026-03-28T09:00:00Z",
            "parser_version": "v3",
            "replace": False,
            "events": [{"event_id": "evt-file-1"}, {"event_id": "evt-file-2"}],
            "received_count": 2,
        }
    )
    meiro_config.append_event_archive_entry(
        {
            "received_at": "2026-03-28T09:05:00Z",
            "parser_version": "v3",
            "replace": False,
            "events": [{"event_id": "evt-file-3"}],
            "received_count": 1,
        }
    )

    client = TestClient(app)

    response = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-db-status-1",
                    "event_payload": {
                        "event_id": "evt-db-status-1",
                        "customer_id": "cust-event-status-1",
                        "timestamp": "2026-03-28T10:00:00Z",
                        "event_name": "page_view",
                    },
                }
            ]
        },
    )

    assert response.status_code == 200

    status = client.get("/api/connectors/meiro/events/archive-status")
    assert status.status_code == 200
    payload = status.json()
    assert payload["available"] is True
    file_status = meiro_config.get_event_archive_status()
    assert payload["entries"] == file_status["entries"]
    assert payload["events_received"] == file_status["events_received"]
    assert payload["last_received_at"] == file_status["last_received_at"]
    assert isinstance(payload["latest_batch_db_id"], int)

    archive = client.get("/api/connectors/meiro/events/archive?limit=2")
    assert archive.status_code == 200
    items = archive.json()["items"]
    assert len(items) == 2
    expected_items = meiro_config.get_event_archive_entries(limit=2)
    assert [item["received_at"] for item in items] == [item["received_at"] for item in expected_items]
    assert items[0]["events"][0]["event_id"] == expected_items[0]["events"][0]["event_id"]


def test_webhook_reprocess_uses_db_event_batches_when_json_archive_is_unavailable(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")
    monkeypatch.setattr(meiro_router, "append_event_archive_entry", lambda entry: None)

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    client = TestClient(app)

    response = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-db-replay-touch",
                    "event_payload": {
                        "event_id": "evt-db-replay-touch",
                        "customer_id": "cust-event-replay-1",
                        "timestamp": "2026-03-28T10:00:00Z",
                        "event_name": "page_view",
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "brand",
                    },
                },
                {
                    "event_id": "evt-db-replay-conv",
                    "event_payload": {
                        "event_id": "evt-db-replay-conv",
                        "customer_id": "cust-event-replay-1",
                        "timestamp": "2026-03-28T10:05:00Z",
                        "event_name": "purchase",
                        "conversion_id": "conv-db-replay-1",
                        "value": 42.0,
                        "currency": "EUR",
                    },
                },
            ]
        },
    )

    assert response.status_code == 200
    replay = client.post(
        "/api/connectors/meiro/webhook/reprocess",
        json={"archive_source": "events"},
    )
    assert replay.status_code == 200
    payload = replay.json()
    assert payload["archive_source"] == "events"
    assert payload["archive_entries_used"] == 1
    assert payload["reprocessed_profiles"] == 1


def test_webhook_reprocess_uses_full_file_event_archive_for_manual_all_replay(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()

    meiro_config.append_event_archive_entry(
        {
            "received_at": "2026-03-28T09:00:00Z",
            "parser_version": "v3",
            "replace": False,
            "events": [
                {
                    "event_payload": {
                        "event_id": "evt-file-touch-1",
                        "customer_id": "cust-file-1",
                        "timestamp": "2026-03-28T09:00:00Z",
                        "event_name": "page_view",
                        "source": "google",
                        "medium": "cpc",
                    }
                }
            ],
            "received_count": 1,
        }
    )
    meiro_config.append_event_archive_entry(
        {
            "received_at": "2026-03-28T09:05:00Z",
            "parser_version": "v3",
            "replace": False,
            "events": [
                {
                    "event_payload": {
                        "event_id": "evt-file-conv-1",
                        "customer_id": "cust-file-1",
                        "timestamp": "2026-03-28T09:05:00Z",
                        "event_name": "purchase",
                        "conversion_id": "conv-file-1",
                        "value": 12.0,
                        "currency": "EUR",
                    }
                }
            ],
            "received_count": 1,
        }
    )

    client = TestClient(app)

    response = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-db-status-1",
                    "event_payload": {
                        "event_id": "evt-db-status-1",
                        "customer_id": "cust-db-only-1",
                        "timestamp": "2026-03-28T10:00:00Z",
                        "event_name": "page_view",
                    },
                }
            ]
        },
    )
    assert response.status_code == 200

    expected_status = meiro_config.get_event_archive_status()

    replay = client.post(
        "/api/connectors/meiro/webhook/reprocess",
        json={
            "replay_mode": "all",
            "archive_source": "events",
            "persist_to_attribution": False,
        },
    )

    assert replay.status_code == 200
    payload = replay.json()
    assert payload["archive_source"] == "events"
    assert payload["replay_mode"] == "all"
    assert payload["archive_entries_used"] == expected_status["entries"]
    assert payload["event_reconstruction_diagnostics"]["events_loaded"] == expected_status["events_received"]


def test_webhook_reprocess_persist_to_attribution_uses_replay_snapshot_without_profiles_file(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")
    monkeypatch.setattr(meiro_router, "append_event_archive_entry", lambda entry: None)

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    client = TestClient(app)

    response = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-db-import-touch",
                    "event_payload": {
                        "event_id": "evt-db-import-touch",
                        "customer_id": "cust-event-import-1",
                        "timestamp": "2026-03-28T10:00:00Z",
                        "event_name": "page_view",
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "brand",
                    },
                },
                {
                    "event_id": "evt-db-import-conv",
                    "event_payload": {
                        "event_id": "evt-db-import-conv",
                        "customer_id": "cust-event-import-1",
                        "timestamp": "2026-03-28T10:05:00Z",
                        "event_name": "purchase",
                        "conversion_id": "conv-db-import-1",
                        "value": 52.0,
                        "currency": "EUR",
                    },
                },
            ]
        },
    )

    assert response.status_code == 200
    assert not (tmp_path / "meiro_cdp_profiles.json").exists()

    replay = client.post(
        "/api/connectors/meiro/webhook/reprocess",
        json={"archive_source": "events", "persist_to_attribution": True},
    )
    assert replay.status_code == 200
    payload = replay.json()
    assert payload["persisted_to_attribution"] is True
    assert payload["replay_snapshot_id"]
    assert payload["import_result"]["count"] >= 1
    assert not (tmp_path / "meiro_cdp_profiles.json").exists()


def test_raw_event_ingest_skips_auto_replay_when_mapping_not_approved(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    meiro_config.save_mapping({})
    meiro_config.update_mapping_approval("rejected", "Needs review")
    meiro_config.save_pull_config(
        {
            "primary_ingest_source": "events",
            "auto_replay_mode": "after_batch",
            "auto_replay_require_mapping_approval": True,
        }
    )

    client = TestClient(app)
    response = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-page",
                    "event_payload": {
                        "event_id": "evt-page",
                        "customer_id": "cust-1",
                        "timestamp": "2026-03-27T10:00:00Z",
                        "event_name": "page_view",
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "brand",
                    },
                }
            ]
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["auto_replay"]["status"] == "skipped"
    assert "Mapping approval is required" in payload["auto_replay"]["reason"]


def test_manual_auto_replay_blocks_on_quarantine_spike(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    meiro_config.save_mapping({})
    meiro_config.update_mapping_approval("approved", "Ready")
    meiro_config.save_pull_config(
        {
            "primary_ingest_source": "events",
            "auto_replay_mode": "after_batch",
            "auto_replay_quarantine_spike_threshold_pct": 0,
            "value_fallback_policy": "quarantine",
        }
    )

    client = TestClient(app)
    ingest = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-page",
                    "event_payload": {
                        "event_id": "evt-page",
                        "customer_id": "cust-2",
                        "timestamp": "2026-03-27T10:00:00Z",
                        "event_name": "page_view",
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "brand",
                    },
                },
                {
                    "event_id": "evt-purchase",
                    "event_payload": {
                        "event_id": "evt-purchase",
                        "customer_id": "cust-2",
                        "timestamp": "2026-03-27T10:05:00Z",
                        "event_name": "purchase",
                        "conversion_id": "conv-quarantine-1",
                        "currency": "EUR",
                    },
                }
            ]
        },
    )
    assert ingest.status_code == 200

    run = client.post("/api/connectors/meiro/auto-replay/run", json={"trigger": "manual"})
    assert run.status_code == 200
    payload = run.json()
    assert payload["status"] == "blocked"
    assert "quarantined journeys reached" in payload["reason"]
    assert "configured 0.0% threshold" in payload["reason"]
    assert isinstance(payload["state"]["last_event_batch_db_id_seen"], int)


def test_after_batch_auto_replay_skips_when_no_new_db_batches(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    meiro_config.save_mapping({})
    meiro_config.update_mapping_approval("approved", "Ready")
    meiro_config.save_pull_config(
        {
            "primary_ingest_source": "events",
            "auto_replay_mode": "after_batch",
            "auto_replay_quarantine_spike_threshold_pct": 0,
            "value_fallback_policy": "quarantine",
        }
    )

    client = TestClient(app)
    ingest = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-page-cursor",
                    "event_payload": {
                        "event_id": "evt-page-cursor",
                        "customer_id": "cust-cursor",
                        "timestamp": "2026-03-27T10:00:00Z",
                        "event_name": "page_view",
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "brand",
                    },
                },
                {
                    "event_id": "evt-purchase-cursor",
                    "event_payload": {
                        "event_id": "evt-purchase-cursor",
                        "customer_id": "cust-cursor",
                        "timestamp": "2026-03-27T10:05:00Z",
                        "event_name": "purchase",
                        "conversion_id": "conv-cursor-1",
                        "currency": "EUR",
                    },
                },
            ]
        },
    )
    assert ingest.status_code == 200
    first_auto_replay = ingest.json()["auto_replay"]
    assert first_auto_replay["status"] == "blocked"

    rerun = client.post("/api/connectors/meiro/auto-replay/run", json={"trigger": "after_batch"})
    assert rerun.status_code == 200
    payload = rerun.json()
    assert payload["status"] == "skipped"
    assert "No new raw-event archive batches" in payload["reason"]


def test_after_batch_auto_replay_replaces_only_affected_profiles(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    _clear_conversion_paths()
    meiro_config.save_mapping({})
    meiro_config.update_mapping_approval("approved", "Ready")
    meiro_config.save_pull_config(
        {
            "primary_ingest_source": "events",
            "auto_replay_mode": "after_batch",
        }
    )

    client = TestClient(app)
    first = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-inc-1-touch",
                    "event_payload": {
                        "event_id": "evt-inc-1-touch",
                        "customer_id": "cust-event-1",
                        "timestamp": "2026-03-29T10:00:00Z",
                        "event_name": "page_view",
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "brand",
                    },
                },
                {
                    "event_id": "evt-inc-1-conv",
                    "event_payload": {
                        "event_id": "evt-inc-1-conv",
                        "customer_id": "cust-event-1",
                        "timestamp": "2026-03-29T10:05:00Z",
                        "event_name": "purchase",
                        "conversion_id": "conv-inc-1",
                        "value": 42.0,
                        "currency": "EUR",
                    },
                },
            ]
        },
    )
    assert first.status_code == 200
    assert first.json()["auto_replay"]["status"] == "success"
    assert first.json()["auto_replay"]["import_result"]["count"] == 1

    db = SessionLocal()
    try:
        persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "customer_id": "upload-preserved",
                    "conversion_id": "conv-upload-1",
                    "kpi_type": "purchase",
                    "touchpoints": [{"channel": "email", "timestamp": "2026-03-29T09:00:00Z"}],
                    "converted": True,
                    "conversion_value": 12.0,
                }
            ],
            replace=False,
            import_source="upload",
        )
    finally:
        db.close()

    second = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-inc-2-touch",
                    "event_payload": {
                        "event_id": "evt-inc-2-touch",
                        "customer_id": "cust-event-2",
                        "timestamp": "2026-03-29T11:00:00Z",
                        "event_name": "page_view",
                        "source": "meta",
                        "medium": "paid_social",
                        "campaign": "launch",
                    },
                },
                {
                    "event_id": "evt-inc-2-conv",
                    "event_payload": {
                        "event_id": "evt-inc-2-conv",
                        "customer_id": "cust-event-2",
                        "timestamp": "2026-03-29T11:05:00Z",
                        "event_name": "purchase",
                        "conversion_id": "conv-inc-2",
                        "value": 84.0,
                        "currency": "EUR",
                    },
                },
            ]
        },
    )
    assert second.status_code == 200
    payload = second.json()["auto_replay"]
    assert payload["status"] == "success"
    assert payload["import_result"]["count"] == 1
    assert payload["incremental"] is True
    assert payload["replace_profile_count"] == 1

    db = SessionLocal()
    try:
        rows = db.query(ConversionPath).order_by(ConversionPath.profile_id.asc(), ConversionPath.conversion_id.asc()).all()
        assert [row.profile_id for row in rows] == [
            "cust-event-1",
            "cust-event-2",
            "upload-preserved",
        ]
        assert any(row.profile_id == "upload-preserved" and row.import_source == "upload" for row in rows)
        assert any(row.profile_id == "cust-event-2" and row.import_source == "meiro_events_replay" for row in rows)
    finally:
        db.close()


def test_raw_event_ingest_marks_auto_replay_unavailable_on_sqlite_io_error(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    meiro_config.save_mapping({})
    meiro_config.update_mapping_approval("approved", "Ready")
    meiro_config.save_pull_config(
        {
            "primary_ingest_source": "events",
            "auto_replay_mode": "after_batch",
        }
    )

    def fail_snapshot(*args, **kwargs):
        raise OperationalError("INSERT INTO meiro_replay_snapshots ...", {}, Exception("disk I/O error"))

    monkeypatch.setattr(meiro_router, "create_meiro_replay_snapshot", fail_snapshot)

    client = TestClient(app)
    response = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-io-touch",
                    "event_payload": {
                        "event_id": "evt-io-touch",
                        "customer_id": "cust-io",
                        "timestamp": "2026-03-29T10:00:00Z",
                        "event_name": "page_view",
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "brand",
                    },
                },
                {
                    "event_id": "evt-io-conv",
                    "event_payload": {
                        "event_id": "evt-io-conv",
                        "customer_id": "cust-io",
                        "timestamp": "2026-03-29T10:05:00Z",
                        "event_name": "purchase",
                        "conversion_id": "conv-io-1",
                        "value": 42.0,
                        "currency": "EUR",
                    },
                },
            ]
        },
    )
    assert response.status_code == 200
    payload = response.json()["auto_replay"]
    assert payload["status"] == "unavailable"
    assert payload["retryable"] is True
    assert "saved and replay can be retried" in payload["reason"]
    assert payload["state"]["last_status"] == "unavailable"
    assert payload["state"].get("last_event_batch_db_id_seen") in (None, "")

    status = client.get("/api/connectors/meiro/auto-replay")
    assert status.status_code == 200
    status_payload = status.json()
    assert status_payload["state"]["last_status"] == "unavailable"
    assert status_payload["history"]
    assert status_payload["history"][0]["status"] == "unavailable"


def test_events_webhook_degrades_when_event_facts_are_unavailable(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    _clear_meiro_event_profile_state()

    def fail_event_facts(*args, **kwargs):
        raise meiro_router.MeiroEventFactsUnavailableError("database disk image is malformed")

    monkeypatch.setattr(meiro_router, "upsert_meiro_event_facts", fail_event_facts)

    client = TestClient(app)
    response = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-degrade-touch",
                    "event_payload": {
                        "event_id": "evt-degrade-touch",
                        "customer_id": "cust-degrade-1",
                        "timestamp": "2026-03-29T10:00:00Z",
                        "event_name": "page_view",
                        "source": "google",
                        "medium": "cpc",
                    },
                },
                {
                    "event_id": "evt-degrade-conv",
                    "event_payload": {
                        "event_id": "evt-degrade-conv",
                        "customer_id": "cust-degrade-1",
                        "timestamp": "2026-03-29T10:05:00Z",
                        "event_name": "purchase",
                        "conversion_id": "conv-degrade-1",
                        "value": 15.0,
                        "currency": "EUR",
                    },
                },
            ]
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["event_facts"]["ok"] is False
    assert payload["event_facts"]["stored"] is False
    assert "event-facts storage is unavailable" in payload["event_facts"]["warning"].lower()

    batch = _latest_meiro_raw_batch("events")
    assert batch is not None
    assert batch.records_count == 2

    db = SessionLocal()
    try:
        state = db.query(MeiroEventProfileState).filter(MeiroEventProfileState.profile_id == "cust-degrade-1").one()
        assert len(state.profile_json["touchpoints"]) == 1
        assert len(state.profile_json["conversions"]) == 1
        assert db.query(MeiroEventFact).count() == 0
    finally:
        db.close()


def test_events_webhook_degrades_when_event_profile_state_is_unavailable(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    _clear_meiro_event_profile_state()
    _clear_meiro_event_facts()

    def fail_event_profile_state(*args, **kwargs):
        raise meiro_router.MeiroEventProfileStateUnavailableError("database disk image is malformed")

    monkeypatch.setattr(meiro_router, "upsert_meiro_event_profile_state", fail_event_profile_state)

    client = TestClient(app)
    response = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-profile-state-touch",
                    "event_payload": {
                        "event_id": "evt-profile-state-touch",
                        "customer_id": "cust-profile-state-1",
                        "timestamp": "2026-03-29T10:00:00Z",
                        "event_name": "page_view",
                        "source": "google",
                        "medium": "cpc",
                    },
                }
            ]
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["event_profile_state"]["ok"] is False
    assert payload["event_profile_state"]["stored"] is False
    assert "profile-state storage is unavailable" in payload["event_profile_state"]["warning"].lower()

    batch = _latest_meiro_raw_batch("events")
    assert batch is not None
    assert batch.records_count == 1

    db = SessionLocal()
    try:
        assert db.query(MeiroEventFact).count() == 1
        assert db.query(MeiroEventProfileState).count() == 0
    finally:
        db.close()


def test_auto_replay_status_endpoint_prefers_db_run_history(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

    _clear_meiro_raw_batches()
    _clear_meiro_replay_runs()
    meiro_config.save_mapping({})
    meiro_config.update_mapping_approval("rejected", "Needs review")
    meiro_config.save_pull_config(
        {
            "primary_ingest_source": "events",
            "auto_replay_mode": "after_batch",
            "auto_replay_require_mapping_approval": True,
        }
    )

    client = TestClient(app)
    response = client.post(
        "/api/connectors/meiro/events",
        json={
            "events": [
                {
                    "event_id": "evt-history",
                    "event_payload": {
                        "event_id": "evt-history",
                        "customer_id": "cust-history",
                        "timestamp": "2026-03-27T10:00:00Z",
                        "event_name": "page_view",
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "brand",
                    },
                }
            ]
        },
    )
    assert response.status_code == 200

    status = client.get("/api/connectors/meiro/auto-replay")
    assert status.status_code == 200
    payload = status.json()
    assert payload["history"]
    assert payload["history"][0]["status"] == "skipped"
    assert payload["history"][0]["scope"] == "auto"
    assert payload["history"][0]["run_id"]
