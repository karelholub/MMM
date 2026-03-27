from fastapi.testclient import TestClient

from app.main import app
from app.utils import meiro_config


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


def test_raw_event_ingest_skips_auto_replay_when_mapping_not_approved(monkeypatch, tmp_path):
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", tmp_path / "meiro_config.json")
    monkeypatch.setattr(meiro_config, "WEBHOOK_ARCHIVE_PATH", tmp_path / "meiro_webhook_archive.jsonl")
    monkeypatch.setattr(meiro_config, "EVENT_ARCHIVE_PATH", tmp_path / "meiro_event_archive.jsonl")

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

    meiro_config.save_mapping({})
    meiro_config.update_mapping_approval("approved", "Ready")
    meiro_config.save_pull_config(
        {
            "primary_ingest_source": "events",
            "auto_replay_mode": "after_batch",
            "auto_replay_quarantine_spike_threshold_pct": 0,
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
