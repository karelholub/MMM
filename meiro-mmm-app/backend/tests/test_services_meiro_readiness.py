from app.services_meiro_readiness import build_meiro_readiness


def test_meiro_readiness_warns_on_dual_ingest_with_auto_replay():
    readiness = build_meiro_readiness(
        meiro_connected=False,
        meiro_config={
            "webhook_received_count": 12,
            "event_webhook_received_count": 34,
            "webhook_has_secret": True,
        },
        mapping_state={"approval": {"status": "approved"}, "version": 3},
        archive_status={"entries": 2, "last_received_at": "2026-03-27T10:00:00Z"},
        event_archive_status={"entries": 5, "last_received_at": "2026-03-27T11:00:00Z"},
        pull_config={
            "conversion_selector": "purchase",
            "primary_ingest_source": "events",
            "replay_archive_source": "auto",
        },
    )

    assert readiness["status"] == "warning"
    assert readiness["summary"]["dual_ingest_detected"] is True
    assert readiness["summary"]["primary_ingest_source"] == "events"
    assert readiness["summary"]["event_archive_entries"] == 5
    assert any("Both Meiro profile and raw-event webhooks are active" in warning for warning in readiness["warnings"])
    assert any(action["id"] == "pin_meiro_replay_source" for action in readiness["recommended_actions"])


def test_meiro_readiness_warns_when_primary_events_have_no_event_traffic():
    readiness = build_meiro_readiness(
        meiro_connected=False,
        meiro_config={
            "webhook_received_count": 20,
            "event_webhook_received_count": 0,
            "webhook_has_secret": True,
        },
        mapping_state={"approval": {"status": "approved"}, "version": 2},
        archive_status={"entries": 4, "last_received_at": "2026-03-27T10:00:00Z"},
        event_archive_status={"entries": 0, "last_received_at": None},
        pull_config={
            "conversion_selector": "purchase",
            "primary_ingest_source": "events",
            "replay_archive_source": "events",
        },
    )

    assert any("Primary Meiro source is set to raw events" in warning for warning in readiness["warnings"])
