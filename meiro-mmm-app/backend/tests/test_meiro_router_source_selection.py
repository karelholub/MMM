from app.modules.meiro_integration.router import (
    _build_event_replay_reconstruction_diagnostics,
    _build_raw_event_stream_diagnostics,
    _prefer_event_archive,
)


def test_prefer_event_archive_respects_primary_source_before_freshness():
    profile_archive = {"available": True, "last_received_at": "2026-03-27T12:00:00Z"}
    event_archive = {"available": True, "last_received_at": "2026-03-27T11:00:00Z"}

    assert _prefer_event_archive("auto", profile_archive, event_archive, primary_source="events") is True
    assert _prefer_event_archive("auto", profile_archive, event_archive, primary_source="profiles") is False


def test_prefer_event_archive_falls_back_to_freshest_when_primary_unavailable():
    profile_archive = {"available": False, "last_received_at": None}
    event_archive = {"available": True, "last_received_at": "2026-03-27T11:00:00Z"}

    assert _prefer_event_archive("auto", profile_archive, event_archive, primary_source="profiles") is True


def test_build_raw_event_stream_diagnostics_flags_referrer_only_and_missing_linkage():
    diagnostics = _build_raw_event_stream_diagnostics(
        event_archive_entries=[
            {
                "events": [
                    {
                        "event_payload": {
                            "event_name": "page_view",
                            "customer_id": "cust-1",
                            "page_referrer": "https://www.google.com/",
                        }
                    },
                    {
                        "event_payload": {
                            "event_name": "purchase",
                            "customer_id": "cust-1",
                        }
                    },
                ]
            }
        ],
        webhook_events=[
            {
                "ingest_kind": "events",
                "received_count": 2,
                "reconstructed_profiles": 1,
            }
        ],
    )

    assert diagnostics["available"] is True
    assert diagnostics["events_examined"] == 2
    assert diagnostics["referrer_only_share"] == 0.5
    assert diagnostics["conversion_like_events"] == 1
    assert diagnostics["conversion_linkage_share"] == 0.0
    assert diagnostics["avg_reconstructed_profiles_per_event"] == 0.5
    assert any("referrer fallback" in item for item in diagnostics["warnings"])


def test_build_event_replay_reconstruction_diagnostics_explains_attrition():
    diagnostics = _build_event_replay_reconstruction_diagnostics(
        archive_entries=[
            {
                "events": [
                    {"event_payload": {"event_name": "page_view"}},
                    {"event_payload": {"event_name": "form_submit"}},
                    {"event_payload": {"event_name": "page_view"}},
                ]
            }
        ],
        archived_profiles=[
            {"customer_id": "cust-1", "touchpoints": [{"id": "tp-1"}], "conversions": [{"id": "cv-1"}]},
            {"customer_id": "cust-2", "touchpoints": [{"id": "tp-2"}], "conversions": []},
        ],
        import_result={
            "count": 0,
            "quarantine_count": 1,
            "import_summary": {
                "valid": 1,
                "quarantined": 1,
                "invalid": 1,
                "converted": 0,
                "cleaning_report": {
                    "top_unresolved_patterns": [
                        {"code": "missing_conversions", "count": 1},
                    ]
                },
            },
        },
    )

    assert diagnostics["events_loaded"] == 3
    assert diagnostics["profiles_reconstructed"] == 2
    assert diagnostics["touchpoints_reconstructed"] == 2
    assert diagnostics["conversions_reconstructed"] == 1
    assert diagnostics["attributable_profiles"] == 1
    assert diagnostics["journeys_quarantined"] == 1
    assert diagnostics["journeys_persisted"] == 0
    assert any("quarantined" in item for item in diagnostics["warnings"])
    assert any("Top unresolved replay issues" in item for item in diagnostics["warnings"])
