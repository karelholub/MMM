from types import SimpleNamespace

from app.services_journey_readiness import build_journey_readiness


def test_journey_readiness_surfaces_latest_event_replay_gap():
    journeys = []
    kpi_config = SimpleNamespace(
        primary_kpi_id="purchase",
        definitions=[
            SimpleNamespace(
                id="purchase",
                label="Purchase",
                event_name="purchase",
                type="conversion",
                value_field="value",
                weight=1.0,
            )
        ],
    )

    def _get_runs(**_kwargs):
        return [
            {
                "id": "run_evt_1",
                "status": "success",
                "source": "meiro_events_replay",
                "started_at": "2026-03-27T10:00:00Z",
                "validation_summary": {
                    "event_reconstruction_diagnostics": {
                        "events_loaded": 120,
                        "profiles_reconstructed": 18,
                        "touchpoints_reconstructed": 18,
                        "conversions_reconstructed": 0,
                        "attributable_profiles": 0,
                        "journeys_persisted": 0,
                        "warnings": [
                            "Reconstructed profiles contain no conversions, so attribution models have nothing to score.",
                        ],
                    }
                },
            }
        ]

    readiness = build_journey_readiness(
        journeys=journeys,
        kpi_config=kpi_config,
        get_import_runs_fn=_get_runs,
        active_settings=SimpleNamespace(validation_json={}, version_label="v1"),
        active_settings_preview={"warnings": []},
    )

    assert readiness["summary"]["latest_event_replay"]["run_id"] == "run_evt_1"
    assert any("reconstructed no conversions" in warning.lower() for warning in readiness["warnings"])
