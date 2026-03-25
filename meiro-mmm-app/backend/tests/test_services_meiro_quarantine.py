from app import services_meiro_quarantine as quarantine


def test_create_and_fetch_quarantine_run(monkeypatch, tmp_path):
    quarantine_path = tmp_path / "meiro_quarantine_runs.json"
    monkeypatch.setattr(quarantine, "QUARANTINE_RUNS_FILE", quarantine_path)

    created = quarantine.create_quarantine_run(
        source="meiro_webhook",
        summary={"dropped": 2},
        records=[{"journey_id": "j1", "reason_codes": ["duplicate_profile_id"]}],
    )

    runs = quarantine.get_quarantine_runs()
    fetched = quarantine.get_quarantine_run(created["id"])

    assert len(runs) == 1
    assert fetched is not None
    assert fetched["id"] == created["id"]
    assert fetched["summary"]["dropped"] == 2
