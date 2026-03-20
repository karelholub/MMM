from app.utils import meiro_config


def test_mapping_state_tracks_version_history_and_approval(monkeypatch, tmp_path):
    config_path = tmp_path / "meiro_config.json"
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", config_path)
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)

    meiro_config.save_mapping({"touchpoint_attr": "touchpoints"})
    state = meiro_config.get_mapping_state()
    assert state["mapping"]["touchpoint_attr"] == "touchpoints"
    assert state["version"] == 1
    assert state["approval"]["status"] == "approved"
    assert len(state["history"]) == 1
    assert state["history"][0]["action"] == "mapping_saved"

    meiro_config.update_mapping_approval("rejected", "Needs review")
    state = meiro_config.get_mapping_state()
    assert state["approval"]["status"] == "rejected"
    assert state["approval"]["note"] == "Needs review"
    assert state["history"][-1]["action"] == "approval_updated"

    meiro_config.save_mapping({"touchpoint_attr": "journey_touchpoints"})
    state = meiro_config.get_mapping_state()
    assert state["mapping"]["touchpoint_attr"] == "journey_touchpoints"
    assert state["version"] == 2
    assert state["approval"]["status"] == "approved"
