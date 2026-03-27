from app.utils import meiro_config


def test_pull_config_normalizes_dedup_settings(monkeypatch, tmp_path):
    config_path = tmp_path / "meiro_config.json"
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", config_path)
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)

    meiro_config.save_pull_config(
        {
            "lookback_days": "45",
            "session_gap_minutes": "60",
            "conversion_selector": "order_completed",
            "dedup_interval_minutes": "15",
            "dedup_mode": "aggressive",
            "primary_dedup_key": "order_id",
            "fallback_dedup_keys": ["event_id", "order_id", "conversion_id", "bad_key"],
        }
    )

    cfg = meiro_config.get_pull_config()
    assert cfg["lookback_days"] == 45
    assert cfg["session_gap_minutes"] == 60
    assert cfg["conversion_selector"] == "order_completed"
    assert cfg["dedup_interval_minutes"] == 15
    assert cfg["dedup_mode"] == "aggressive"
    assert cfg["primary_dedup_key"] == "order_id"
    assert cfg["fallback_dedup_keys"] == ["event_id", "conversion_id"]


def test_pull_config_falls_back_to_safe_defaults(monkeypatch, tmp_path):
    config_path = tmp_path / "meiro_config.json"
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", config_path)
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)

    meiro_config.save_pull_config(
        {
            "lookback_days": -10,
            "session_gap_minutes": 0,
            "dedup_interval_minutes": -1,
            "dedup_mode": "bad",
            "primary_dedup_key": "bad",
            "fallback_dedup_keys": [],
        }
    )

    cfg = meiro_config.get_pull_config()
    assert cfg["lookback_days"] == 1
    assert cfg["session_gap_minutes"] == 1
    assert cfg["dedup_interval_minutes"] == 0
    assert cfg["dedup_mode"] == "balanced"
    assert cfg["primary_dedup_key"] == "auto"
    assert cfg["fallback_dedup_keys"] == ["conversion_id", "order_id", "event_id"]


def test_pull_config_normalizes_primary_ingest_source(monkeypatch, tmp_path):
    config_path = tmp_path / "meiro_config.json"
    monkeypatch.setattr(meiro_config, "CONFIG_PATH", config_path)
    monkeypatch.setattr(meiro_config, "DATA_DIR", tmp_path)

    meiro_config.save_pull_config(
        {
            "primary_ingest_source": "events",
            "replay_archive_source": "auto",
        }
    )

    cfg = meiro_config.get_pull_config()
    assert cfg["primary_ingest_source"] == "events"
    assert cfg["replay_archive_source"] == "auto"
