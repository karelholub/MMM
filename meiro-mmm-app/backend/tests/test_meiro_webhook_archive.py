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
