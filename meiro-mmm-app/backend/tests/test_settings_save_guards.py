from __future__ import annotations

from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient

from app.main import app
import app.modules.settings.router as settings_router


def _admin_headers() -> Dict[str, str]:
    return {"X-User-Role": "admin", "X-User-Id": "qa-settings-guard"}


def _get_settings(client: TestClient) -> Dict[str, Any]:
    resp = client.get("/api/settings", headers=_admin_headers())
    assert resp.status_code == 200
    return resp.json()


def _save_settings(
    client: TestClient, payload: Dict[str, Any], *, query: str = ""
) -> Any:
    path = f"/api/settings{query}"
    return client.post(path, headers=_admin_headers(), json=payload)


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


def test_attribution_warning_requires_confirmation(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        settings_router,
        "build_attribution_defaults_overview",
        lambda **_: {
            "decision": {
                "status": "warning",
                "warnings": ["Synthetic attribution warning"],
                "blockers": [],
                "recommended_actions": [],
            }
        },
    )

    original = _get_settings(client)
    candidate = {**original, "attribution": {**original["attribution"]}}
    candidate["attribution"]["lookback_window_days"] = int(
        candidate["attribution"].get("lookback_window_days", 30)
    ) + 1

    denied = _save_settings(client, candidate)
    assert denied.status_code == 409
    assert denied.json()["detail"]["decision"]["status"] == "warning"

    allowed = _save_settings(
        client,
        candidate,
        query="?confirm_attribution_warnings=true",
    )
    assert allowed.status_code == 200

    restored = _save_settings(
        client,
        original,
        query="?confirm_attribution_warnings=true&confirm_nba_warnings=true&confirm_mmm_warnings=true",
    )
    assert restored.status_code == 200


def test_nba_blocked_rejects_even_with_confirmation(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        settings_router,
        "build_nba_preview_summary",
        lambda **_: {
            "decision": {
                "status": "blocked",
                "blockers": ["Synthetic nba blocker"],
                "warnings": [],
                "recommended_actions": [],
            }
        },
    )

    original = _get_settings(client)
    candidate = {**original, "nba": {**original["nba"]}}
    candidate["nba"]["min_prefix_support"] = int(candidate["nba"].get("min_prefix_support", 5)) + 1
    candidate["nba"]["min_next_support"] = max(
        int(candidate["nba"].get("min_next_support", 1)),
        int(candidate["nba"]["min_prefix_support"]),
    )

    denied = _save_settings(client, candidate)
    assert denied.status_code == 409
    assert denied.json()["detail"]["decision"]["status"] == "blocked"

    denied_with_confirm = _save_settings(
        client,
        candidate,
        query="?confirm_nba_warnings=true",
    )
    assert denied_with_confirm.status_code == 409
    assert denied_with_confirm.json()["detail"]["decision"]["status"] == "blocked"

    restored = _save_settings(
        client,
        original,
        query="?confirm_attribution_warnings=true&confirm_nba_warnings=true&confirm_mmm_warnings=true",
    )
    assert restored.status_code == 200


def test_mmm_warning_requires_confirmation(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        settings_router,
        "build_mmm_defaults_preview",
        lambda **_: {
            "decision": {
                "status": "warning",
                "warnings": ["Synthetic mmm warning"],
                "blockers": [],
                "recommended_actions": [],
            }
        },
    )

    original = _get_settings(client)
    candidate = {**original, "mmm": {**original["mmm"]}}
    candidate["mmm"]["frequency"] = "M" if original["mmm"].get("frequency") == "W" else "W"

    denied = _save_settings(client, candidate)
    assert denied.status_code == 409
    assert denied.json()["detail"]["decision"]["status"] == "warning"

    allowed = _save_settings(
        client,
        candidate,
        query="?confirm_mmm_warnings=true",
    )
    assert allowed.status_code == 200

    restored = _save_settings(
        client,
        original,
        query="?confirm_attribution_warnings=true&confirm_nba_warnings=true&confirm_mmm_warnings=true",
    )
    assert restored.status_code == 200
