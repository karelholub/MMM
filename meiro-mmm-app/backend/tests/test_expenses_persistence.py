from fastapi.testclient import TestClient

from app import main
from app.main import app


def test_expenses_persist_across_state_reload(monkeypatch, tmp_path):
    expenses_file = tmp_path / "expenses.json"
    audit_file = tmp_path / "expenses_audit.json"
    monkeypatch.setattr(main, "EXPENSES_FILE", expenses_file)
    monkeypatch.setattr(main, "EXPENSE_AUDIT_FILE", audit_file)
    monkeypatch.setattr(main, "EXPENSES", {})
    monkeypatch.setattr(main, "EXPENSE_AUDIT_LOG", [])

    client = TestClient(app)
    response = client.post(
        "/api/expenses",
        json={
            "channel": "google_ads",
            "campaign": "spring-launch",
            "cost_type": "Media Spend",
            "amount": 123.45,
            "currency": "USD",
            "service_period_start": "2026-04-01",
            "service_period_end": "2026-04-04",
            "notes": "Manual persistence test",
            "source_type": "manual",
            "actor_type": "manual",
        },
    )
    assert response.status_code == 200
    created = response.json()
    expense_id = created["id"]

    assert expenses_file.exists()
    assert audit_file.exists()

    monkeypatch.setattr(main, "EXPENSES", {})
    monkeypatch.setattr(main, "EXPENSE_AUDIT_LOG", [])
    main._load_expense_state()

    assert expense_id in main.EXPENSES
    loaded = main.EXPENSES[expense_id]
    assert loaded.amount == 123.45
    assert loaded.campaign == "spring-launch"
    assert any(event.expense_id == expense_id and event.event_type == "created" for event in main.EXPENSE_AUDIT_LOG)


def test_expenses_default_seed_used_only_when_no_persisted_state(monkeypatch, tmp_path):
    expenses_file = tmp_path / "expenses.json"
    audit_file = tmp_path / "expenses_audit.json"
    monkeypatch.setattr(main, "EXPENSES_FILE", expenses_file)
    monkeypatch.setattr(main, "EXPENSE_AUDIT_FILE", audit_file)
    monkeypatch.setattr(main, "EXPENSES", {})
    monkeypatch.setattr(main, "EXPENSE_AUDIT_LOG", [])

    main._load_expense_state()

    assert "google_ads_2024-01" in main.EXPENSES
    assert len(main.EXPENSE_AUDIT_LOG) == 0
