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


def test_expenses_exclude_out_of_scope_campaigns_by_default(monkeypatch, tmp_path):
    expenses_file = tmp_path / "expenses.json"
    audit_file = tmp_path / "expenses_audit.json"
    monkeypatch.setattr(main, "EXPENSES_FILE", expenses_file)
    monkeypatch.setattr(main, "EXPENSE_AUDIT_FILE", audit_file)
    monkeypatch.setattr(main, "EXPENSES", {})
    monkeypatch.setattr(main, "EXPENSE_AUDIT_LOG", [])
    monkeypatch.setattr(main, "get_target_site_domains", lambda: ["meiro.io", "meir.store"])

    def fake_expense_site_scope(expense):
        campaign = getattr(expense, "campaign", None) if not isinstance(expense, dict) else expense.get("campaign")
        status = "out_of_scope" if campaign == "myTimi" else "target_site"
        return {"status": status, "campaign": campaign, "target_sites": ["meiro.io", "meir.store"]}

    monkeypatch.setattr(main, "expense_site_scope", fake_expense_site_scope)
    main.EXPENSES = {
        "target": main.ExpenseEntry(
            channel="paid_search",
            campaign="brand",
            amount=100,
            currency="USD",
            reporting_currency="USD",
            source_type="manual",
        ),
        "legacy": main.ExpenseEntry(
            channel="paid_social",
            campaign="myTimi",
            amount=50,
            currency="USD",
            reporting_currency="USD",
            source_type="import",
        ),
    }

    client = TestClient(app)
    listed = client.get("/api/expenses").json()
    assert [item["id"] for item in listed] == ["target"]

    listed_with_scope = client.get("/api/expenses?include_out_of_scope=true").json()
    assert {item["id"] for item in listed_with_scope} == {"target", "legacy"}
    assert next(item for item in listed_with_scope if item["id"] == "legacy")["site_scope"]["status"] == "out_of_scope"

    summary = client.get("/api/expenses/summary").json()
    assert summary["total"] == 100
    assert summary["site_scope"]["out_of_scope_count"] == 1
    assert summary["site_scope"]["out_of_scope_total"] == 50
    assert summary["site_scope"]["out_of_scope_excluded"] is True

    summary_with_scope = client.get("/api/expenses/summary?include_out_of_scope=true").json()
    assert summary_with_scope["total"] == 150
