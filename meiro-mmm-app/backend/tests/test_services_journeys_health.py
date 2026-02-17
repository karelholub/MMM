from types import SimpleNamespace

from app.services_journeys_health import build_journeys_preview, build_journeys_summary


def test_build_journeys_preview_includes_revenue_value():
    journeys = [
        {
            "customer_id": "c1",
            "converted": True,
            "touchpoints": [{"channel": "email", "timestamp": "2026-01-01T00:00:00+00:00"}],
            "_revenue_entries": [{"dedup_key": "cv:1", "value_in_base": 30.0}],
            "conversion_value": 10.0,
        }
    ]
    out = build_journeys_preview(journeys=journeys, limit=20)
    assert out["total"] == 1
    assert out["rows"][0]["revenue_value"] == 30.0
    assert "conversion_value" in out["rows"][0]


def test_build_journeys_summary_uses_kpi_config_and_import_runs():
    journeys = [
        {
            "customer_id": "c1",
            "converted": True,
            "kpi_type": "purchase",
            "touchpoints": [{"channel": "email", "timestamp": "2026-01-01T00:00:00+00:00"}],
            "_revenue_entries": [{"dedup_key": "cv:1", "value_in_base": 100.0}],
        }
    ]
    kpi_config = SimpleNamespace(
        primary_kpi_id="purchase",
        definitions=[SimpleNamespace(id="purchase", label="Purchase")],
    )

    def _get_runs(**_kwargs):
        return [{"status": "success", "at": "2026-01-02T00:00:00Z", "source": "upload"}]

    out = build_journeys_summary(journeys=journeys, kpi_config=kpi_config, get_import_runs_fn=_get_runs)
    assert out["count"] == 1
    assert out["primary_kpi_id"] == "purchase"
    assert out["primary_kpi_label"] == "Purchase"
    assert out["total_value"] == 100.0
