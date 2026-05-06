from fastapi.testclient import TestClient

from app.main import app
from app.modules.performance import router as performance_router


client = TestClient(app)


def _admin_headers():
    return {"X-User-Role": "admin", "X-User-Id": "qa-admin"}


def test_channel_summary_requires_attribution_view_permission():
    res = client.get(
        "/api/performance/channel/summary",
        params={"date_from": "2026-02-01", "date_to": "2026-02-14"},
    )
    assert res.status_code == 403
    assert res.json()["detail"]["permission"] == "attribution.view"


def test_channel_summary_response_shape():
    res = client.get(
        "/api/performance/channel/summary",
        params={"date_from": "2026-02-01", "date_to": "2026-02-14", "channels": ["google_ads,meta_ads"]},
        headers=_admin_headers(),
    )
    assert res.status_code == 200
    body = res.json()
    assert "current_period" in body
    assert "previous_period" in body
    assert "items" in body
    assert "totals" in body
    assert "config" in body
    assert "mapping_coverage" in body
    assert "meta" in body
    assert isinstance(body["meta"]["channels"], list)
    scope = body["meta"]["meiro_measurement_scope"]
    assert scope["target_sites"]
    assert "source_scope" in scope
    assert "event_archive_site_scope" in scope
    assert "campaign_rows_excluded" in scope


def test_campaign_summary_response_shape_and_note():
    res = client.get(
        "/api/performance/campaign/summary",
        params={"date_from": "2026-02-01", "date_to": "2026-02-14", "compare": "1"},
        headers=_admin_headers(),
    )
    assert res.status_code == 200
    body = res.json()
    assert "items" in body
    assert "totals" in body
    assert "config" in body
    assert "mapping_coverage" in body
    assert "spend_quality" in body
    assert "notes" in body
    assert body["notes"]
    assert "meta" in body
    assert "query_context" in body["meta"]
    assert "scope_filter" in body
    scope = body["meta"]["meiro_measurement_scope"]
    assert scope["target_sites"]
    assert "source_scope" in scope
    assert "event_archive_site_scope" in scope
    assert scope["campaign_rows_excluded"] == body["scope_filter"]["campaign_rows_excluded"]


def test_campaign_summary_includes_conversion_key_meta_when_passed():
    res = client.get(
        "/api/performance/campaign/summary",
        params={"date_from": "2026-02-01", "date_to": "2026-02-14", "conversion_key": "purchase"},
        headers=_admin_headers(),
    )
    assert res.status_code == 200
    body = res.json()
    assert body["meta"]["conversion_key"] == "purchase"


def test_campaign_summary_scope_filter_excludes_out_of_scope_rows(monkeypatch):
    monkeypatch.setattr(performance_router, "get_out_of_scope_campaign_labels", lambda: {"legacy campaign"})
    out = {
        "items": [
            {
                "campaign_id": "legacy campaign",
                "campaign_name": "Legacy Campaign",
                "current": {"spend": 10, "visits": 4, "conversions": 1, "revenue": 20},
                "previous": {"spend": 5, "visits": 2, "conversions": 0, "revenue": 0},
            },
            {
                "campaign_id": "target campaign",
                "campaign_name": "Target Campaign",
                "current": {"spend": 30, "visits": 8, "conversions": 2, "revenue": 80},
                "previous": {"spend": 15, "visits": 3, "conversions": 1, "revenue": 25},
            },
        ],
        "totals": {"current": {}, "previous": {}},
        "notes": [],
    }

    filtered = performance_router._filter_out_of_scope_campaign_items(out)

    assert [item["campaign_id"] for item in filtered["items"]] == ["target campaign"]
    assert filtered["scope_filter"]["out_of_scope_campaign_labels"] == 1
    assert filtered["scope_filter"]["campaign_rows_excluded"] == 1
    assert filtered["totals"]["current"] == {"spend": 30.0, "visits": 8.0, "conversions": 2.0, "revenue": 80.0}
    assert filtered["totals"]["previous"] == {"spend": 15.0, "visits": 3.0, "conversions": 1.0, "revenue": 25.0}
    assert any("Excluded 1 campaign rows" in note for note in filtered["notes"])


def test_performance_conversion_scope_defaults_to_all_when_config_has_primary_key():
    key, resolution = performance_router._resolve_effective_conversion_key(
        None,
        requested_conversion_key=None,
        configured_conversion_key="purchase",
        date_from="2026-04-01",
        date_to="2026-04-15",
    )
    assert key is None
    assert resolution["configured_conversion_key"] == "purchase"
    assert resolution["applied_conversion_key"] is None


def test_performance_conversion_scope_treats_empty_query_as_all_conversions():
    key, resolution = performance_router._resolve_effective_conversion_key(
        None,
        requested_conversion_key="",
        configured_conversion_key="purchase",
        date_from="2026-04-01",
        date_to="2026-04-15",
    )
    assert key is None
    assert resolution["configured_conversion_key"] == "purchase"


def test_channel_lag_response_shape():
    res = client.get(
        "/api/performance/channel/lag",
        params={"date_from": "2026-02-01", "date_to": "2026-02-14"},
        headers=_admin_headers(),
    )
    assert res.status_code == 200
    body = res.json()
    assert body["scope_type"] == "channel"
    assert "current_period" in body
    assert "summary" in body
    assert "items" in body


def test_campaign_lag_response_shape():
    res = client.get(
        "/api/performance/campaign/lag",
        params={"date_from": "2026-02-01", "date_to": "2026-02-14", "conversion_key": "purchase"},
        headers=_admin_headers(),
    )
    assert res.status_code == 200
    body = res.json()
    assert body["scope_type"] == "campaign"
    assert body["conversion_key"] == "purchase"
    assert "summary" in body
    assert "items" in body


def test_campaign_suggestions_payload_keeps_promoted_policy():
    journeys = [
        {
            "converted": True,
            "kpi_type": "purchase",
            "touchpoints": [
                {"timestamp": "2026-02-10T10:00:00Z", "channel": "Paid Search", "campaign": "Brand"},
                {"timestamp": "2026-02-10T10:05:00Z", "channel": "Email", "campaign": "Checkout Rescue"},
            ],
            "conversions": [{"id": "conv-1", "value": 100.0}],
        }
    ]
    payload = performance_router._build_campaign_suggestions_payload(
        journeys=journeys,
        settings=performance_router.NBASettings(
            min_prefix_support=1,
            min_conversion_rate=0.5,
            max_prefix_depth=5,
            min_next_support=5,
            max_suggestions_per_prefix=3,
            min_uplift_pct=0.1,
            excluded_channels=["direct"],
            promoted_journey_policies=[
                {
                    "hypothesis_id": "hyp-campaign-policy",
                    "title": "Promote checkout rescue",
                    "journey_definition_id": "jd-1",
                    "prefix": "Paid Search:Brand",
                    "prefix_steps": ["Paid Search:Brand"],
                    "step": "Email:Checkout Rescue",
                    "channel": "Email",
                    "campaign": "Checkout Rescue",
                }
            ],
        ),
    )

    rec = payload["items"]["Paid Search:Brand"]
    assert rec["step"] == "Email:Checkout Rescue"
    assert rec["is_promoted_policy"] is True
    assert rec["promoted_policy_hypothesis_id"] == "hyp-campaign-policy"
    assert rec["promoted_policy_journey_definition_id"] == "jd-1"


def test_channel_suggestions_payload_keeps_promoted_policy():
    journeys = [
        {
            "converted": True,
            "kpi_type": "purchase",
            "touchpoints": [
                {"timestamp": "2026-02-10T10:00:00Z", "channel": "Paid Search", "campaign": "Brand"},
                {"timestamp": "2026-02-10T10:05:00Z", "channel": "Email", "campaign": "Checkout Rescue"},
            ],
            "conversions": [{"id": "conv-1", "value": 100.0}],
        }
    ]
    payload = performance_router._build_channel_suggestions_payload(
        journeys=journeys,
        settings=performance_router.NBASettings(
            min_prefix_support=1,
            min_conversion_rate=0.5,
            max_prefix_depth=5,
            min_next_support=5,
            max_suggestions_per_prefix=3,
            min_uplift_pct=0.1,
            excluded_channels=["direct"],
            promoted_journey_policies=[
                {
                    "hypothesis_id": "hyp-channel-policy",
                    "title": "Promote checkout rescue email",
                    "journey_definition_id": "jd-1",
                    "prefix": "Paid Search",
                    "prefix_steps": ["Paid Search"],
                    "step": "Email",
                    "channel": "Email",
                    "campaign": None,
                }
            ],
        ),
    )

    assert "" not in payload["items"]
    rec = payload["items"]["Paid Search"]
    assert rec["step"] == "Email"
    assert rec["is_promoted_policy"] is True
    assert rec["promoted_policy_hypothesis_id"] == "hyp-channel-policy"
    assert rec["promoted_policy_journey_definition_id"] == "jd-1"


def test_filter_journeys_for_campaign_suggestions_respects_period_channels_and_conversion_key():
    journeys = [
        {
            "converted": True,
            "kpi_type": "purchase",
            "touchpoints": [
                {"timestamp": "2026-02-10T10:00:00Z", "channel": "Paid Search", "campaign": "Brand"},
            ],
        },
        {
            "converted": True,
            "kpi_type": "lead",
            "touchpoints": [
                {"timestamp": "2026-02-10T10:00:00Z", "channel": "Paid Search", "campaign": "Lead Gen"},
            ],
        },
        {
            "converted": True,
            "kpi_type": "purchase",
            "touchpoints": [
                {"timestamp": "2026-01-10T10:00:00Z", "channel": "Paid Search", "campaign": "Old"},
            ],
        },
        {
            "converted": True,
            "kpi_type": "purchase",
            "touchpoints": [
                {"timestamp": "2026-02-10T10:00:00Z", "channel": "Organic", "campaign": "SEO"},
            ],
        },
    ]

    filtered = performance_router._filter_journeys_for_campaign_suggestions(
        journeys=journeys,
        date_from="2026-02-01",
        date_to="2026-02-14",
        timezone="UTC",
        channels=["Paid Search"],
        conversion_key="purchase",
    )

    assert len(filtered) == 1
    assert filtered[0]["touchpoints"][-1]["campaign"] == "Brand"


def test_resolve_effective_conversion_key_does_not_apply_configured_key_by_default():
    effective_key, resolution = performance_router._resolve_effective_conversion_key(
        db=None,
        requested_conversion_key=None,
        configured_conversion_key="form_submit",
        date_from="2026-04-01",
        date_to="2026-04-01",
    )

    assert effective_key is None
    assert resolution == {
        "requested_conversion_key": None,
        "configured_conversion_key": "form_submit",
        "applied_conversion_key": None,
        "reason": "performance_defaults_to_all_conversions_until_user_selects_a_conversion_key",
    }
