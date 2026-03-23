from app.services_model_config_suggestions import suggest_model_config_from_journeys


def test_suggest_model_config_from_journeys_uses_observed_windows_and_channels():
    journeys = [
        {
            "meta": {"parser": {"used_inferred_mapping": False}},
            "touchpoints": [
                {"channel": "paid_search", "ts": "2026-03-01T09:00:00Z"},
                {"channel": "email", "ts": "2026-03-03T09:00:00Z"},
            ],
            "conversions": [
                {"name": "purchase", "ts": "2026-03-05T09:00:00Z"},
            ],
            "_revenue_entries": [{"default_applied": False}],
        },
        {
            "meta": {"parser": {"used_inferred_mapping": True}},
            "touchpoints": [
                {"channel": "paid_social", "ts": "2026-03-02T09:00:00Z"},
                {"channel": "direct", "ts": "2026-03-04T09:00:00Z"},
            ],
            "conversions": [
                {"name": "purchase", "ts": "2026-03-08T09:00:00Z"},
            ],
            "_revenue_entries": [{"default_applied": True}],
        },
    ]
    kpis = [
        {"id": "purchase", "label": "Purchase", "event_name": "purchase_completed", "value_field": "revenue"},
        {"id": "signup", "label": "Signup", "event_name": "signup"},
    ]

    result = suggest_model_config_from_journeys(journeys, kpi_definitions=kpis, strategy="balanced")

    assert result["preview_available"] is True
    assert result["config_json"]["conversions"]["primary_conversion_key"] == "purchase"
    assert "paid_search" in result["config_json"]["eligible_touchpoints"]["include_channels"]
    assert "direct" in result["config_json"]["eligible_touchpoints"]["exclude_channels"]
    assert result["config_json"]["windows"]["click_lookback_days"] >= 3
    assert result["data_summary"]["inferred_mapping_journey_pct"] > 0
    assert result["warnings"]


def test_suggest_model_config_maps_observed_conversion_names_to_known_kpi_keys():
    journeys = [
        {
            "touchpoints": [{"channel": "email", "ts": "2026-03-01T09:00:00Z"}],
            "conversions": [{"name": "lead", "ts": "2026-03-02T09:00:00Z"}],
        },
        {
            "touchpoints": [{"channel": "paid_search", "ts": "2026-03-03T09:00:00Z"}],
            "conversions": [{"name": "purchase", "ts": "2026-03-05T09:00:00Z"}],
        },
    ]
    kpis = [
        {"id": "form_submit", "label": "Lead", "event_name": "lead_submitted", "value_field": "value"},
        {"id": "order_finalized", "label": "Purchase", "event_name": "purchase_completed", "value_field": "value"},
    ]

    result = suggest_model_config_from_journeys(journeys, kpi_definitions=kpis, strategy="balanced")

    definitions = result["config_json"]["conversions"]["conversion_definitions"]
    keys = [item["key"] for item in definitions]

    assert result["config_json"]["conversions"]["primary_conversion_key"] == "form_submit"
    assert keys == ["form_submit", "order_finalized"]
