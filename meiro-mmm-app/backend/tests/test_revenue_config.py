from app.services_revenue_config import compute_payload_revenue_value, normalize_revenue_config


def test_revenue_config_filters_event_names_and_dedup_by_order_id():
    cfg = normalize_revenue_config(
        {
            "conversion_names": ["purchase"],
            "dedup_key": "order_id",
            "value_field_path": "value",
            "currency_field_path": "currency",
            "fx_enabled": False,
        }
    )
    dedupe_seen = set()
    payload_a = {
        "conversion_id": "cv-1",
        "conversions": [
            {"id": "cv-1", "name": "purchase", "order_id": "ord-1", "value": 100, "currency": "EUR"},
            {"id": "cv-2", "name": "signup", "order_id": "ord-2", "value": 999, "currency": "EUR"},
        ],
    }
    payload_b = {
        "conversion_id": "cv-3",
        "conversions": [
            {"id": "cv-3", "name": "purchase", "order_id": "ord-1", "value": 250, "currency": "EUR"}
        ],
    }
    first = compute_payload_revenue_value(payload_a, cfg, dedupe_seen=dedupe_seen)
    second = compute_payload_revenue_value(payload_b, cfg, dedupe_seen=dedupe_seen)
    assert first == 100.0
    assert second == 0.0


def test_revenue_config_applies_static_fx_rate_to_base_currency():
    cfg = normalize_revenue_config(
        {
            "conversion_names": ["purchase"],
            "base_currency": "EUR",
            "fx_enabled": True,
            "fx_mode": "static_rates",
            "fx_rates_json": {"USD": 0.9},
        }
    )
    payload = {
        "conversion_id": "cv-1",
        "conversions": [{"id": "cv-1", "name": "purchase", "value": 50, "currency": "USD"}],
    }
    total = compute_payload_revenue_value(payload, cfg, dedupe_seen=set())
    assert total == 45.0


def test_revenue_config_defaults_for_unset_fields_are_backward_compatible():
    cfg = normalize_revenue_config({})
    assert cfg["conversion_names"] == ["purchase"]
    assert cfg["dedup_key"] == "conversion_id"
    assert cfg["base_currency"] == "EUR"
    payload = {
        "conversion_id": "cv-1",
        "conversions": [{"id": "cv-1", "name": "purchase", "value": 12.5, "currency": "EUR"}],
    }
    total = compute_payload_revenue_value(payload, cfg, dedupe_seen=set())
    assert total == 12.5

