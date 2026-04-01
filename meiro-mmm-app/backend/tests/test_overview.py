"""Tests for Overview (Cover) Dashboard API endpoints."""

from datetime import datetime

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import ChannelPerformanceDaily, ConversionPath, JourneyDefinition, JourneyDefinitionInstanceFact, JourneyPathDaily, SilverConversionFact
from app.main import app
from app import services_overview as overview
from app.services_conversions import persist_journeys_as_conversion_paths
from app.services_overview import get_overview_drivers, get_overview_funnels
from app.services_overview import get_overview_summary, get_overview_trend_insights

client = TestClient(app)


def _unit_db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _admin_headers():
    return {"X-User-Role": "admin", "X-User-Id": "qa-admin"}


def test_overview_summary_returns_consistent_shape():
    """GET /api/overview/summary returns kpi_tiles, highlights, freshness."""
    resp = client.get(
        "/api/overview/summary",
        params={"date_from": "2024-01-01", "date_to": "2024-01-31"},
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "kpi_tiles" in body
    assert "highlights" in body
    assert "freshness" in body
    assert "outcomes" in body
    assert isinstance(body["kpi_tiles"], list)
    assert isinstance(body["highlights"], list)
    assert "last_touchpoint_ts" in body["freshness"]
    assert "last_conversion_ts" in body["freshness"]
    assert "ingest_lag_minutes" in body["freshness"]
    assert "current_period" in body
    assert "previous_period" in body
    for tile in body["kpi_tiles"]:
        assert "kpi_key" in tile
        assert "value" in tile
        assert "delta_pct" in tile
        assert "delta_abs" in tile
        assert "current_period" in tile
        assert "previous_period" in tile
        assert "series" in tile
        assert "series_prev" in tile
        assert "confidence_score" in tile
        assert "confidence_level" in tile
        assert "confidence_reasons" in tile
        assert tile["kpi_key"] in ("spend", "visits", "conversions", "revenue", "net_conversions", "net_revenue")


def test_overview_summary_previous_period_is_equal_length():
    resp = client.get(
        "/api/overview/summary",
        params={"date_from": "2024-02-01", "date_to": "2024-02-14"},
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    cp = body["current_period"]
    pp = body["previous_period"]
    cp_from = datetime.fromisoformat(cp["date_from"])
    cp_to = datetime.fromisoformat(cp["date_to"])
    pp_from = datetime.fromisoformat(pp["date_from"])
    pp_to = datetime.fromisoformat(pp["date_to"])
    assert (cp_to - cp_from) == (pp_to - pp_from)
    assert pp_to < cp_from


def test_overview_summary_empty_series_do_not_fake_flat_lines():
    resp = client.get(
        "/api/overview/summary",
        params={"date_from": "2024-03-01", "date_to": "2024-03-07"},
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    for tile in body["kpi_tiles"]:
        if tile["value"] == 0:
            assert tile["series"] == []


def test_overview_summary_same_day_series_uses_naive_silver_timestamps():
    db = _unit_db_session()
    try:
        db.add(
            SilverConversionFact(
                conversion_id="conv-hourly",
                profile_id="p-hourly",
                conversion_key="purchase",
                conversion_ts=datetime(2024, 2, 15, 12, 34),
                interaction_path_type="click_through",
                gross_conversions_total=1.0,
                net_conversions_total=1.0,
                gross_revenue_total=120.0,
                net_revenue_total=120.0,
            )
        )
        db.commit()

        body = get_overview_summary(
            db,
            date_from="2024-02-15",
            date_to="2024-02-15",
            timezone="UTC",
            expenses={},
            import_runs_get_last_successful=lambda: None,
        )

        conversions_tile = next(tile for tile in body["kpi_tiles"] if tile["kpi_key"] == "conversions")
        revenue_tile = next(tile for tile in body["kpi_tiles"] if tile["kpi_key"] == "revenue")
        assert any(point["value"] == 1.0 for point in conversions_tile["series"])
        assert any(point["value"] == 120.0 for point in revenue_tile["series"])
    finally:
        db.close()


def test_overview_summary_optional_params():
    """Summary accepts timezone, currency, workspace, account, model_id."""
    resp = client.get(
        "/api/overview/summary",
        params={
            "date_from": "2024-01-01",
            "date_to": "2024-01-31",
            "timezone": "Europe/London",
            "currency": "USD",
            "workspace": "ws1",
            "model_id": "cfg-1",
        },
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("timezone") == "Europe/London"
    assert body.get("currency") == "USD"
    assert body.get("model_id") == "cfg-1"


def test_overview_summary_missing_dates_validation():
    """Summary requires date_from and date_to."""
    resp = client.get("/api/overview/summary", params={"date_from": "2024-01-01"}, headers=_admin_headers())
    assert resp.status_code == 422


def test_overview_drivers_returns_consistent_shape():
    """GET /api/overview/drivers returns by_channel, by_campaign, biggest_movers."""
    resp = client.get(
        "/api/overview/drivers",
        params={"date_from": "2024-01-01", "date_to": "2024-01-31"},
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "by_channel" in body
    assert "by_campaign" in body
    assert "biggest_movers" in body
    assert isinstance(body["by_channel"], list)
    assert isinstance(body["by_campaign"], list)
    assert isinstance(body["biggest_movers"], list)
    for ch in body["by_channel"]:
        assert "channel" in ch
        assert "spend" in ch
        assert "visits" in ch
        assert "conversions" in ch
        assert "revenue" in ch
        assert "outcomes" in ch


def test_overview_drivers_top_n():
    """Drivers accepts top_campaigns_n and conversion_key."""
    resp = client.get(
        "/api/overview/drivers",
        params={
            "date_from": "2024-01-01",
            "date_to": "2024-01-31",
            "top_campaigns_n": 5,
        },
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["by_campaign"]) <= 5


def test_overview_alerts_returns_consistent_shape():
    """GET /api/overview/alerts returns alerts list with deep_link."""
    resp = client.get("/api/overview/alerts", headers=_admin_headers())
    assert resp.status_code == 200
    body = resp.json()
    assert "alerts" in body
    assert "total" in body
    assert isinstance(body["alerts"], list)
    for a in body["alerts"]:
        assert "id" in a
        assert "ts_detected" in a
        assert "deep_link" in a
        assert "status" in a


def test_overview_alerts_optional_filters():
    """Alerts accepts scope, status, limit."""
    resp = client.get(
        "/api/overview/alerts",
        params={"scope": "default", "limit": 10},
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["alerts"]) <= 10


def test_overview_drivers_last_touch_count_uses_position_not_value_equality():
    db = _unit_db_session()
    try:
        row = ConversionPath(
            conversion_id="conv-1",
            profile_id="p-1",
            conversion_key="signup",
            conversion_ts=datetime(2024, 2, 15, 12, 0),
            path_json={
                "conversion_value": 100.0,
                "touchpoints": [
                    {"channel": "email", "campaign": "camp-a"},
                    {"channel": "email", "campaign": "camp-a"},
                ],
            },
            path_hash="hash-1",
            length=2,
            first_touch_ts=datetime(2024, 2, 15, 11, 0),
            last_touch_ts=datetime(2024, 2, 15, 11, 30),
        )
        db.add(row)
        db.commit()

        out = get_overview_drivers(
            db,
            date_from="2024-02-01",
            date_to="2024-02-29",
            expenses={},
            top_campaigns_n=10,
        )
        by_channel = {x["channel"]: x for x in out["by_channel"]}
        assert by_channel["email"]["conversions"] == 1
    finally:
        db.close()


def test_overview_drivers_date_only_window_includes_same_day_rows():
    db = _unit_db_session()
    try:
        row = ConversionPath(
            conversion_id="conv-same-day",
            profile_id="p-same-day",
            conversion_key="purchase",
            conversion_ts=datetime(2024, 2, 15, 12, 0),
            path_json={
                "conversion_value": 100.0,
                "touchpoints": [
                    {"timestamp": "2024-02-15T10:00:00Z", "channel": "paid_social", "campaign": "launch"},
                    {"timestamp": "2024-02-15T11:00:00Z", "channel": "direct"},
                ],
            },
            path_hash="same-day-hash",
            length=2,
            first_touch_ts=datetime(2024, 2, 15, 10, 0),
            last_touch_ts=datetime(2024, 2, 15, 11, 0),
        )
        db.add(row)
        db.commit()

        out = get_overview_drivers(
            db,
            date_from="2024-02-15",
            date_to="2024-02-15",
            expenses={},
            top_campaigns_n=10,
        )

        by_channel = {item["channel"]: item for item in out["by_channel"]}
        assert by_channel["direct"]["conversions"] == 1
        assert by_channel["paid_social"]["revenue"] > 0
    finally:
        db.close()


def test_overview_funnels_ranks_paths_by_conversions_revenue_and_speed():
    db = _unit_db_session()
    try:
        rows = [
            ConversionPath(
                conversion_id="conv-1",
                profile_id="p-1",
                conversion_key="purchase",
                conversion_ts=datetime(2024, 2, 10, 12, 0),
                path_json={
                    "conversion_value": 100.0,
                    "touchpoints": [
                        {"channel": "paid_social"},
                        {"channel": "direct"},
                    ],
                },
                path_hash="hash-1",
                length=2,
                first_touch_ts=datetime(2024, 2, 8, 12, 0),
                last_touch_ts=datetime(2024, 2, 10, 11, 0),
            ),
            ConversionPath(
                conversion_id="conv-2",
                profile_id="p-2",
                conversion_key="purchase",
                conversion_ts=datetime(2024, 2, 11, 12, 0),
                path_json={
                    "conversion_value": 90.0,
                    "touchpoints": [
                        {"channel": "paid_social"},
                        {"channel": "direct"},
                    ],
                },
                path_hash="hash-2",
                length=2,
                first_touch_ts=datetime(2024, 2, 10, 12, 0),
                last_touch_ts=datetime(2024, 2, 11, 11, 0),
            ),
            ConversionPath(
                conversion_id="conv-3",
                profile_id="p-3",
                conversion_key="purchase",
                conversion_ts=datetime(2024, 2, 12, 12, 0),
                path_json={
                    "conversion_value": 400.0,
                    "touchpoints": [
                        {"channel": "email"},
                        {"channel": "direct"},
                    ],
                },
                path_hash="hash-3",
                length=2,
                first_touch_ts=datetime(2024, 2, 12, 6, 0),
                last_touch_ts=datetime(2024, 2, 12, 11, 0),
            ),
            ConversionPath(
                conversion_id="conv-4",
                profile_id="p-4",
                conversion_key="purchase",
                conversion_ts=datetime(2024, 2, 13, 12, 0),
                path_json={
                    "conversion_value": 40.0,
                    "touchpoints": [
                        {"channel": "organic_search"},
                        {"channel": "email"},
                    ],
                },
                path_hash="hash-4",
                length=2,
                first_touch_ts=datetime(2024, 2, 13, 10, 0),
                last_touch_ts=datetime(2024, 2, 13, 11, 0),
            ),
        ]
        for row in rows:
            db.add(row)
        db.commit()

        out = get_overview_funnels(
            db,
            date_from="2024-02-01",
            date_to="2024-02-29",
            limit=5,
        )

        assert out["summary"]["total_conversions"] == 4
        assert "net_conversions" in out["summary"]
        assert "gross_revenue" in out["summary"]
        assert out["tabs"]["conversions"][0]["path"] == "paid_social > direct"
        assert out["tabs"]["conversions"][0]["conversions"] == 2
        assert out["tabs"]["revenue"][0]["path"] == "email > direct"
        assert out["tabs"]["revenue"][0]["revenue"] == 400.0
        assert out["tabs"]["speed"][0]["path"] == "email > direct"
        assert out["tabs"]["speed"][0]["median_days_to_convert"] < out["tabs"]["conversions"][0]["median_days_to_convert"]
        assert out["summary"]["top_paths_conversion_share"] == 1.0
    finally:
        db.close()


def test_overview_funnels_endpoint_returns_expected_shape():
    resp = client.get(
        "/api/overview/funnels",
        params={"date_from": "2024-01-01", "date_to": "2024-01-31"},
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "summary" in body
    assert "tabs" in body
    assert set(body["tabs"].keys()) == {"conversions", "revenue", "speed"}


def test_overview_funnels_prefers_daily_aggregates_when_single_definition_exists():
    db = _unit_db_session()
    try:
        definition = JourneyDefinition(
            id="def-overview",
            name="Overview Journey",
            conversion_kpi_id="purchase",
            lookback_window_days=30,
            mode_default="conversion_only",
            created_by="test",
            updated_by="test",
            is_archived=False,
            created_at=datetime(2024, 2, 1, 0, 0),
            updated_at=datetime(2024, 2, 1, 0, 0),
        )
        db.add(definition)
        db.add_all(
            [
                JourneyPathDaily(
                    date=datetime(2024, 2, 10).date(),
                    journey_definition_id="def-overview",
                    path_hash="path-a",
                    path_steps=["Paid Landing", "Purchase / Lead Won (conversion)"],
                    path_length=2,
                    count_journeys=2,
                    count_conversions=2,
                    gross_conversions_total=2.0,
                    net_conversions_total=2.0,
                    gross_revenue_total=190.0,
                    net_revenue_total=190.0,
                    click_through_conversions_total=2.0,
                    view_through_conversions_total=0.0,
                    mixed_path_conversions_total=0.0,
                    avg_time_to_convert_sec=129600.0,
                    p50_time_to_convert_sec=129600.0,
                    p90_time_to_convert_sec=129600.0,
                    channel_group="paid",
                    campaign_id="cmp-a",
                    device="mobile",
                    country="US",
                    created_at=datetime(2024, 2, 10, 0, 0),
                    updated_at=datetime(2024, 2, 10, 0, 0),
                ),
                JourneyPathDaily(
                    date=datetime(2024, 2, 11).date(),
                    journey_definition_id="def-overview",
                    path_hash="path-b",
                    path_steps=["Organic Landing", "Purchase / Lead Won (conversion)"],
                    path_length=2,
                    count_journeys=1,
                    count_conversions=1,
                    gross_conversions_total=1.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=300.0,
                    net_revenue_total=300.0,
                    click_through_conversions_total=1.0,
                    view_through_conversions_total=0.0,
                    mixed_path_conversions_total=0.0,
                    avg_time_to_convert_sec=43200.0,
                    p50_time_to_convert_sec=43200.0,
                    p90_time_to_convert_sec=43200.0,
                    channel_group="organic",
                    campaign_id=None,
                    device="desktop",
                    country="US",
                    created_at=datetime(2024, 2, 11, 0, 0),
                    updated_at=datetime(2024, 2, 11, 0, 0),
                ),
            ]
        )
        db.commit()

        out = get_overview_funnels(
            db,
            date_from="2024-02-01",
            date_to="2024-02-29",
            conversion_key="purchase",
            limit=5,
        )

        assert out["summary"]["total_conversions"] == 3
        assert out["summary"]["gross_revenue"] == 490.0
        assert out["tabs"]["conversions"][0]["path"] == "Paid Landing > Purchase / Lead Won (conversion)"
        assert out["tabs"]["revenue"][0]["revenue"] == 300.0
        assert out["tabs"]["speed"][0]["path"] == "Organic Landing > Purchase / Lead Won (conversion)"
    finally:
        db.close()


def test_overview_funnels_falls_back_to_definition_outputs_when_daily_rows_absent():
    db = _unit_db_session()
    try:
        definition = JourneyDefinition(
            id="def-funnels-fallback",
            name="Overview Journey",
            conversion_kpi_id="purchase",
            lookback_window_days=30,
            mode_default="conversion_only",
            created_by="test",
            updated_by="test",
            is_archived=False,
            created_at=datetime(2024, 2, 1, 0, 0),
            updated_at=datetime(2024, 2, 1, 0, 0),
        )
        db.add(definition)
        db.add_all(
            [
                JourneyDefinitionInstanceFact(
                    date=datetime(2024, 2, 12).date(),
                    journey_definition_id="def-funnels-fallback",
                    conversion_id="conv-a",
                    profile_id="p-1",
                    conversion_key="purchase",
                    conversion_ts=datetime(2024, 2, 12, 12, 0),
                    path_hash="hash-a",
                    steps_json=["Paid Landing", "Purchase / Lead Won (conversion)"],
                    path_length=2,
                    channel_group="paid",
                    last_touch_channel="google_ads",
                    campaign_id="cmp-1",
                    device="mobile",
                    country="US",
                    interaction_path_type="click_through",
                    time_to_convert_sec=86400.0,
                    gross_conversions_total=1.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=300.0,
                    net_revenue_total=250.0,
                    created_at=datetime(2024, 2, 12, 12, 0),
                    updated_at=datetime(2024, 2, 12, 12, 0),
                ),
                JourneyDefinitionInstanceFact(
                    date=datetime(2024, 2, 13).date(),
                    journey_definition_id="def-funnels-fallback",
                    conversion_id="conv-b",
                    profile_id="p-2",
                    conversion_key="purchase",
                    conversion_ts=datetime(2024, 2, 13, 12, 0),
                    path_hash="hash-b",
                    steps_json=["Organic Landing", "Purchase / Lead Won (conversion)"],
                    path_length=2,
                    channel_group="organic",
                    last_touch_channel="seo",
                    campaign_id=None,
                    device="desktop",
                    country="US",
                    interaction_path_type="click_through",
                    time_to_convert_sec=43200.0,
                    gross_conversions_total=1.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=100.0,
                    net_revenue_total=90.0,
                    created_at=datetime(2024, 2, 13, 12, 0),
                    updated_at=datetime(2024, 2, 13, 12, 0),
                ),
            ]
        )
        db.commit()

        out = get_overview_funnels(
            db,
            date_from="2024-02-10",
            date_to="2024-02-15",
            conversion_key="purchase",
            limit=5,
        )

        assert out["tabs"]["conversions"][0]["path"] == "Paid Landing > Purchase / Lead Won (conversion)"
        assert out["tabs"]["revenue"][0]["revenue"] == 300.0
        assert out["tabs"]["speed"][0]["path"] == "Organic Landing > Purchase / Lead Won (conversion)"
        assert out["summary"]["gross_revenue"] == 400.0
    finally:
        db.close()


def test_overview_summary_uses_instance_facts_when_conversion_and_silver_rows_are_missing():
    db = _unit_db_session()
    try:
        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-1"},
                    "touchpoints": [
                        {"ts": "2026-02-01T10:00:00Z", "channel": "google_ads", "interaction_type": "click"},
                    ],
                    "conversions": [{"id": "conv-1", "name": "purchase", "ts": "2026-02-01T12:00:00Z", "value": 120.0}],
                }
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="overview-instance-summary-batch",
        )
        assert inserted == 1
        db.query(ConversionPath).delete(synchronize_session=False)
        db.query(SilverConversionFact).delete(synchronize_session=False)
        db.commit()

        out = get_overview_summary(
            db,
            date_from="2026-02-01",
            date_to="2026-02-01",
            expenses={},
        )

        tiles = {tile["kpi_key"]: tile for tile in out["kpi_tiles"]}
        assert tiles["visits"]["value"] == 1
        assert tiles["conversions"]["value"] == 1
        assert tiles["revenue"]["value"] == 120.0
        assert out["outcomes"]["current"]["gross_value"] == 120.0
        assert out["outcomes"]["current"]["net_value"] == 120.0
    finally:
        db.close()


def test_overview_drivers_uses_instance_facts_when_conversion_and_silver_rows_are_missing():
    db = _unit_db_session()
    try:
        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                {
                        "_schema": "v2",
                        "customer": {"id": "cust-prev"},
                        "touchpoints": [
                            {
                                "ts": "2026-01-30T10:00:00Z",
                                "channel": "email",
                                "campaign": {"id": "cmp-prev", "name": "Prev"},
                                "interaction_type": "click",
                            },
                        ],
                        "conversions": [{"id": "conv-prev", "name": "purchase", "ts": "2026-01-30T12:00:00Z", "value": 80.0}],
                    },
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-1"},
                    "touchpoints": [
                        {
                            "ts": "2026-02-01T10:00:00Z",
                            "channel": "google_ads",
                            "campaign": {"id": "cmp-1", "name": "Brand"},
                            "interaction_type": "click",
                        },
                    ],
                    "conversions": [{"id": "conv-1", "name": "purchase", "ts": "2026-02-01T12:00:00Z", "value": 120.0}],
                }
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="overview-instance-drivers-batch",
        )
        assert inserted == 2
        db.query(ConversionPath).delete(synchronize_session=False)
        db.query(SilverConversionFact).delete(synchronize_session=False)
        db.commit()

        out = get_overview_drivers(
            db,
            date_from="2026-02-01",
            date_to="2026-02-02",
            expenses={},
            top_campaigns_n=5,
        )

        assert out["by_channel"]
        by_channel = {row["channel"]: row for row in out["by_channel"]}
        assert by_channel["google_ads"]["revenue"] == 120.0
        assert out["by_campaign"]
        by_campaign = {row["campaign"]: row for row in out["by_campaign"]}
        assert by_campaign["Brand"]["revenue"] == 120.0
    finally:
        db.close()


def test_overview_summary_prefers_daily_aggregates_when_single_definition_exists():
    db = _unit_db_session()
    try:
        definition = JourneyDefinition(
            id="def-summary",
            name="Summary Journey",
            conversion_kpi_id="purchase",
            lookback_window_days=30,
            mode_default="conversion_only",
            created_by="test",
            updated_by="test",
            is_archived=False,
            created_at=datetime(2024, 2, 1, 0, 0),
            updated_at=datetime(2024, 2, 1, 0, 0),
        )
        db.add(definition)
        db.add_all(
            [
                JourneyPathDaily(
                    date=datetime(2024, 2, 11).date(),
                    journey_definition_id="def-summary",
                    path_hash="current-path",
                    path_steps=["Paid Landing", "Purchase / Lead Won (conversion)"],
                    path_length=2,
                    count_journeys=4,
                    count_conversions=3,
                    gross_conversions_total=3.0,
                    net_conversions_total=2.0,
                    gross_revenue_total=490.0,
                    net_revenue_total=450.0,
                    click_through_conversions_total=2.0,
                    view_through_conversions_total=0.0,
                    mixed_path_conversions_total=0.0,
                    avg_time_to_convert_sec=86400.0,
                    p50_time_to_convert_sec=86400.0,
                    p90_time_to_convert_sec=86400.0,
                    created_at=datetime(2024, 2, 11, 0, 0),
                    updated_at=datetime(2024, 2, 11, 0, 0),
                ),
                JourneyPathDaily(
                    date=datetime(2024, 2, 5).date(),
                    journey_definition_id="def-summary",
                    path_hash="previous-path",
                    path_steps=["Email", "Purchase / Lead Won (conversion)"],
                    path_length=2,
                    count_journeys=2,
                    count_conversions=1,
                    gross_conversions_total=1.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=120.0,
                    net_revenue_total=120.0,
                    click_through_conversions_total=1.0,
                    view_through_conversions_total=0.0,
                    mixed_path_conversions_total=0.0,
                    avg_time_to_convert_sec=43200.0,
                    p50_time_to_convert_sec=43200.0,
                    p90_time_to_convert_sec=43200.0,
                    created_at=datetime(2024, 2, 5, 0, 0),
                    updated_at=datetime(2024, 2, 5, 0, 0),
                ),
            ]
        )
        db.commit()

        out = get_overview_summary(
            db,
            date_from="2024-02-10",
            date_to="2024-02-15",
            expenses={},
        )

        tiles = {tile["kpi_key"]: tile for tile in out["kpi_tiles"]}
        assert tiles["conversions"]["value"] == 3
        assert tiles["revenue"]["value"] == 490.0
        assert tiles["net_conversions"]["value"] == 2
        assert tiles["net_revenue"]["value"] == 450.0
        assert any(point["value"] == 490.0 for point in tiles["revenue"]["series"])
        assert out["outcomes"]["current"]["gross_conversions"] == 3.0
        assert out["outcomes"]["current"]["net_conversions"] == 2.0
        assert out["outcomes"]["previous"]["gross_value"] == 120.0
    finally:
        db.close()


def test_overview_summary_prefers_channel_facts_when_available():
    db = _unit_db_session()
    try:
        db.add_all(
            [
                ChannelPerformanceDaily(
                    date=datetime(2024, 2, 10).date(),
                    channel="google_ads",
                    conversion_key=None,
                    visits_total=5,
                    count_conversions=2,
                    gross_conversions_total=2.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=250.0,
                    net_revenue_total=200.0,
                    click_through_conversions_total=1.0,
                    view_through_conversions_total=0.0,
                    mixed_path_conversions_total=0.0,
                    created_at=datetime(2024, 2, 10, 0, 0),
                    updated_at=datetime(2024, 2, 10, 0, 0),
                ),
                ChannelPerformanceDaily(
                    date=datetime(2024, 2, 8).date(),
                    channel="google_ads",
                    conversion_key=None,
                    visits_total=4,
                    count_conversions=1,
                    gross_conversions_total=1.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=100.0,
                    net_revenue_total=100.0,
                    click_through_conversions_total=1.0,
                    view_through_conversions_total=0.0,
                    mixed_path_conversions_total=0.0,
                    created_at=datetime(2024, 2, 8, 0, 0),
                    updated_at=datetime(2024, 2, 8, 0, 0),
                ),
            ]
        )
        db.commit()

        out = get_overview_summary(
            db,
            date_from="2024-02-10",
            date_to="2024-02-15",
            expenses={},
        )

        tiles = {tile["kpi_key"]: tile for tile in out["kpi_tiles"]}
        assert tiles["visits"]["value"] == 5
        assert tiles["conversions"]["value"] == 2
        assert tiles["revenue"]["value"] == 250.0
        assert tiles["net_revenue"]["value"] == 200.0
    finally:
        db.close()


def test_overview_summary_prefers_silver_facts_when_channel_and_daily_aggregates_absent(monkeypatch):
    db = _unit_db_session()
    try:
        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-1"},
                    "touchpoints": [
                        {"channel": "google_ads", "interaction_type": "click", "ts": "2024-02-11T10:00:00Z"},
                    ],
                    "conversions": [
                        {
                            "id": "conv-current",
                            "name": "purchase",
                            "ts": "2024-02-11T12:00:00Z",
                            "value": 200.0,
                            "status": "partially_refunded",
                            "adjustments": [{"type": "refund", "value": 50.0, "currency": "EUR"}],
                        }
                    ],
                },
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-2"},
                    "touchpoints": [
                        {"channel": "meta_ads", "interaction_type": "impression", "ts": "2024-02-08T10:00:00Z"},
                    ],
                    "conversions": [
                        {
                            "id": "conv-prev",
                            "name": "purchase",
                            "ts": "2024-02-08T12:00:00Z",
                            "value": 90.0,
                        }
                    ],
                },
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="silver-summary-batch",
        )
        assert inserted == 2

        monkeypatch.setattr(overview, "_single_active_overview_definition_id", lambda _db: None)

        original_iter = overview._iter_conversion_path_rows

        def _fail_raw_rows(*args, **kwargs):
            raise AssertionError("raw conversion path fallback should not run when silver facts exist")

        monkeypatch.setattr(overview, "_iter_conversion_path_rows", _fail_raw_rows)

        out = get_overview_summary(
            db,
            date_from="2024-02-10",
            date_to="2024-02-15",
            expenses={},
        )

        tiles = {tile["kpi_key"]: tile for tile in out["kpi_tiles"]}
        assert tiles["visits"]["value"] == 1
        assert tiles["conversions"]["value"] == 1
        assert tiles["revenue"]["value"] == 200.0
        assert tiles["net_conversions"]["value"] == 1.0
        assert tiles["net_revenue"]["value"] == 150.0
        assert out["outcomes"]["current"]["gross_value"] == 200.0
        assert out["outcomes"]["current"]["refunded_value"] == 50.0
        assert out["outcomes"]["current"]["click_through_conversions"] == 1.0
        assert out["outcomes"]["previous"]["gross_value"] == 90.0
        assert out["outcomes"]["previous"]["view_through_conversions"] == 1.0

        monkeypatch.setattr(overview, "_iter_conversion_path_rows", original_iter)
    finally:
        db.close()


def test_overview_drivers_prefers_daily_aggregates_for_campaign_rollups():
    db = _unit_db_session()
    try:
        definition = JourneyDefinition(
            id="def-drivers",
            name="Drivers Journey",
            conversion_kpi_id="purchase",
            lookback_window_days=30,
            mode_default="conversion_only",
            created_by="test",
            updated_by="test",
            is_archived=False,
            created_at=datetime(2024, 2, 1, 0, 0),
            updated_at=datetime(2024, 2, 1, 0, 0),
        )
        db.add(definition)
        db.add_all(
            [
                JourneyPathDaily(
                    date=datetime(2024, 2, 10).date(),
                    journey_definition_id="def-drivers",
                    path_hash="campaign-a",
                    path_steps=["Paid Landing", "Purchase / Lead Won (conversion)"],
                    path_length=2,
                    count_journeys=2,
                    count_conversions=2,
                    gross_conversions_total=2.0,
                    net_conversions_total=2.0,
                    gross_revenue_total=500.0,
                    net_revenue_total=450.0,
                    click_through_conversions_total=2.0,
                    view_through_conversions_total=0.0,
                    mixed_path_conversions_total=0.0,
                    channel_group="paid",
                    campaign_id="Spring",
                    created_at=datetime(2024, 2, 10, 0, 0),
                    updated_at=datetime(2024, 2, 10, 0, 0),
                ),
                JourneyPathDaily(
                    date=datetime(2024, 2, 8).date(),
                    journey_definition_id="def-drivers",
                    path_hash="campaign-prev",
                    path_steps=["Paid Landing", "Purchase / Lead Won (conversion)"],
                    path_length=2,
                    count_journeys=1,
                    count_conversions=1,
                    gross_conversions_total=1.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=100.0,
                    net_revenue_total=100.0,
                    click_through_conversions_total=1.0,
                    view_through_conversions_total=0.0,
                    mixed_path_conversions_total=0.0,
                    channel_group="paid",
                    campaign_id="Spring",
                    created_at=datetime(2024, 2, 8, 0, 0),
                    updated_at=datetime(2024, 2, 8, 0, 0),
                ),
            ]
        )
        db.commit()

        out = get_overview_drivers(
            db,
            date_from="2024-02-10",
            date_to="2024-02-15",
            expenses=[],
            top_campaigns_n=5,
            conversion_key="purchase",
        )

        assert out["by_campaign"][0]["campaign"] == "Spring"
        assert out["by_campaign"][0]["revenue"] == 500.0
        assert out["by_campaign"][0]["conversions"] == 2
        assert out["by_campaign"][0]["delta_revenue_pct"] == 400.0
        assert out["by_campaign"][0]["outcomes"]["net_value"] == 450.0
    finally:
        db.close()


def test_overview_drivers_prefers_channel_facts_for_by_channel():
    db = _unit_db_session()
    try:
        db.add_all(
            [
                ChannelPerformanceDaily(
                    date=datetime(2024, 2, 10).date(),
                    channel="google_ads",
                    conversion_key=None,
                    visits_total=5,
                    count_conversions=2,
                    gross_conversions_total=2.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=250.0,
                    net_revenue_total=200.0,
                    click_through_conversions_total=1.0,
                    view_through_conversions_total=0.0,
                    mixed_path_conversions_total=0.0,
                    created_at=datetime(2024, 2, 10, 0, 0),
                    updated_at=datetime(2024, 2, 10, 0, 0),
                ),
                ChannelPerformanceDaily(
                    date=datetime(2024, 2, 8).date(),
                    channel="google_ads",
                    conversion_key=None,
                    visits_total=4,
                    count_conversions=1,
                    gross_conversions_total=1.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=100.0,
                    net_revenue_total=100.0,
                    click_through_conversions_total=1.0,
                    view_through_conversions_total=0.0,
                    mixed_path_conversions_total=0.0,
                    created_at=datetime(2024, 2, 8, 0, 0),
                    updated_at=datetime(2024, 2, 8, 0, 0),
                ),
            ]
        )
        db.commit()

        out = get_overview_drivers(
            db,
            date_from="2024-02-10",
            date_to="2024-02-15",
            expenses=[],
            top_campaigns_n=5,
        )

        by_channel = {row["channel"]: row for row in out["by_channel"]}
        assert by_channel["google_ads"]["visits"] == 5
        assert by_channel["google_ads"]["conversions"] == 2
        assert by_channel["google_ads"]["revenue"] == 250.0
        assert by_channel["google_ads"]["outcomes"]["net_value"] == 200.0
    finally:
        db.close()


def test_overview_trend_insights_prefers_silver_facts_when_channel_daily_facts_absent(monkeypatch):
    db = _unit_db_session()
    try:
        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-prev"},
                    "touchpoints": [{"channel": "paid_social", "interaction_type": "click", "ts": "2024-02-05T10:00:00Z"}],
                    "conversions": [{"id": "conv-prev", "name": "purchase", "ts": "2024-02-05T12:00:00Z", "value": 100.0}],
                },
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-current"},
                    "touchpoints": [{"channel": "email", "interaction_type": "click", "ts": "2024-02-12T10:00:00Z"}],
                    "conversions": [{"id": "conv-current", "name": "purchase", "ts": "2024-02-12T12:00:00Z", "value": 200.0}],
                },
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="silver-trends-batch",
        )
        assert inserted == 2

        def _fail_raw_rows(*args, **kwargs):
            raise AssertionError("raw conversion path fallback should not run when silver facts exist")

        monkeypatch.setattr(overview, "_iter_conversion_path_rows", _fail_raw_rows)

        out = get_overview_trend_insights(
            db,
            date_from="2024-02-08",
            date_to="2024-02-14",
        )

        assert out["decomposition"]["current"]["revenue"] == 200.0
        assert out["decomposition"]["previous"]["revenue"] == 100.0
        assert out["decomposition"]["current"]["conversions"] == 1.0
        assert out["decomposition"]["current"]["visits"] == 1.0
        assert out["momentum"]["rising"][0]["channel"] == "email"
        assert any(row["channel"] == "email" for row in out["mix_shift"])
    finally:
        db.close()


def test_overview_drivers_uses_current_silver_campaign_rollups_when_previous_period_is_empty():
    db = _unit_db_session()
    try:
        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-current"},
                    "touchpoints": [
                        {
                            "channel": "paid_social",
                            "campaign": "Launch",
                            "interaction_type": "click",
                            "ts": "2024-02-12T10:00:00Z",
                        }
                    ],
                    "conversions": [
                        {
                            "id": "conv-current",
                            "name": "purchase",
                            "ts": "2024-02-12T12:00:00Z",
                            "value": 200.0,
                        }
                    ],
                },
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="silver-campaign-drivers-batch",
        )
        assert inserted == 1

        out = get_overview_drivers(
            db,
            date_from="2024-02-12",
            date_to="2024-02-12",
            expenses=[],
            top_campaigns_n=5,
        )

        assert out["by_campaign"]
        assert out["by_campaign"][0]["campaign"] == "Launch"
        assert out["by_campaign"][0]["revenue"] == 200.0
        assert out["by_campaign"][0]["conversions"] == 1
    finally:
        db.close()


def test_overview_drivers_prefers_silver_facts_for_by_channel_when_channel_daily_facts_absent(monkeypatch):
    db = _unit_db_session()
    try:
        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-prev"},
                    "touchpoints": [{"channel": "paid_social", "campaign": "Winter", "interaction_type": "click", "ts": "2024-02-05T10:00:00Z"}],
                    "conversions": [{"id": "conv-prev", "name": "purchase", "ts": "2024-02-05T12:00:00Z", "value": 100.0}],
                },
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-current"},
                    "touchpoints": [{"channel": "email", "campaign": "Spring", "interaction_type": "click", "ts": "2024-02-12T10:00:00Z"}],
                    "conversions": [{"id": "conv-current", "name": "purchase", "ts": "2024-02-12T12:00:00Z", "value": 200.0}],
                },
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="silver-drivers-batch",
        )
        assert inserted == 2

        def _fail_raw_rows(*args, **kwargs):
            raise AssertionError("raw conversion path fallback should not run when silver facts exist")

        monkeypatch.setattr(overview, "_iter_conversion_path_rows", _fail_raw_rows)

        out = get_overview_drivers(
            db,
            date_from="2024-02-08",
            date_to="2024-02-14",
            expenses=[],
            top_campaigns_n=5,
        )

        by_channel = {row["channel"]: row for row in out["by_channel"]}
        assert by_channel["email"]["visits"] == 1
        assert by_channel["email"]["conversions"] == 1
        assert by_channel["email"]["revenue"] == 200.0
        assert by_channel["email"]["outcomes"]["gross_value"] == 200.0
        assert by_channel["email"]["delta_revenue_pct"] == 100.0
        assert out["by_campaign"][0]["campaign"] == "Spring"
        assert out["by_campaign"][0]["revenue"] == 200.0
    finally:
        db.close()
