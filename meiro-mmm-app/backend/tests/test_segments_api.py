from fastapi.testclient import TestClient
import pytest
from datetime import date, datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app.connectors import meiro_cdp
from app.models_config_dq import (
    ConversionScopeDiagnosticFact,
    JourneyDefinitionInstanceFact,
    JourneyRoleFact,
    MeiroEventProfileState,
    MeiroProfileFact,
)


@pytest.fixture
def client():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)

    def override_get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides.clear()
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()
    engine.dispose()


def test_local_segment_crud_and_registry(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(meiro_cdp, "is_connected", lambda: True)
    monkeypatch.setattr(
        meiro_cdp,
        "list_segments",
        lambda: [{"id": "seg-1", "name": "VIP buyers", "profiles_count": 42}],
    )

    headers_view = {"X-User-Role": "viewer", "X-User-Id": "qa-viewer"}
    headers_edit = {"X-User-Role": "editor", "X-User-Id": "qa-editor"}

    create_resp = client.post(
        "/api/segments/local",
        headers=headers_edit,
        json={
            "name": "Mobile CZ paid",
            "description": "Local analytical segment",
            "definition": {
                "channel_group": "paid_search",
                "device": "mobile",
                "country": "cz",
                "ignored_key": "x",
            },
        },
    )
    assert create_resp.status_code == 200
    created = create_resp.json()
    assert created["name"] == "Mobile CZ paid"
    assert created["source"] == "local_analytical"
    assert created["definition"]["version"] == "v2"
    assert created["definition"]["match"] == "all"
    assert {rule["field"] for rule in created["definition"]["rules"]} == {
        "channel_group",
        "device",
        "country",
    }
    assert created["compatibility"]["auto_filter_compatible"] is True
    segment_id = created["id"]

    registry_resp = client.get("/api/segments/registry", headers=headers_view)
    assert registry_resp.status_code == 200
    registry = registry_resp.json()
    assert registry["summary"]["local_analytical"] == 1
    assert registry["summary"]["meiro_pipes"] == 1
    assert any(item["id"] == segment_id for item in registry["items"])
    assert any(item["source"] == "meiro_pipes" for item in registry["items"])

    update_resp = client.put(
        f"/api/segments/local/{segment_id}",
        headers=headers_edit,
        json={
            "name": "Desktop CZ paid",
            "description": "Updated segment",
            "definition": {
                "channel_group": "paid_search",
                "device": "desktop",
                "country": "cz",
            },
        },
    )
    assert update_resp.status_code == 200
    assert any(
        rule["field"] == "device" and rule["value"] == "desktop"
        for rule in update_resp.json()["definition"]["rules"]
    )

    archive_resp = client.post(f"/api/segments/local/{segment_id}/archive", headers=headers_edit)
    assert archive_resp.status_code == 200
    assert archive_resp.json()["status"] == "archived"

    active_list = client.get("/api/segments/local", headers=headers_view)
    assert active_list.status_code == 200
    assert active_list.json()["items"] == []

    all_list = client.get("/api/segments/local?include_archived=true", headers=headers_view)
    assert all_list.status_code == 200
    assert len(all_list.json()["items"]) == 1

    restore_resp = client.post(f"/api/segments/local/{segment_id}/restore", headers=headers_edit)
    assert restore_resp.status_code == 200
    assert restore_resp.json()["status"] == "active"


def test_segment_context_uses_observed_workspace_values(client: TestClient):
    db_gen = app.dependency_overrides[get_db]()
    db = next(db_gen)
    try:
        db.add_all(
            [
                JourneyDefinitionInstanceFact(
                    date=date(2026, 4, 4),
                    journey_definition_id="def-1",
                    conversion_id="conv-1",
                    profile_id="p-1",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 4, 10, 0, tzinfo=timezone.utc),
                    path_hash="h-1",
                    steps_json=["paid_search", "checkout"],
                    path_length=2,
                    channel_group="paid_search",
                    last_touch_channel="google_ads",
                    campaign_id="brand_search",
                    device="mobile",
                    country="cz",
                ),
                JourneyDefinitionInstanceFact(
                    date=date(2026, 4, 5),
                    journey_definition_id="def-2",
                    conversion_id="conv-2",
                    profile_id="p-2",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 5, 10, 0, tzinfo=timezone.utc),
                    path_hash="h-2",
                    steps_json=["email", "checkout"],
                    path_length=2,
                    channel_group="email",
                    last_touch_channel="email",
                    campaign_id="promo_april",
                    device="desktop",
                    country="de",
                ),
            ]
        )
        db.commit()
    finally:
        db.close()
        try:
            next(db_gen)
        except StopIteration:
            pass

    response = client.get("/api/segments/context", headers={"X-User-Role": "viewer", "X-User-Id": "qa-viewer"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["journey_rows"] == 2
    assert payload["summary"]["date_from"] == "2026-04-04"
    assert payload["summary"]["date_to"] == "2026-04-05"
    assert [item["value"] for item in payload["channels"]] == ["email", "paid_search"] or [item["value"] for item in payload["channels"]] == ["paid_search", "email"]
    assert {item["value"] for item in payload["campaigns"]} == {"brand_search", "promo_april"}
    assert {item["value"] for item in payload["devices"]} == {"mobile", "desktop"}
    assert {item["value"] for item in payload["countries"]} == {"cz", "de"}
    assert {item["value"] for item in payload["conversion_keys"]} == {"purchase"}
    assert {item["value"] for item in payload["last_touch_channels"]} == {"google_ads", "email"}
    assert payload["suggested_rules"]["lag_days"][0]["field"] == "lag_days"


def test_local_segment_overlap_analysis(client: TestClient):
    db_gen = app.dependency_overrides[get_db]()
    db = next(db_gen)
    try:
        db.add_all(
            [
                JourneyDefinitionInstanceFact(
                    date=date(2026, 4, 4),
                    journey_definition_id="def-1",
                    conversion_id="conv-1",
                    profile_id="p-1",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 4, 10, 0, tzinfo=timezone.utc),
                    path_hash="h-1",
                    steps_json=["paid_search", "checkout"],
                    path_length=2,
                    channel_group="paid_search",
                    last_touch_channel="google_ads",
                    campaign_id="brand_search",
                    device="mobile",
                    country="cz",
                    net_revenue_total=100,
                    net_conversions_total=1,
                ),
                JourneyDefinitionInstanceFact(
                    date=date(2026, 4, 5),
                    journey_definition_id="def-1",
                    conversion_id="conv-2",
                    profile_id="p-2",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 5, 10, 0, tzinfo=timezone.utc),
                    path_hash="h-2",
                    steps_json=["paid_search", "checkout"],
                    path_length=2,
                    channel_group="paid_search",
                    last_touch_channel="google_ads",
                    campaign_id="brand_search",
                    device="desktop",
                    country="cz",
                    net_revenue_total=120,
                    net_conversions_total=1,
                ),
                JourneyDefinitionInstanceFact(
                    date=date(2026, 4, 6),
                    journey_definition_id="def-1",
                    conversion_id="conv-3",
                    profile_id="p-3",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 6, 10, 0, tzinfo=timezone.utc),
                    path_hash="h-3",
                    steps_json=["email", "checkout"],
                    path_length=2,
                    channel_group="email",
                    last_touch_channel="email",
                    campaign_id="promo_april",
                    device="mobile",
                    country="de",
                    net_revenue_total=80,
                    net_conversions_total=1,
                ),
            ]
        )
        db.commit()
    finally:
        db.close()
        try:
            next(db_gen)
        except StopIteration:
            pass

    headers_view = {"X-User-Role": "viewer", "X-User-Id": "qa-viewer"}
    headers_edit = {"X-User-Role": "editor", "X-User-Id": "qa-editor"}

    primary_resp = client.post(
        "/api/segments/local",
        headers=headers_edit,
        json={
            "name": "CZ traffic",
            "description": "All CZ journeys",
            "definition": {
                "match": "all",
                "rules": [{"field": "country", "op": "eq", "value": "cz"}],
            },
        },
    )
    assert primary_resp.status_code == 200
    primary_id = primary_resp.json()["id"]

    overlapping_resp = client.post(
        "/api/segments/local",
        headers=headers_edit,
        json={
            "name": "Paid search",
            "description": "Paid-search journeys",
            "definition": {
                "match": "all",
                "rules": [{"field": "channel_group", "op": "eq", "value": "paid_search"}],
            },
        },
    )
    assert overlapping_resp.status_code == 200

    distinct_resp = client.post(
        "/api/segments/local",
        headers=headers_edit,
        json={
            "name": "DE traffic",
            "description": "DE journeys",
            "definition": {
                "match": "all",
                "rules": [{"field": "country", "op": "eq", "value": "de"}],
            },
        },
    )
    assert distinct_resp.status_code == 200

    response = client.get(f"/api/segments/local/{primary_id}/overlap", headers=headers_view)
    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["journey_rows"] == 2
    assert payload["summary"]["compared_segments"] == 2
    top = payload["items"][0]
    assert top["segment"]["name"] == "Paid search"
    assert top["overlap_rows"] == 2
    assert top["relationship"] in {"near_duplicate", "mostly_contained_in_other", "mostly_contains_other"}


def test_local_segment_compare_analysis(client: TestClient):
    db_gen = app.dependency_overrides[get_db]()
    db = next(db_gen)
    try:
        db.add_all(
            [
                JourneyDefinitionInstanceFact(
                    date=date(2026, 4, 4),
                    journey_definition_id="def-1",
                    conversion_id="conv-1",
                    profile_id="p-1",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 4, 10, 0, tzinfo=timezone.utc),
                    path_hash="h-1",
                    steps_json=["paid_search", "checkout"],
                    path_length=2,
                    channel_group="paid_search",
                    last_touch_channel="google_ads",
                    campaign_id="brand_search",
                    device="mobile",
                    country="cz",
                    net_revenue_total=100,
                    net_conversions_total=1,
                ),
                JourneyDefinitionInstanceFact(
                    date=date(2026, 4, 5),
                    journey_definition_id="def-1",
                    conversion_id="conv-2",
                    profile_id="p-2",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 5, 10, 0, tzinfo=timezone.utc),
                    path_hash="h-2",
                    steps_json=["email", "checkout"],
                    path_length=3,
                    channel_group="email",
                    last_touch_channel="email",
                    campaign_id="promo_april",
                    device="desktop",
                    country="de",
                    net_revenue_total=80,
                    net_conversions_total=1,
                    time_to_convert_sec=172800,
                ),
            ]
        )
        db.commit()
    finally:
        db.close()
        try:
            next(db_gen)
        except StopIteration:
            pass

    headers_view = {"X-User-Role": "viewer", "X-User-Id": "qa-viewer"}
    headers_edit = {"X-User-Role": "editor", "X-User-Id": "qa-editor"}

    left = client.post(
        "/api/segments/local",
        headers=headers_edit,
        json={
            "name": "Paid search journeys",
            "description": "Left side",
            "definition": {"rules": [{"field": "channel_group", "op": "eq", "value": "paid_search"}]},
        },
    )
    right = client.post(
        "/api/segments/local",
        headers=headers_edit,
        json={
            "name": "Long lag journeys",
            "description": "Right side",
            "definition": {"rules": [{"field": "lag_days", "op": "gte", "value": 1}]},
        },
    )
    assert left.status_code == 200
    assert right.status_code == 200

    response = client.get(
        f"/api/segments/local/{left.json()['id']}/compare?other_segment_id={right.json()['id']}",
        headers=headers_view,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["primary_segment"]["name"] == "Paid search journeys"
    assert payload["other_segment"]["name"] == "Long lag journeys"
    assert payload["primary_summary"]["journey_rows"] == 1
    assert payload["other_summary"]["journey_rows"] == 1
    assert payload["overlap"]["relationship"] == "distinct"
    assert payload["deltas"]["revenue"] == 20.0


def test_segment_registry_includes_webhook_derived_meiro_segments(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(meiro_cdp, "is_connected", lambda: False)

    db_gen = app.dependency_overrides[get_db]()
    db = next(db_gen)
    try:
        db.add(
            MeiroProfileFact(
                profile_id="cust-profile-1",
                profile_json={
                    "customer_id": "cust-profile-1",
                    "attributes": {
                        "segments": [
                            {"id": "vip", "name": "VIP buyers"},
                        ]
                    },
                },
            )
        )
        db.add(
            MeiroEventProfileState(
                profile_id="cust-profile-2",
                profile_json={
                    "customer_id": "cust-profile-2",
                    "segments": [
                        {"id": "vip", "name": "VIP buyers"},
                        {"id": "long_lag", "name": "Long-lag converters"},
                    ],
                },
            )
        )
        db.commit()
    finally:
        db.close()
        try:
            next(db_gen)
        except StopIteration:
            pass

    response = client.get("/api/segments/registry", headers={"X-User-Role": "viewer", "X-User-Id": "qa-viewer"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["meiro_pipes"] == 2
    vip = next(item for item in payload["items"] if item["id"] == "meiro:vip")
    assert vip["name"] == "VIP buyers"
    assert vip["size"] == 2
    assert vip["definition"]["derived_from"] == "webhook_payload"
    assert set(vip["definition"]["ingestion_sources"]) == {"profiles_webhook", "raw_events_replay"}
    long_lag = next(item for item in payload["items"] if item["id"] == "meiro:long_lag")
    assert long_lag["size"] == 1


def test_smart_segment_v2_preview_and_compatibility(client: TestClient):
    db_gen = app.dependency_overrides[get_db]()
    db = next(db_gen)
    try:
        db.add_all(
            [
                JourneyDefinitionInstanceFact(
                    date=date(2026, 4, 4),
                    journey_definition_id="def-1",
                    conversion_id="conv-1",
                    profile_id="p-1",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 4, 10, 0, tzinfo=timezone.utc),
                    path_hash="h-1",
                    steps_json=["paid_search", "email", "checkout"],
                    path_length=3,
                    channel_group="paid_search",
                    last_touch_channel="email",
                    campaign_id="brand_search",
                    device="mobile",
                    country="cz",
                    interaction_path_type="multi_touch",
                    time_to_convert_sec=4 * 86400,
                    net_conversions_total=1,
                    net_revenue_total=120,
                ),
                JourneyDefinitionInstanceFact(
                    date=date(2026, 4, 5),
                    journey_definition_id="def-1",
                    conversion_id="conv-2",
                    profile_id="p-2",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 5, 10, 0, tzinfo=timezone.utc),
                    path_hash="h-2",
                    steps_json=["direct", "checkout"],
                    path_length=2,
                    channel_group="direct",
                    last_touch_channel="direct",
                    campaign_id="brand_home",
                    device="desktop",
                    country="de",
                    interaction_path_type="short_path",
                    time_to_convert_sec=0.5 * 86400,
                    net_conversions_total=1,
                    net_revenue_total=40,
                ),
            ]
        )
        db.commit()
    finally:
        db.close()
        try:
            next(db_gen)
        except StopIteration:
            pass

    response = client.post(
        "/api/segments/local",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
        json={
            "name": "Long-lag high-value journeys",
            "description": "Smart analytical segment",
            "definition": {
                "version": "v2",
                "match": "all",
                "rules": [
                    {"field": "lag_days", "op": "gte", "value": 3},
                    {"field": "net_revenue_total", "op": "gte", "value": 100},
                    {"field": "country", "op": "eq", "value": "cz"},
                ],
            },
        },
    )
    assert response.status_code == 200
    created = response.json()
    registry_response = client.get("/api/segments/registry", headers={"X-User-Role": "viewer", "X-User-Id": "qa-viewer"})
    assert registry_response.status_code == 200
    payload = next(item for item in registry_response.json()["items"] if item["id"] == created["id"])
    assert payload["segment_family"] == "lag_timing"
    assert payload["compatibility"]["auto_filter_compatible"] is False
    assert payload["compatibility"]["advanced"] is True
    assert payload["preview"]["journey_rows"] == 1
    assert payload["preview"]["profiles"] == 1
    assert payload["preview"]["conversions"] == 1.0
    assert payload["preview"]["revenue"] == 120.0
    assert payload["preview"]["median_lag_days"] == 4.0
    assert "Lag (days) >=" in payload["criteria_label"]


def test_local_segment_analysis_endpoint_returns_distributions_and_roles(client: TestClient):
    db_gen = app.dependency_overrides[get_db]()
    db = next(db_gen)
    try:
        db.add_all(
            [
                JourneyDefinitionInstanceFact(
                    date=date(2026, 4, 4),
                    journey_definition_id="def-1",
                    conversion_id="conv-1",
                    profile_id="p-1",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 4, 10, 0, tzinfo=timezone.utc),
                    path_hash="h-1",
                    steps_json=["paid_search", "email", "checkout"],
                    path_length=3,
                    channel_group="paid_search",
                    last_touch_channel="email",
                    campaign_id="brand_search",
                    device="mobile",
                    country="cz",
                    interaction_path_type="multi_touch",
                    time_to_convert_sec=2 * 86400,
                    net_conversions_total=1,
                    net_revenue_total=80,
                ),
                JourneyRoleFact(
                    conversion_id="conv-1",
                    profile_id="p-1",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 4, 10, 0, tzinfo=timezone.utc),
                    role_key="first",
                    ordinal=0,
                    channel_group="paid_search",
                    channel="google_ads",
                    campaign="brand_search",
                    net_conversions_total=1,
                    net_revenue_total=80,
                ),
                JourneyRoleFact(
                    conversion_id="conv-1",
                    profile_id="p-1",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 4, 10, 0, tzinfo=timezone.utc),
                    role_key="last",
                    ordinal=1,
                    channel_group="email",
                    channel="email",
                    campaign="newsletter",
                    net_conversions_total=1,
                    net_revenue_total=80,
                ),
                ConversionScopeDiagnosticFact(
                    conversion_id="conv-1",
                    profile_id="p-1",
                    conversion_key="purchase",
                    scope_type="channel",
                    scope_key="paid_search",
                    scope_channel="paid_search",
                    first_touch_ts=datetime(2026, 4, 2, 10, 0, tzinfo=timezone.utc),
                    last_touch_ts=datetime(2026, 4, 4, 10, 0, tzinfo=timezone.utc),
                    conversion_ts=datetime(2026, 4, 4, 10, 0, tzinfo=timezone.utc),
                    touch_journeys=1,
                    content_journeys=1,
                    checkout_journeys=1,
                    converted_journeys=1,
                    first_touch_conversions=1,
                    last_touch_conversions=0,
                    assist_conversions=0,
                    first_touch_revenue=80,
                    last_touch_revenue=0,
                    assist_revenue=0,
                ),
            ]
        )
        db.commit()
    finally:
        db.close()
        try:
            next(db_gen)
        except StopIteration:
            pass

    create_response = client.post(
        "/api/segments/local",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
        json={
            "name": "Paid search CZ",
            "description": "Focus paid search journeys",
            "definition": {
                "version": "v2",
                "match": "all",
                "rules": [
                    {"field": "channel_group", "op": "eq", "value": "paid_search"},
                    {"field": "country", "op": "eq", "value": "cz"},
                ],
            },
        },
    )
    assert create_response.status_code == 200
    segment_id = create_response.json()["id"]

    response = client.get(
        f"/api/segments/local/{segment_id}/analysis?date_from=2026-04-01&date_to=2026-04-06&journey_definition_id=def-1",
        headers={"X-User-Role": "viewer", "X-User-Id": "qa-viewer"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["journey_rows"] == 1
    assert payload["distributions"]["channels"][0]["value"] == "paid_search"
    assert payload["role_mix"]["first_touch_conversions"] == 1.0
    assert payload["role_entities"]["channels"][0]["id"] == "paid_search"
