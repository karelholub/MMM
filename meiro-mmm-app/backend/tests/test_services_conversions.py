from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import ConversionDataQualityFact, ConversionPath, ConversionScopeDiagnosticFact
from app.services_conversions import (
    classify_journey_interaction,
    conversion_path_is_converted,
    conversion_path_payload,
    conversion_path_revenue_value,
    conversion_path_touchpoints,
    filter_journeys_by_quality,
    filter_journeys_by_windows,
    persist_journeys_as_conversion_paths,
    v2_to_legacy,
)


def _make_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)()


def test_filter_journeys_by_quality_uses_ingest_quality_score():
    journeys = [
        {"customer_id": "high", "quality_score": 82},
        {"customer_id": "low", "quality_score": 41},
    ]

    filtered = filter_journeys_by_quality(journeys, min_quality_score=50)

    assert [j["customer_id"] for j in filtered] == ["high"]


def test_v2_to_legacy_preserves_quality_metadata():
    journey = {
        "_schema": "v2",
        "customer": {"id": "cust-1"},
        "touchpoints": [
            {
                "channel": "paid",
                "ts": "2026-03-01T00:00:00Z",
                "source": "google",
                "medium": "cpc",
                "campaign": {"name": "brand"},
                "utm": {"source": "google", "medium": "cpc", "campaign": "brand"},
            }
        ],
        "conversions": [{"name": "purchase", "value": 120.0}],
        "meta": {"quality": {"score": 73, "band": "medium", "drivers": ["unknown_channel"]}},
    }

    legacy = v2_to_legacy(journey)

    assert legacy["customer_id"] == "cust-1"
    assert legacy["quality_score"] == 73
    assert legacy["quality_band"] == "medium"
    assert legacy["meta"]["quality"]["drivers"] == ["unknown_channel"]
    assert legacy["touchpoints"][0]["source"] == "google"
    assert legacy["touchpoints"][0]["medium"] == "cpc"
    assert legacy["touchpoints"][0]["utm_source"] == "google"
    assert legacy["touchpoints"][0]["utm_medium"] == "cpc"
    assert legacy["touchpoints"][0]["utm_campaign"] == "brand"
    assert legacy["touchpoints"][0]["utm"] == {"source": "google", "medium": "cpc", "campaign": "brand"}


def test_v2_to_legacy_preserves_interaction_type_and_outcome_summary():
    journey = {
        "_schema": "v2",
        "customer": {"id": "cust-1"},
        "touchpoints": [
            {"channel": "google_ads", "ts": "2026-03-01T00:00:00Z", "interaction_type": "impression"},
            {"channel": "google_ads", "ts": "2026-03-02T00:00:00Z", "interaction_type": "click"},
        ],
        "conversions": [
            {
                "id": "ord-1",
                "name": "purchase",
                "value": 120.0,
                "status": "partially_refunded",
                "adjustments": [{"id": "adj-1", "type": "refund", "value": 20.0, "currency": "EUR"}],
            }
        ],
    }

    legacy = v2_to_legacy(journey)

    assert legacy["touchpoints"][0]["interaction_type"] == "impression"
    assert legacy["touchpoints"][1]["interaction_type"] == "click"
    assert legacy["interaction_path_type"] == "mixed_path"
    assert legacy["conversion_outcome"]["gross_conversions"] == 1.0


def test_filter_journeys_by_windows_supports_click_only_and_view_through_modes():
    journeys = [
        {
            "customer_id": "view-only",
            "touchpoints": [
                {"channel": "meta_ads", "timestamp": "2026-03-01T00:00:00Z", "interaction_type": "impression"},
            ],
            "converted": True,
            "conversion_value": 10.0,
        },
        {
            "customer_id": "mixed",
            "touchpoints": [
                {"channel": "google_ads", "timestamp": "2026-03-01T00:00:00Z", "interaction_type": "impression", "campaign": "Brand"},
                {"channel": "google_ads", "timestamp": "2026-03-02T00:00:00Z", "interaction_type": "click", "campaign": "Brand"},
            ],
            "converted": True,
            "conversion_value": 10.0,
        },
    ]

    click_only = filter_journeys_by_windows(
        journeys,
        {"windows": {"click_lookback_days": 30, "impression_lookback_days": 7}, "attribution": {"interaction_mode": "click_only"}},
    )
    assert [journey["customer_id"] for journey in click_only] == ["mixed"]
    assert all(tp["interaction_type"] == "click" for tp in click_only[0]["touchpoints"])

    click_preferred = filter_journeys_by_windows(
        journeys,
        {"windows": {"click_lookback_days": 30, "impression_lookback_days": 7}, "attribution": {"interaction_mode": "click_preferred", "include_impression_only_paths": True}},
    )
    assert classify_journey_interaction(click_preferred[0]) == "view_through"
    assert classify_journey_interaction(click_preferred[1]) == "click_through"


def test_persist_journeys_as_conversion_paths_stamps_import_metadata():
    db = _make_session()
    inserted = persist_journeys_as_conversion_paths(
        db,
        [
            {
                "_schema": "v2",
                "journey_id": "journey-1",
                "customer": {"id": "cust-1"},
                "touchpoints": [{"channel": "google_ads", "ts": "2026-03-01T00:00:00Z"}],
                "conversions": [{"id": "conv-1", "ts": "2026-03-01T01:00:00Z", "name": "purchase", "value": 10.0}],
            }
        ],
        replace=True,
        import_source="meiro_webhook",
        import_batch_id="batch-123",
        source_snapshot_id="snapshot-123",
    )

    row = db.query(ConversionPath).one()

    assert inserted == 1
    assert row.import_source == "meiro_webhook"
    assert row.import_batch_id == "batch-123"
    assert row.source_snapshot_id == "snapshot-123"
    assert row.imported_at is not None
    facts = db.query(ConversionScopeDiagnosticFact).filter(ConversionScopeDiagnosticFact.conversion_id == row.conversion_id).all()
    assert facts
    assert {fact.scope_type for fact in facts} == {"channel", "campaign"}
    dq_fact = db.query(ConversionDataQualityFact).filter(ConversionDataQualityFact.conversion_id == row.conversion_id).one()
    assert dq_fact.touchpoint_count == 1
    assert dq_fact.missing_profile is False


def test_persist_journeys_as_conversion_paths_append_mode_skips_existing_conversion_ids():
    db = _make_session()
    persist_journeys_as_conversion_paths(
        db,
        [
            {
                "customer_id": "cust-1",
                "conversion_id": "conv-1",
                "kpi_type": "purchase",
                "touchpoints": [{"channel": "google_ads", "timestamp": "2026-03-01T00:00:00Z"}],
                "converted": True,
                "conversion_value": 10.0,
            }
        ],
        replace=True,
        import_source="upload",
    )

    inserted = persist_journeys_as_conversion_paths(
        db,
        [
            {
                "customer_id": "cust-1",
                "conversion_id": "conv-1",
                "kpi_type": "purchase",
                "touchpoints": [{"channel": "google_ads", "timestamp": "2026-03-01T00:00:00Z"}],
                "converted": True,
                "conversion_value": 10.0,
            },
            {
                "customer_id": "cust-2",
                "conversion_id": "conv-2",
                "kpi_type": "purchase",
                "touchpoints": [{"channel": "meta_ads", "timestamp": "2026-03-02T00:00:00Z"}],
                "converted": True,
                "conversion_value": 25.0,
            },
        ],
        replace=False,
        import_source="meiro_events_replay",
        import_batch_id="batch-append",
    )

    rows = db.query(ConversionPath).order_by(ConversionPath.conversion_id.asc()).all()

    assert inserted == 1
    assert [row.conversion_id for row in rows] == ["conv-1", "conv-2"]
    assert rows[0].import_source == "upload"
    assert rows[1].import_source == "meiro_events_replay"
    assert rows[1].import_batch_id == "batch-append"


def test_persist_journeys_as_conversion_paths_replaces_only_selected_profiles():
    db = _make_session()
    persist_journeys_as_conversion_paths(
        db,
        [
            {
                "customer_id": "cust-1",
                "conversion_id": "conv-1",
                "kpi_type": "purchase",
                "touchpoints": [{"channel": "google_ads", "timestamp": "2026-03-01T00:00:00Z"}],
                "converted": True,
                "conversion_value": 10.0,
            },
            {
                "customer_id": "cust-2",
                "conversion_id": "conv-2",
                "kpi_type": "purchase",
                "touchpoints": [{"channel": "meta_ads", "timestamp": "2026-03-02T00:00:00Z"}],
                "converted": True,
                "conversion_value": 25.0,
            },
        ],
        replace=True,
        import_source="upload",
    )

    inserted = persist_journeys_as_conversion_paths(
        db,
        [],
        replace=True,
        replace_profile_ids=["cust-1"],
        import_source="meiro_events_replay",
        import_batch_id="batch-replace-profile",
    )

    rows = db.query(ConversionPath).order_by(ConversionPath.profile_id.asc()).all()

    assert inserted == 0
    assert [row.profile_id for row in rows] == ["cust-2"]


def test_conversion_path_helpers_normalize_payload_touchpoints_and_conversion_state():
    row = ConversionPath(
        conversion_id="conv-1",
        profile_id="cust-1",
        conversion_key=None,
        path_json={
            "converted": True,
            "touchpoints": [
                {"channel": "google_ads", "timestamp": "2026-03-01T00:00:00Z"},
                "bad-row",
            ],
        },
    )

    assert conversion_path_payload(row)["converted"] is True
    assert conversion_path_touchpoints(row) == [{"channel": "google_ads", "timestamp": "2026-03-01T00:00:00Z"}]
    assert conversion_path_is_converted(row) is True


def test_conversion_path_revenue_value_falls_back_to_legacy_conversion_value():
    row = ConversionPath(
        conversion_id="conv-legacy",
        profile_id="cust-1",
        conversion_key="purchase",
        path_json={
            "converted": True,
            "conversion_value": 42.5,
            "touchpoints": [{"channel": "direct", "timestamp": "2026-03-01T00:00:00Z"}],
        },
    )

    assert conversion_path_revenue_value(row, revenue_config={"mode": "sum"}) == 42.5


def test_conversion_path_revenue_value_falls_back_to_first_conversion_value():
    row = ConversionPath(
        conversion_id="conv-list",
        profile_id="cust-1",
        conversion_key="purchase",
        path_json={
            "conversions": [{"name": "purchase", "value": 123.45}],
            "touchpoints": [{"channel": "direct", "timestamp": "2026-03-01T00:00:00Z"}],
        },
    )

    assert conversion_path_revenue_value(row, revenue_config={"mode": "sum"}) == 123.45
