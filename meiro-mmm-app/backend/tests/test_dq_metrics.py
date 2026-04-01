from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app import services_data_quality
from app.services_conversions import persist_journeys_as_conversion_paths
from app.services_quality import ConfidenceComponents, score_confidence
from app.utils.taxonomy import Taxonomy


def _make_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)()


def test_compute_journeys_completeness_basic_counts(monkeypatch):
    monkeypatch.setattr(services_data_quality, "load_taxonomy", Taxonomy.default)
    journeys = [
        {
            "customer_id": "c1",
            "touchpoints": [
                {"channel": "google", "timestamp": "2024-01-01T00:00:00"},
                {"channel": "meta", "timestamp": "2024-01-02T00:00:00"},
            ],
            "meta": {"parser": {"used_inferred_mapping": False}},
            "_revenue_entries": [
                {"default_applied": False, "raw_value_zero": False},
            ],
        },
        {
            # Missing customer_id should count towards missing_profile_pct
            "touchpoints": [{"channel": "unknown", "timestamp": ""}],
            "meta": {"parser": {"used_inferred_mapping": True}},
            "_revenue_entries": [
                {"default_applied": True, "raw_value_zero": False},
            ],
        },
        {
            "customer_id": "c1",  # duplicate id
            "touchpoints": [{"channel": "direct", "timestamp": "2024-01-03T00:00:00", "source": "mystery", "medium": "weird"}],
            "meta": {"parser": {"used_inferred_mapping": False}},
            "_revenue_entries": [
                {"default_applied": False, "raw_value_zero": True},
            ],
        },
    ]

    metrics = services_data_quality.compute_journeys_completeness(journeys)
    keys = {m[1] for m in metrics}

    assert "missing_profile_pct" in keys
    assert "missing_timestamp_pct" in keys
    assert "missing_channel_pct" in keys
    assert "duplicate_id_pct" in keys
    assert "conversion_attributable_pct" in keys
    assert "defaulted_conversion_value_pct" in keys
    assert "raw_zero_conversion_value_pct" in keys
    assert "unresolved_source_medium_touchpoint_pct" in keys
    assert "inferred_mapping_journey_pct" in keys

    missing_profile = next(v for (_, k, v, _) in metrics if k == "missing_profile_pct")
    duplicate_pct = next(v for (_, k, v, _) in metrics if k == "duplicate_id_pct")
    defaulted_pct = next(v for (_, k, v, _) in metrics if k == "defaulted_conversion_value_pct")
    raw_zero_pct = next(v for (_, k, v, _) in metrics if k == "raw_zero_conversion_value_pct")
    unresolved_pct = next(v for (_, k, v, _) in metrics if k == "unresolved_source_medium_touchpoint_pct")
    inferred_pct = next(v for (_, k, v, _) in metrics if k == "inferred_mapping_journey_pct")

    # One of three journeys is missing a profile id
    assert missing_profile > 0
    # Duplicate id present
    assert duplicate_pct > 0
    assert defaulted_pct > 0
    assert raw_zero_pct > 0
    assert unresolved_pct > 0
    assert inferred_pct > 0


def test_score_confidence_ranges_and_labels():
    high = ConfidenceComponents(
        match_rate=0.95,
        join_rate=0.9,
        missing_rate=0.05,
        freshness_lag_minutes=10.0,
        dedup_rate=0.95,
        consent_share=0.9,
    )
    score_high, label_high = score_confidence(high)
    assert 0.0 <= score_high <= 100.0
    assert label_high in {"high", "medium", "low"}
    assert label_high != "low"

    low = ConfidenceComponents(
        match_rate=0.2,
        join_rate=0.2,
        missing_rate=0.8,
        freshness_lag_minutes=24 * 60.0,  # very stale
        dedup_rate=0.2,
        consent_share=0.2,
    )
    score_low, label_low = score_confidence(low)
    assert 0.0 <= score_low <= 100.0
    assert label_low == "low"
    assert score_low < score_high


def test_compute_dq_snapshots_prefers_persisted_facts_when_taxonomy_disabled(monkeypatch):
    db = _make_session()
    try:
        monkeypatch.setattr(services_data_quality, "load_taxonomy", Taxonomy.default)
        persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "customer_id": "c1",
                    "conversion_id": "conv-1",
                    "kpi_type": "purchase",
                    "touchpoints": [
                        {"channel": "google", "timestamp": "2024-01-01T00:00:00"},
                        {"channel": "unknown", "timestamp": "2024-01-02T00:00:00", "source": "mystery", "medium": "weird"},
                    ],
                    "meta": {"parser": {"used_inferred_mapping": True}},
                    "_revenue_entries": [{"default_applied": True, "raw_value_zero": False}],
                    "converted": True,
                    "conversion_value": 10.0,
                }
            ],
            replace=True,
            import_source="upload",
        )

        def _unexpected_load(_db):
            raise AssertionError("_load_journeys should not be called when DQ facts are available")

        monkeypatch.setattr(services_data_quality, "_load_journeys", _unexpected_load)

        snapshots = services_data_quality.compute_dq_snapshots(db, include_taxonomy=False)
        metrics = {snapshot.metric_key: snapshot.metric_value for snapshot in snapshots}

        assert metrics["missing_profile_pct"] == 0.0
        assert metrics["defaulted_conversion_value_pct"] == 100.0
        assert metrics["inferred_mapping_journey_pct"] == 100.0
    finally:
        db.close()


def test_load_journeys_uses_silver_when_conversion_paths_absent(monkeypatch):
    db = _make_session()
    try:
        monkeypatch.setattr(services_data_quality, "load_taxonomy", Taxonomy.default)
        persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "_schema": "v2",
                    "customer": {"id": "c1"},
                    "touchpoints": [
                        {"channel": "google", "timestamp": "2024-01-01T00:00:00Z", "source": "google", "medium": "cpc"},
                    ],
                    "conversions": [{"id": "conv-1", "name": "purchase", "ts": "2024-01-01T01:00:00Z", "value": 10.0}],
                }
            ],
            replace=True,
            import_source="upload",
        )
        db.query(services_data_quality.ConversionPath).delete(synchronize_session=False)
        db.commit()

        journeys = services_data_quality._load_journeys(db)

        assert len(journeys) == 1
        assert journeys[0]["customer"]["id"] == "c1"
        assert journeys[0]["touchpoints"][0]["channel"] == "google"
        assert journeys[0]["conversions"][0]["value"] == 10.0
    finally:
        db.close()
