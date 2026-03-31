from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.services_conversions import v2_to_legacy
from app.services_conversions import persist_journeys_as_conversion_paths
from app.services_taxonomy_suggestions import generate_taxonomy_suggestions, generate_taxonomy_suggestions_from_db
from app.services_taxonomy import (
    compute_channel_confidence_from_db,
    compute_taxonomy_coverage_from_db,
    compute_unknown_share,
    compute_unknown_share_from_db,
    map_to_channel,
    normalize_touchpoint_with_confidence,
    persist_taxonomy_dq_snapshots_from_db,
)
from app.services_taxonomy_decisions import build_taxonomy_overview, build_taxonomy_overview_from_db
from app.utils.taxonomy import Taxonomy, normalize_touchpoint


def _make_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)()


def test_map_to_channel_returns_match_for_active_rule_conditions():
    mapping = map_to_channel("google", "cpc")

    assert mapping.channel == "paid_search"
    assert mapping.confidence == 1.0


def test_compute_unknown_share_reads_nested_utm_fields():
    journeys = [
        {
            "touchpoints": [
                {
                    "utm": {
                        "source": "google",
                        "medium": "cpc",
                        "campaign": "brand",
                    }
                }
            ]
        }
    ]

    report = compute_unknown_share(journeys)

    assert report.total_touchpoints == 1
    assert report.unknown_count == 0
    assert report.unknown_share == 0.0


def test_compute_unknown_share_on_v2_legacy_roundtrip_preserves_utm_mapping():
    v2_journey = {
        "_schema": "v2",
        "customer": {"id": "cust-1"},
        "touchpoints": [
            {
                "channel": "paid_search",
                "ts": "2026-03-01T00:00:00Z",
                "source": "google",
                "medium": "cpc",
                "campaign": {"name": "brand"},
                "utm": {"source": "google", "medium": "cpc", "campaign": "brand"},
            }
        ],
        "conversions": [{"name": "purchase", "value": 99.0}],
    }

    report = compute_unknown_share([v2_to_legacy(v2_journey)])

    assert report.total_touchpoints == 1
    assert report.unknown_count == 0
    assert report.unknown_share == 0.0


def test_build_taxonomy_overview_handles_dict_shaped_campaign_fields():
    v2_journey = {
        "_schema": "v2",
        "customer": {"id": "cust-2"},
        "touchpoints": [
            {
                "channel": "paid_search",
                "ts": "2026-03-01T00:00:00Z",
                "source": "google",
                "medium": "cpc",
                "campaign": {"name": "brand"},
                "utm": {"source": "google", "medium": "cpc", "campaign": "brand"},
            }
        ],
        "conversions": [{"name": "purchase", "value": 99.0}],
    }

    overview = build_taxonomy_overview([v2_to_legacy(v2_journey)], suggestion_count=0)

    assert overview["summary"]["unknown_share"] == 0.0
    assert overview["summary"]["unknown_count"] == 0


def test_normalize_touchpoint_infers_organic_from_external_search_referrer():
    normalized = normalize_touchpoint(
        {
            "page_location": "https://www.copygeneral.cz/brno",
            "page_referrer": "https://www.google.com/",
        }
    )

    assert normalized["source"] == "google"
    assert normalized["medium"] == "organic"
    assert normalized["meta"]["inferred_source_medium_from_referrer"] is True


def test_normalize_touchpoint_ignores_self_referrals():
    normalized = normalize_touchpoint(
        {
            "page_location": "https://www.copygeneral.cz/brno",
            "page_referrer": "https://www.copygeneral.cz/kontakty",
        }
    )

    assert "source" not in normalized
    assert "medium" not in normalized
    assert normalized.get("meta", {}).get("inferred_source_medium_from_referrer") is None


def test_normalize_touchpoint_with_confidence_marks_referrer_inference():
    normalized, confidence = normalize_touchpoint_with_confidence(
        {
            "page_location": "https://www.copygeneral.cz/brno",
            "page_referrer": "https://www.google.com/",
        }
    )

    assert normalized["source"] == "google"
    assert normalized["medium"] == "organic"
    assert normalized["_inference"]["source_medium_from_referrer"] is True
    assert confidence > 0


def test_generate_taxonomy_suggestions_handles_source_only_unknown_patterns():
    journeys = [
        {
            "touchpoints": [
                {"source": "google", "medium": "", "campaign": None},
                {"source": "google", "medium": "", "campaign": None},
                {"source": "google", "medium": "", "campaign": None},
            ]
        }
    ]

    out = generate_taxonomy_suggestions(journeys, taxonomy=Taxonomy.default(), limit=5)

    assert out["suggestions"]
    first = out["suggestions"][0]
    assert first["type"] == "channel_rule"
    assert "google" in first["title"].lower()
    assert first["channel"] == "organic_search"


def test_generate_taxonomy_suggestions_from_db_uses_persisted_touchpoint_facts():
    db = _make_session()
    try:
        persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "customer_id": "c1",
                    "conversion_id": "conv-1",
                    "kpi_type": "purchase",
                    "touchpoints": [
                        {"source": "google", "medium": "", "campaign": None},
                        {"source": "google", "medium": "", "campaign": None},
                        {"source": "google", "medium": "", "campaign": None},
                    ],
                    "converted": True,
                }
            ],
            replace=True,
            import_source="upload",
        )

        out = generate_taxonomy_suggestions_from_db(db, taxonomy=Taxonomy.default(), limit=5)

        assert out is not None
        assert out["suggestions"]
        first = out["suggestions"][0]
        assert first["type"] == "channel_rule"
        assert "google" in first["title"].lower()
        assert first["channel"] == "organic_search"
    finally:
        db.close()


def test_taxonomy_overview_from_db_uses_persisted_touchpoint_facts():
    db = _make_session()
    try:
        persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "customer_id": "c1",
                    "conversion_id": "conv-1",
                    "kpi_type": "purchase",
                    "touchpoints": [
                        {"source": "google", "medium": "", "campaign": None},
                        {"source": "newsletter", "medium": "email", "campaign": "welcome"},
                    ],
                    "converted": True,
                }
            ],
            replace=True,
            import_source="upload",
        )

        overview = build_taxonomy_overview_from_db(db, taxonomy=Taxonomy.default(), suggestion_count=1)
        assert overview is not None
        assert overview["summary"]["total_touchpoints"] == 2
        assert overview["summary"]["unknown_count"] >= 1
        assert overview["top_unmapped_patterns"]
    finally:
        db.close()


def test_taxonomy_reports_from_db_use_persisted_touchpoint_facts():
    db = _make_session()
    try:
        persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "customer_id": "c1",
                    "conversion_id": "conv-1",
                    "kpi_type": "purchase",
                    "touchpoints": [
                        {"source": "google", "medium": "", "campaign": None},
                        {"source": "newsletter", "medium": "email", "campaign": "welcome"},
                    ],
                    "converted": True,
                }
            ],
            replace=True,
            import_source="upload",
        )

        unknown = compute_unknown_share_from_db(db, taxonomy=Taxonomy.default())
        coverage = compute_taxonomy_coverage_from_db(db, taxonomy=Taxonomy.default())

        assert unknown is not None
        assert coverage is not None
        assert unknown.total_touchpoints == 2
        assert unknown.unknown_count >= 1
        assert coverage["top_unmapped_patterns"]
        channel_confidence = compute_channel_confidence_from_db(db, "email", taxonomy=Taxonomy.default())
        assert channel_confidence is not None
        assert channel_confidence["touchpoint_count"] == 1
    finally:
        db.close()


def test_persist_taxonomy_dq_snapshots_from_db_uses_persisted_touchpoint_facts():
    db = _make_session()
    try:
        persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "customer_id": "c1",
                    "conversion_id": "conv-1",
                    "kpi_type": "purchase",
                    "touchpoints": [
                        {"source": "google", "medium": "", "campaign": None},
                        {"source": "newsletter", "medium": "email", "campaign": "welcome"},
                    ],
                    "converted": True,
                }
            ],
            replace=True,
            import_source="upload",
        )

        snapshots = persist_taxonomy_dq_snapshots_from_db(db, taxonomy=Taxonomy.default())

        assert snapshots is not None
        assert len(snapshots) == 6
        metric_map = {snapshot.metric_key: snapshot for snapshot in snapshots}
        assert metric_map["unknown_channel_share"].metric_value > 0
        assert metric_map["source_coverage"].metric_value >= 0
        assert metric_map["medium_coverage"].metric_value >= 0
        assert metric_map["mean_touchpoint_confidence"].meta_json["source"] == "db_touchpoint_facts"
        assert metric_map["mean_journey_confidence"].meta_json["source"] == "db_touchpoint_facts"
    finally:
        db.close()
