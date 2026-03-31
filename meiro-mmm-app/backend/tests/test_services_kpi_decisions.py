from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.modules.settings.schemas import KpiConfigModel, KpiDefinitionModel
from app.services_conversions import persist_journeys_as_conversion_paths
from app.services_kpi_decisions import (
    build_kpi_overview_from_db,
    build_kpi_suggestions,
    build_kpi_suggestions_from_db,
)


def _make_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)()


def test_build_kpi_suggestions_uses_journey_kpi_type_when_events_are_missing():
    journeys = [
        {"kpi_type": "purchase", "converted": True, "touchpoints": [{"channel": "Paid Search"}]},
        {"kpi_type": "purchase", "converted": True, "touchpoints": [{"channel": "Paid Search"}]},
        {"kpi_type": "form_submit", "converted": True, "touchpoints": [{"channel": "Organic"}]},
    ]
    cfg = KpiConfigModel(
        primary_kpi_id="form_submit",
        definitions=[
            KpiDefinitionModel(
                id="form_submit",
                label="Form Submit",
                type="primary",
                event_name="form_submit",
                value_field=None,
                weight=1.0,
                lookback_days=14,
            )
        ],
    )

    result = build_kpi_suggestions(journeys, cfg, limit=6)
    suggestion_ids = [item["id"] for item in result["suggestions"]]

    assert "definition:purchase" in suggestion_ids


def test_build_kpi_suggestions_flags_generic_conversion_fallback():
    journeys = [
        {"kpi_type": "conversion", "converted": True, "touchpoints": [{"channel": "Direct"}]},
        {"kpi_type": "conversion", "converted": True, "touchpoints": [{"channel": "Direct"}]},
        {"kpi_type": "form_submit", "converted": True, "touchpoints": [{"channel": "Organic"}]},
    ]
    cfg = KpiConfigModel(
        primary_kpi_id="form_submit",
        definitions=[
            KpiDefinitionModel(
                id="form_submit",
                label="Form Submit",
                type="primary",
                event_name="form_submit",
                value_field=None,
                weight=1.0,
                lookback_days=14,
            ),
            KpiDefinitionModel(
                id="conversion",
                label="Generic Conversion",
                type="micro",
                event_name="conversion",
                value_field=None,
                weight=0.5,
                lookback_days=14,
            ),
        ],
    )

    result = build_kpi_suggestions(journeys, cfg, limit=6)
    suggestion_ids = [item["id"] for item in result["suggestions"]]

    assert "review_generic_conversion_mapping" in suggestion_ids


def test_build_kpi_suggestions_from_db_uses_persisted_signal_facts():
    db = _make_session()
    try:
        persist_journeys_as_conversion_paths(
            db,
            [
                {"customer_id": "c1", "conversion_id": "conv-1", "kpi_type": "purchase", "converted": True, "touchpoints": [{"channel": "Paid Search"}]},
                {"customer_id": "c2", "conversion_id": "conv-2", "kpi_type": "purchase", "converted": True, "touchpoints": [{"channel": "Paid Search"}]},
                {
                    "customer_id": "c3",
                    "conversion_id": "conv-3",
                    "kpi_type": "form_submit",
                    "converted": True,
                    "touchpoints": [{"channel": "Organic"}],
                },
            ],
            replace=True,
            import_source="upload",
        )
        cfg = KpiConfigModel(
            primary_kpi_id="form_submit",
            definitions=[
                KpiDefinitionModel(
                    id="form_submit",
                    label="Form Submit",
                    type="primary",
                    event_name="form_submit",
                    value_field=None,
                    weight=1.0,
                    lookback_days=14,
                )
            ],
        )

        result = build_kpi_suggestions_from_db(db, cfg, limit=6)
        assert result is not None
        suggestion_ids = [item["id"] for item in result["suggestions"]]
        assert "definition:purchase" in suggestion_ids
    finally:
        db.close()


def test_build_kpi_overview_from_db_uses_persisted_signal_facts():
    db = _make_session()
    try:
        persist_journeys_as_conversion_paths(
            db,
            [
                {"customer_id": "c1", "conversion_id": "conv-1", "kpi_type": "purchase", "converted": True, "touchpoints": [{"channel": "Paid Search"}]},
                {"customer_id": "c2", "conversion_id": "conv-2", "kpi_type": "purchase", "converted": True, "touchpoints": [{"channel": "Paid Search"}]},
                {"customer_id": "c3", "conversion_id": "conv-3", "kpi_type": "form_submit", "converted": True, "touchpoints": [{"channel": "Organic"}]},
            ],
            replace=True,
            import_source="upload",
        )
        cfg = KpiConfigModel(
            primary_kpi_id="form_submit",
            definitions=[
                KpiDefinitionModel(
                    id="form_submit",
                    label="Form Submit",
                    type="primary",
                    event_name="form_submit",
                    value_field=None,
                    weight=1.0,
                    lookback_days=14,
                ),
                KpiDefinitionModel(
                    id="purchase",
                    label="Purchase",
                    type="micro",
                    event_name="purchase",
                    value_field=None,
                    weight=0.5,
                    lookback_days=14,
                ),
            ],
        )

        overview = build_kpi_overview_from_db(db, cfg, suggestion_count=1)
        assert overview is not None
        assert overview["summary"]["journeys_total"] == 3
        assert overview["summary"]["journeys_with_primary_kpi"] == 1
    finally:
        db.close()
