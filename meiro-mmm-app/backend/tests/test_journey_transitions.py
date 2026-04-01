from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import JourneyDefinition, JourneyTransitionDaily
from app.services_conversions import persist_journeys_as_conversion_paths
from app.services_journey_transitions import list_transitions_for_journey_definition


def _unit_db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _seed(db):
    jd = JourneyDefinition(
        id="jd-1",
        name="Journey",
        conversion_kpi_id="purchase",
        lookback_window_days=30,
        mode_default="conversion_only",
        created_by="test",
        updated_by="test",
        is_archived=False,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db.add(jd)
    rows = [
        JourneyTransitionDaily(
            date=date(2026, 2, 10),
            journey_definition_id="jd-1",
            from_step="Paid Landing",
            to_step="Product View / Content View",
            count_transitions=100,
            count_profiles=80,
            avg_time_between_sec=3600.0,
            p50_time_between_sec=3000.0,
            p90_time_between_sec=5400.0,
            channel_group="paid",
            campaign_id="cmp-1",
            device="mobile",
            country="US",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ),
        JourneyTransitionDaily(
            date=date(2026, 2, 10),
            journey_definition_id="jd-1",
            from_step="Product View / Content View",
            to_step="Add to Cart / Form Start",
            count_transitions=80,
            count_profiles=70,
            avg_time_between_sec=2400.0,
            p50_time_between_sec=1800.0,
            p90_time_between_sec=4200.0,
            channel_group="paid",
            campaign_id="cmp-1",
            device="mobile",
            country="US",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ),
        JourneyTransitionDaily(
            date=date(2026, 2, 10),
            journey_definition_id="jd-1",
            from_step="Add to Cart / Form Start",
            to_step="Checkout / Form Submit",
            count_transitions=60,
            count_profiles=50,
            avg_time_between_sec=1800.0,
            p50_time_between_sec=1500.0,
            p90_time_between_sec=2700.0,
            channel_group="paid",
            campaign_id="cmp-1",
            device="mobile",
            country="US",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ),
        JourneyTransitionDaily(
            date=date(2026, 2, 10),
            journey_definition_id="jd-1",
            from_step="Checkout / Form Submit",
            to_step="Purchase / Lead Won (conversion)",
            count_transitions=50,
            count_profiles=45,
            avg_time_between_sec=1200.0,
            p50_time_between_sec=900.0,
            p90_time_between_sec=1800.0,
            channel_group="paid",
            campaign_id="cmp-1",
            device="mobile",
            country="US",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ),
        JourneyTransitionDaily(
            date=date(2026, 2, 10),
            journey_definition_id="jd-1",
            from_step="Rare Step A",
            to_step="Rare Step B",
            count_transitions=2,
            count_profiles=2,
            avg_time_between_sec=600.0,
            p50_time_between_sec=600.0,
            p90_time_between_sec=600.0,
            channel_group="paid",
            campaign_id="cmp-1",
            device="mobile",
            country="US",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ),
    ]
    db.add_all(rows)
    db.commit()


def test_list_transitions_response_shape_and_filters():
    db = _unit_db_session()
    try:
        _seed(db)
        out = list_transitions_for_journey_definition(
            db,
            journey_definition_id="jd-1",
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 28),
            channel_group="paid",
            campaign_id="cmp-1",
            device="mobile",
            country="US",
            min_count=5,
            max_nodes=20,
            max_depth=5,
        )
        assert "nodes" in out and isinstance(out["nodes"], list)
        assert "edges" in out and isinstance(out["edges"], list)
        assert "meta" in out and isinstance(out["meta"], dict)
        assert out["meta"]["min_count"] == 5
        assert out["meta"]["max_nodes"] == 20
        assert out["meta"]["max_depth"] == 5
        if out["nodes"]:
            assert "id" in out["nodes"][0]
            assert "label" in out["nodes"][0]
        if out["edges"]:
            assert "source" in out["edges"][0]
            assert "target" in out["edges"][0]
            assert "value" in out["edges"][0]
            assert "avg_time_between_sec" in out["edges"][0]
            assert out["edges"][0]["avg_time_between_sec"] is not None
    finally:
        db.close()


def test_list_transitions_groups_rare_nodes_into_other():
    db = _unit_db_session()
    try:
        _seed(db)
        out = list_transitions_for_journey_definition(
            db,
            journey_definition_id="jd-1",
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 28),
            min_count=1,
            max_nodes=3,
            max_depth=10,
        )
        assert out["meta"]["grouped_to_other"] is True
        node_ids = {n["id"] for n in out["nodes"]}
        assert "Other" in node_ids
    finally:
        db.close()


def test_list_transitions_applies_max_depth():
    db = _unit_db_session()
    try:
        _seed(db)
        out = list_transitions_for_journey_definition(
            db,
            journey_definition_id="jd-1",
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 28),
            min_count=1,
            max_nodes=20,
            max_depth=2,
        )
        assert out["edges"]
        assert all(e["target"] != "Purchase / Lead Won (conversion)" for e in out["edges"])
    finally:
        db.close()


def test_list_transitions_falls_back_to_transition_facts_when_daily_rows_absent():
    db = _unit_db_session()
    try:
        jd = JourneyDefinition(
            id="jd-facts",
            name="Journey",
            conversion_kpi_id="purchase",
            lookback_window_days=30,
            mode_default="conversion_only",
            created_by="test",
            updated_by="test",
            is_archived=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add(jd)
        db.commit()

        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-1"},
                    "touchpoints": [
                        {"ts": "2026-02-10T09:00:00Z", "channel": "google_ads", "interaction_type": "click"},
                        {"ts": "2026-02-10T10:00:00Z", "channel": "google_ads", "event_name": "page_view"},
                        {"ts": "2026-02-10T11:00:00Z", "channel": "google_ads", "event_name": "add_to_cart"},
                    ],
                    "conversions": [{"id": "conv-1", "name": "purchase", "ts": "2026-02-10T12:00:00Z", "value": 100.0}],
                    "device": "mobile",
                    "country": "US",
                }
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="transition-facts-batch",
        )
        assert inserted == 1
        db.query(JourneyTransitionDaily).delete(synchronize_session=False)
        db.commit()

        out = list_transitions_for_journey_definition(
            db,
            journey_definition_id="jd-facts",
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 28),
            min_count=1,
            max_nodes=20,
            max_depth=5,
        )

        assert out["edges"]
        assert out["meta"]["source"] == "transition_facts"
        assert any(edge["avg_time_between_sec"] is not None for edge in out["edges"])
    finally:
        db.close()
