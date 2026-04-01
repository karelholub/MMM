from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import ConversionPath, JourneyDefinition, JourneyExampleFact, SilverConversionFact, SilverTouchpointFact
from app.services_conversions import persist_journeys_as_conversion_paths
from app.services_journey_examples import list_examples_for_journey_definition


def _unit_db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def test_list_examples_for_journey_definition_uses_shared_conversion_path_helpers():
    db = _unit_db_session()
    try:
        definition = JourneyDefinition(
            id="jd-1",
            name="J1",
            conversion_kpi_id="purchase",
            lookback_window_days=30,
            mode_default="conversion_only",
            created_by="test",
            updated_by="test",
            is_archived=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add(definition)
        db.add(
            ConversionPath(
                conversion_id="conv-1",
                profile_id="p-1",
                conversion_key="purchase",
                conversion_ts=datetime(2026, 2, 5, 12, 0),
                path_json={
                    "touchpoints": [
                        {"ts": "2026-02-01T09:00:00Z", "channel": "paid", "campaign": {"id": "cmp-1", "name": "Brand"}},
                        {"ts": "2026-02-03T09:00:00Z", "channel": "email", "event_name": "product_view"},
                    ],
                    "conversions": [{"name": "purchase", "value": 123.45}],
                },
                path_hash="hash-1",
                length=2,
                first_touch_ts=datetime(2026, 2, 1, 9, 0),
                last_touch_ts=datetime(2026, 2, 3, 9, 0),
            )
        )
        db.commit()

        out = list_examples_for_journey_definition(
            db,
            definition=definition,
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 10),
            limit=10,
        )

        assert out["total"] == 1
        item = out["items"][0]
        assert item["conversion_id"] == "conv-1"
        assert item["touchpoints_count"] == 2
        assert item["conversion_value"] == 123.45
        assert item["touchpoints_preview"][0]["campaign"] == "Brand"
    finally:
        db.close()


def test_list_examples_for_journey_definition_prefers_persisted_example_facts():
    db = _unit_db_session()
    try:
        definition = JourneyDefinition(
            id="jd-1",
            name="J1",
            conversion_kpi_id="purchase",
            lookback_window_days=30,
            mode_default="conversion_only",
            created_by="test",
            updated_by="test",
            is_archived=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add(definition)
        db.add(
            JourneyExampleFact(
                date=date(2026, 2, 5),
                journey_definition_id="jd-1",
                conversion_id="conv-1",
                profile_id="p-1",
                conversion_key="purchase",
                conversion_ts=datetime(2026, 2, 5, 12, 0),
                path_hash="fact-hash-1",
                steps_json=["Paid Landing", "Product View / Content View", "Purchase / Lead Won (conversion)"],
                touchpoints_count=2,
                conversion_value=123.45,
                channel_group="paid",
                campaign_id="cmp-1",
                device=None,
                country=None,
                touchpoints_preview_json=[
                    {"ts": "2026-02-01T09:00:00Z", "channel": "paid", "event": None, "campaign": "Brand"},
                    {"ts": "2026-02-03T09:00:00Z", "channel": "email", "event": "product_view", "campaign": None},
                ],
            )
        )
        db.commit()

        out = list_examples_for_journey_definition(
            db,
            definition=definition,
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 10),
            limit=10,
        )

        assert out["total"] == 1
        item = out["items"][0]
        assert item["conversion_id"] == "conv-1"
        assert item["path_hash"] == "fact-hash-1"
        assert item["touchpoints_count"] == 2
        assert item["conversion_value"] == 123.45
        assert item["touchpoints_preview"][0]["campaign"] == "Brand"
    finally:
        db.close()


def test_list_examples_for_journey_definition_uses_instance_facts_when_raw_and_silver_absent():
    db = _unit_db_session()
    try:
        definition = JourneyDefinition(
            id="jd-instance",
            name="J1",
            conversion_kpi_id="purchase",
            lookback_window_days=30,
            mode_default="conversion_only",
            created_by="test",
            updated_by="test",
            is_archived=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add(definition)
        db.commit()

        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "_schema": "v2",
                    "customer": {"id": "p-1"},
                    "touchpoints": [
                        {"ts": "2026-02-01T09:00:00Z", "channel": "google_ads", "campaign": "Brand"},
                        {"ts": "2026-02-03T09:00:00Z", "channel": "email", "event_name": "product_view"},
                    ],
                    "conversions": [{"id": "conv-1", "name": "purchase", "ts": "2026-02-05T12:00:00Z", "value": 123.45}],
                }
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="instance-examples-batch",
        )
        assert inserted == 1

        db.query(ConversionPath).delete(synchronize_session=False)
        db.query(SilverTouchpointFact).delete(synchronize_session=False)
        db.query(SilverConversionFact).delete(synchronize_session=False)
        db.commit()

        out = list_examples_for_journey_definition(
            db,
            definition=definition,
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 10),
            limit=10,
        )

        assert out["total"] == 1
        item = out["items"][0]
        assert item["conversion_id"] == "p-1-0"
        assert item["conversion_value"] == 123.45
        assert item["touchpoints_preview"][0]["campaign"] == "Brand"
    finally:
        db.close()
