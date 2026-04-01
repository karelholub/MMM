from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import (
    ConversionPath,
    FunnelDefinition,
    JourneyDefinition,
    JourneyTransitionDaily,
    SilverConversionFact,
    SilverTouchpointFact,
)
from app.services_conversions import persist_journeys_as_conversion_paths
from app.services_funnels import get_funnel_results


def _unit_db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def test_funnel_results_use_aggregate_transition_timings_without_raw_fallback():
    db = _unit_db_session()
    try:
        journey = JourneyDefinition(
            id="jd-funnel",
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
        funnel = FunnelDefinition(
            id="funnel-1",
            journey_definition_id="jd-funnel",
            workspace_id="default",
            user_id="default",
            name="Checkout Funnel",
            description=None,
            steps_json=[
                "Paid Landing",
                "Product View / Content View",
                "Purchase / Lead Won (conversion)",
            ],
            counting_method="unique_profiles",
            window_days=30,
            is_archived=False,
            created_by="test",
            updated_by="test",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add_all(
            [
                journey,
                funnel,
                JourneyTransitionDaily(
                    date=date(2026, 2, 10),
                    journey_definition_id="jd-funnel",
                    from_step="Paid Landing",
                    to_step="Product View / Content View",
                    count_transitions=20,
                    count_profiles=18,
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
                    journey_definition_id="jd-funnel",
                    from_step="Product View / Content View",
                    to_step="Purchase / Lead Won (conversion)",
                    count_transitions=10,
                    count_profiles=9,
                    avg_time_between_sec=7200.0,
                    p50_time_between_sec=6600.0,
                    p90_time_between_sec=8400.0,
                    channel_group="paid",
                    campaign_id="cmp-1",
                    device="mobile",
                    country="US",
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                ),
            ]
        )
        db.commit()

        out = get_funnel_results(
            db,
            funnel=funnel,
            journey_definition=journey,
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 28),
            channel_group="paid",
            campaign_id="cmp-1",
            device="mobile",
            country="US",
        )

        assert out["meta"]["source"] == "aggregates"
        assert out["meta"]["used_raw_fallback"] is False
        assert out["meta"]["warning"] is None
        assert out["time_between_steps"] == [
            {
                "from_step": "Paid Landing",
                "to_step": "Product View / Content View",
                "count": 20,
                "avg_sec": 3600.0,
                "p50_sec": 3000.0,
                "p90_sec": 5400.0,
            },
            {
                "from_step": "Product View / Content View",
                "to_step": "Purchase / Lead Won (conversion)",
                "count": 10,
                "avg_sec": 7200.0,
                "p50_sec": 6600.0,
                "p90_sec": 8400.0,
            },
        ]
    finally:
        db.close()


def test_funnel_results_fallback_uses_silver_when_conversion_paths_absent():
    db = _unit_db_session()
    try:
        journey = JourneyDefinition(
            id="jd-funnel-silver",
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
        funnel = FunnelDefinition(
            id="funnel-silver",
            journey_definition_id="jd-funnel-silver",
            workspace_id="default",
            user_id="default",
            name="Checkout Funnel",
            description=None,
            steps_json=[
                "Paid Landing",
                "Product View / Content View",
                "Purchase / Lead Won (conversion)",
            ],
            counting_method="unique_profiles",
            window_days=30,
            is_archived=False,
            created_by="test",
            updated_by="test",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add_all([journey, funnel])
        db.commit()

        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-1"},
                    "touchpoints": [
                        {"ts": "2026-02-10T09:00:00Z", "channel": "google_ads", "interaction_type": "click", "campaign": "cmp-1", "event_name": "landing"},
                        {"ts": "2026-02-10T09:30:00Z", "channel": "google_ads", "interaction_type": "click", "campaign": "cmp-1", "event_name": "page_view"},
                    ],
                    "conversions": [{"id": "conv-1", "name": "purchase", "ts": "2026-02-10T10:00:00Z", "value": 100.0}],
                    "device": "mobile",
                    "country": "US",
                }
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="silver-funnel-batch",
        )
        assert inserted == 1

        db.query(ConversionPath).delete(synchronize_session=False)
        db.commit()

        out = get_funnel_results(
            db,
            funnel=funnel,
            journey_definition=journey,
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 28),
            channel_group="paid",
            campaign_id="cmp-1",
            device="mobile",
            country="US",
        )

        assert out["meta"]["source"] == "raw"
        assert out["meta"]["used_raw_fallback"] is True
        assert out["steps"][0]["count"] == 1
        assert out["steps"][1]["count"] == 1
        assert out["steps"][2]["count"] == 1
        assert out["time_between_steps"]
    finally:
        db.close()


def test_funnel_results_fallback_uses_instance_facts_when_silver_and_paths_absent():
    db = _unit_db_session()
    try:
        journey = JourneyDefinition(
            id="jd-funnel-instance",
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
        funnel = FunnelDefinition(
            id="funnel-instance",
            journey_definition_id="jd-funnel-instance",
            workspace_id="default",
            user_id="default",
            name="Checkout Funnel",
            description=None,
            steps_json=[
                "Paid Landing",
                "Product View / Content View",
                "Purchase / Lead Won (conversion)",
            ],
            counting_method="unique_profiles",
            window_days=30,
            is_archived=False,
            created_by="test",
            updated_by="test",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add_all([journey, funnel])
        db.commit()

        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-1"},
                    "touchpoints": [
                        {"ts": "2026-02-10T09:00:00Z", "channel": "google_ads", "interaction_type": "click", "campaign": "cmp-1", "event_name": "landing"},
                        {"ts": "2026-02-10T09:30:00Z", "channel": "google_ads", "interaction_type": "click", "campaign": "cmp-1", "event_name": "page_view"},
                    ],
                    "conversions": [{"id": "conv-1", "name": "purchase", "ts": "2026-02-10T10:00:00Z", "value": 100.0}],
                    "device": "mobile",
                    "country": "US",
                }
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="instance-funnel-batch",
        )
        assert inserted == 1

        db.query(ConversionPath).delete(synchronize_session=False)
        db.query(SilverTouchpointFact).delete(synchronize_session=False)
        db.query(SilverConversionFact).delete(synchronize_session=False)
        db.commit()

        out = get_funnel_results(
            db,
            funnel=funnel,
            journey_definition=journey,
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 28),
            channel_group="paid",
            campaign_id="cmp-1",
            device="mobile",
            country="US",
        )

        assert out["meta"]["source"] == "raw"
        assert out["meta"]["used_raw_fallback"] is True
        assert out["steps"][0]["count"] == 1
        assert out["steps"][1]["count"] == 1
        assert out["steps"][2]["count"] == 1
        assert out["time_between_steps"]
    finally:
        db.close()
