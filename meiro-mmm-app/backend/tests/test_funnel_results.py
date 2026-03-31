from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import FunnelDefinition, JourneyDefinition, JourneyTransitionDaily
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
