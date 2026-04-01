from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import (
    ChannelPerformanceDaily,
    ConversionPath,
    JourneyDefinition,
    JourneyExampleFact,
    JourneyPathDaily,
    JourneyTransitionFact,
    JourneyTransitionDaily,
    SilverConversionFact,
    SilverTouchpointFact,
)
from app.services_journey_aggregates import (
    STEP_ADD_TO_CART,
    STEP_CHECKOUT,
    STEP_CONVERSION,
    STEP_CONTENT_VIEW,
    STEP_ORGANIC_LANDING,
    STEP_PAID_LANDING,
    dedup_steps,
    map_touchpoint_step,
    run_daily_journey_aggregates,
)
from app.services_conversions import persist_journeys_as_conversion_paths


def _unit_db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _mk_conversion_path(
    *,
    conversion_id: str,
    profile_id: str,
    conversion_ts: datetime,
    conversion_key: str = "purchase",
    touchpoints: list,
    value: float = 0.0,
) -> ConversionPath:
    return ConversionPath(
        conversion_id=conversion_id,
        profile_id=profile_id,
        conversion_key=conversion_key,
        conversion_ts=conversion_ts,
        path_json={
            "touchpoints": touchpoints,
            "conversions": [{"name": conversion_key, "ts": conversion_ts.isoformat() + "Z", "value": value}],
        },
        path_hash=f"hash-{conversion_id}",
        length=len(touchpoints),
        first_touch_ts=conversion_ts - timedelta(days=2),
        last_touch_ts=conversion_ts - timedelta(hours=1),
    )


def test_map_touchpoint_step_paid_and_funnel_events():
    first_paid = {"channel": "google_ads", "utm": {"medium": "cpc"}}
    assert map_touchpoint_step(first_paid, 0) == STEP_PAID_LANDING

    first_organic = {"channel": "direct"}
    assert map_touchpoint_step(first_organic, 0) == STEP_ORGANIC_LANDING

    assert map_touchpoint_step({"event_name": "view_item"}, 1) == STEP_CONTENT_VIEW
    assert map_touchpoint_step({"event": "add_to_cart"}, 2) == STEP_ADD_TO_CART
    assert map_touchpoint_step({"action": "form_submit"}, 3) == STEP_CHECKOUT


def test_dedup_steps_collapse_consecutive_and_cap():
    raw = [STEP_PAID_LANDING, STEP_PAID_LANDING, STEP_CONTENT_VIEW, STEP_CONTENT_VIEW, STEP_ADD_TO_CART]
    assert dedup_steps(raw, max_steps=20) == [STEP_PAID_LANDING, STEP_CONTENT_VIEW, STEP_ADD_TO_CART]

    many = [STEP_CONTENT_VIEW] * 100
    assert len(dedup_steps(many + [STEP_CONVERSION], max_steps=20)) <= 20


def test_run_daily_journey_aggregates_writes_paths_and_transitions_and_backfills_missing_days():
    db = _unit_db_session()
    try:
        definition = JourneyDefinition(
            id="def-1",
            name="Default Journey",
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

        day1 = datetime(2026, 2, 8, 12, 0, 0)
        day2 = datetime(2026, 2, 9, 12, 0, 0)
        db.add(
            _mk_conversion_path(
                conversion_id="c1",
                profile_id="p1",
                conversion_ts=day1,
                value=120.0,
                touchpoints=[
                    {"ts": "2026-02-06T10:00:00Z", "channel": "google_ads", "utm": {"medium": "cpc"}},
                    {"ts": "2026-02-07T10:00:00Z", "event_name": "view_item", "channel": "google_ads"},
                    {"ts": "2026-02-08T09:00:00Z", "event": "add_to_cart", "channel": "google_ads"},
                    {"ts": "2026-02-08T11:00:00Z", "action": "checkout", "channel": "google_ads"},
                ],
            )
        )
        db.add(
            _mk_conversion_path(
                conversion_id="c2",
                profile_id="p2",
                conversion_ts=day2,
                value=80.0,
                touchpoints=[
                    {"ts": "2026-02-08T08:00:00Z", "channel": "direct"},
                    {"ts": "2026-02-09T10:00:00Z", "event_name": "content_view", "channel": "email"},
                ],
            )
        )
        db.commit()

        # as_of=2026-02-10 => latest complete day is 2026-02-09
        out1 = run_daily_journey_aggregates(db, as_of_date=date(2026, 2, 10), reprocess_days=1)
        assert out1["days_processed"] >= 2  # backfills day1 and processes recent day2
        assert out1["source_rows_processed"] >= 2

        path_rows = (
            db.query(JourneyPathDaily)
            .filter(JourneyPathDaily.journey_definition_id == "def-1")
            .all()
        )
        transition_rows = (
            db.query(JourneyTransitionDaily)
            .filter(JourneyTransitionDaily.journey_definition_id == "def-1")
            .all()
        )
        channel_rows = db.query(ChannelPerformanceDaily).all()
        example_rows = db.query(JourneyExampleFact).all()
        assert len(path_rows) >= 2
        assert len(transition_rows) >= 1
        assert channel_rows
        assert example_rows
        assert any(r.from_step in {STEP_PAID_LANDING, STEP_ORGANIC_LANDING} for r in transition_rows)
        assert any(float(r.avg_time_between_sec or 0.0) > 0.0 for r in transition_rows)
        assert any(float(r.p50_time_between_sec or 0.0) > 0.0 for r in transition_rows)
        assert any(float(r.gross_conversions_total or 0.0) >= 1.0 for r in path_rows)
        assert any(float(r.net_conversions_total or 0.0) >= 1.0 for r in path_rows)
        assert round(sum(float(r.gross_revenue_total or 0.0) for r in path_rows), 2) == 200.0
        paid_row = next(r for r in channel_rows if r.date == day1.date() and r.channel == "google_ads" and r.conversion_key is None)
        assert paid_row.visits_total >= 1
        assert float(paid_row.gross_revenue_total or 0.0) == 120.0
        keyed_row = next(r for r in channel_rows if r.date == day1.date() and r.channel == "google_ads" and r.conversion_key == "purchase")
        assert keyed_row.visits_total == 0
        assert keyed_row.count_conversions == 1
        example_row = next(r for r in example_rows if r.conversion_id == "c1")
        assert example_row.path_hash
        assert example_row.touchpoints_count == 4

        # Simulate data loss for day1 aggregates; rerun should backfill missing day1.
        day1_date = date(2026, 2, 8)
        db.query(JourneyPathDaily).filter(JourneyPathDaily.date == day1_date).delete(synchronize_session=False)
        db.query(JourneyTransitionDaily).filter(JourneyTransitionDaily.date == day1_date).delete(synchronize_session=False)
        db.query(ChannelPerformanceDaily).filter(ChannelPerformanceDaily.date == day1_date).delete(synchronize_session=False)
        db.query(JourneyExampleFact).filter(JourneyExampleFact.date == day1_date).delete(synchronize_session=False)
        db.commit()

        out2 = run_daily_journey_aggregates(db, as_of_date=date(2026, 2, 10), reprocess_days=1)
        assert out2["days_processed"] >= 2
        restored = (
            db.query(JourneyPathDaily)
            .filter(JourneyPathDaily.journey_definition_id == "def-1", JourneyPathDaily.date == day1_date)
            .count()
        )
        assert restored > 0
        assert db.query(ChannelPerformanceDaily).filter(ChannelPerformanceDaily.date == day1_date).count() > 0
        assert db.query(JourneyExampleFact).filter(JourneyExampleFact.date == day1_date).count() > 0
    finally:
        db.close()


def test_run_daily_journey_aggregates_rebuilds_daily_facts_from_silver_without_conversion_paths():
    db = _unit_db_session()
    try:
        definition = JourneyDefinition(
            id="def-silver",
            name="Silver Journey",
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
                    "customer": {"id": "cust-1"},
                    "touchpoints": [
                        {"ts": "2026-02-08T09:00:00Z", "channel": "google_ads", "interaction_type": "impression"},
                        {"ts": "2026-02-08T10:00:00Z", "channel": "google_ads", "campaign": "Brand", "interaction_type": "click"},
                    ],
                    "conversions": [
                        {"id": "conv-silver", "name": "purchase", "ts": "2026-02-08T12:00:00Z", "value": 150.0}
                    ],
                }
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="silver-channel-batch",
        )
        assert inserted == 1

        db.query(ConversionPath).delete(synchronize_session=False)
        db.query(SilverTouchpointFact).delete(synchronize_session=False)
        db.query(SilverConversionFact).delete(synchronize_session=False)
        db.commit()

        out = run_daily_journey_aggregates(db, as_of_date=date(2026, 2, 9), reprocess_days=1)

        channel_rows = (
            db.query(ChannelPerformanceDaily)
            .filter(ChannelPerformanceDaily.date == date(2026, 2, 8))
            .order_by(ChannelPerformanceDaily.channel.asc(), ChannelPerformanceDaily.conversion_key.asc().nullsfirst())
            .all()
        )
        path_rows = db.query(JourneyPathDaily).filter(JourneyPathDaily.date == date(2026, 2, 8)).all()
        transition_rows = db.query(JourneyTransitionDaily).filter(JourneyTransitionDaily.date == date(2026, 2, 8)).all()
        example_rows = db.query(JourneyExampleFact).filter(JourneyExampleFact.date == date(2026, 2, 8)).all()

        assert out["channel_rows_written"] >= 2
        assert out["path_rows_written"] >= 1
        assert out["transition_rows_written"] >= 1
        assert channel_rows
        assert any(row.channel == "google_ads" and row.conversion_key is None and row.visits_total == 2 for row in channel_rows)
        assert any(
            row.channel == "google_ads"
            and row.conversion_key == "purchase"
            and float(row.gross_revenue_total or 0.0) == 150.0
            for row in channel_rows
        )
        assert path_rows
        assert transition_rows
        assert example_rows
        assert path_rows[0].path_steps[-1] == STEP_CONVERSION
    finally:
        db.close()


def test_run_daily_journey_aggregates_rebuilds_from_instance_transition_facts_without_silver_or_paths():
    db = _unit_db_session()
    try:
        definition = JourneyDefinition(
            id="def-instance-transitions",
            name="Instance Transition Journey",
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
                    "customer": {"id": "cust-1"},
                    "touchpoints": [
                        {"ts": "2026-02-08T09:00:00Z", "channel": "google_ads", "interaction_type": "click"},
                        {"ts": "2026-02-08T10:00:00Z", "channel": "google_ads", "event_name": "page_view"},
                    ],
                    "conversions": [
                        {"id": "conv-instance", "name": "purchase", "ts": "2026-02-08T12:00:00Z", "value": 150.0}
                    ],
                }
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="instance-transition-batch",
        )
        assert inserted == 1
        assert db.query(JourneyTransitionFact).count() >= 1

        db.query(ConversionPath).delete(synchronize_session=False)
        db.query(SilverTouchpointFact).delete(synchronize_session=False)
        db.query(SilverConversionFact).delete(synchronize_session=False)
        db.commit()

        out = run_daily_journey_aggregates(db, as_of_date=date(2026, 2, 9), reprocess_days=1)

        transition_rows = db.query(JourneyTransitionDaily).filter(JourneyTransitionDaily.date == date(2026, 2, 8)).all()
        assert out["transition_rows_written"] >= 1
        assert transition_rows
        assert any(float(row.avg_time_between_sec or 0.0) > 0.0 for row in transition_rows)
    finally:
        db.close()
