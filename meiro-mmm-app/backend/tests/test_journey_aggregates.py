from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import (
    ConversionPath,
    JourneyDefinition,
    JourneyPathDaily,
    JourneyTransitionDaily,
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
) -> ConversionPath:
    return ConversionPath(
        conversion_id=conversion_id,
        profile_id=profile_id,
        conversion_key=conversion_key,
        conversion_ts=conversion_ts,
        path_json={"touchpoints": touchpoints, "conversions": [{"name": conversion_key, "ts": conversion_ts.isoformat() + "Z"}]},
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
        assert len(path_rows) >= 2
        assert len(transition_rows) >= 1
        assert any(r.from_step in {STEP_PAID_LANDING, STEP_ORGANIC_LANDING} for r in transition_rows)

        # Simulate data loss for day1 aggregates; rerun should backfill missing day1.
        day1_date = date(2026, 2, 8)
        db.query(JourneyPathDaily).filter(JourneyPathDaily.date == day1_date).delete(synchronize_session=False)
        db.query(JourneyTransitionDaily).filter(JourneyTransitionDaily.date == day1_date).delete(synchronize_session=False)
        db.commit()

        out2 = run_daily_journey_aggregates(db, as_of_date=date(2026, 2, 10), reprocess_days=1)
        assert out2["days_processed"] >= 2
        restored = (
            db.query(JourneyPathDaily)
            .filter(JourneyPathDaily.journey_definition_id == "def-1", JourneyPathDaily.date == day1_date)
            .count()
        )
        assert restored > 0
    finally:
        db.close()
