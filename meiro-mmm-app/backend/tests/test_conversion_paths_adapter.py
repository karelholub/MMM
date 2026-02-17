from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import JourneyDefinition, JourneyPathDaily
from app.services_conversion_paths_adapter import (
    build_conversion_path_details_from_daily,
    build_conversion_paths_analysis_from_daily,
)


def _unit_db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _seed(db):
    jd = JourneyDefinition(
        id="jd-adapter",
        name="Adapter Journey",
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
        JourneyPathDaily(
            date=date(2026, 2, 1),
            journey_definition_id="jd-adapter",
            path_hash="p1",
            path_steps=["Paid Landing", "Product View / Content View", "Purchase / Lead Won (conversion)"],
            path_length=3,
            count_journeys=10,
            count_conversions=8,
            avg_time_to_convert_sec=172800.0,
            p50_time_to_convert_sec=160000.0,
            p90_time_to_convert_sec=260000.0,
            channel_group="paid",
            campaign_id="cmp-1",
            device="mobile",
            country="US",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ),
        JourneyPathDaily(
            date=date(2026, 2, 2),
            journey_definition_id="jd-adapter",
            path_hash="p2",
            path_steps=["Organic Landing", "Product View / Content View", "Checkout / Form Submit"],
            path_length=3,
            count_journeys=5,
            count_conversions=2,
            avg_time_to_convert_sec=86400.0,
            p50_time_to_convert_sec=80000.0,
            p90_time_to_convert_sec=120000.0,
            channel_group="organic",
            campaign_id="cmp-2",
            device="desktop",
            country="US",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ),
    ]
    db.add_all(rows)
    db.commit()


def test_analysis_shape_and_source():
    db = _unit_db_session()
    try:
        _seed(db)
        out = build_conversion_paths_analysis_from_daily(
            db,
            definition_id="jd-adapter",
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 28),
            direct_mode="include",
            path_scope="all",
            nba_config={"min_prefix_support": 3, "min_conversion_rate": 0.01},
        )
        assert out["source"] == "journey_paths_daily"
        assert out["total_journeys"] == 15
        assert out["common_paths"]
        assert out["path_length_distribution"]["max"] >= 3
        assert isinstance(out["next_best_by_prefix"], dict)
    finally:
        db.close()


def test_details_returns_selected_path_summary():
    db = _unit_db_session()
    try:
        _seed(db)
        path = "Paid Landing > Product View / Content View > Purchase / Lead Won (conversion)"
        out = build_conversion_path_details_from_daily(
            db,
            path=path,
            definition_id="jd-adapter",
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 28),
            direct_mode="include",
            path_scope="all",
        )
        assert out["path"] == path
        assert out["summary"]["count"] == 10
        assert out["step_breakdown"]
    finally:
        db.close()
