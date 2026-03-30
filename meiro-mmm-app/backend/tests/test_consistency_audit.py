from datetime import date, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.consistency_audit import (
    _date_scoped_conversion_path_totals,
    _date_scoped_journey_path_daily_totals,
)
from app.db import Base
from app.models_config_dq import ConversionPath, JourneyDefinition, JourneyPathDaily


def _session():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)()


def test_consistency_audit_scoped_totals_match_raw_and_daily_facts():
    db = _session()
    try:
        db.add(
            JourneyDefinition(
                id="def-audit",
                name="Audit Journey",
                conversion_kpi_id="purchase",
                lookback_window_days=30,
                mode_default="conversion_only",
                created_by="test",
                updated_by="test",
                is_archived=False,
                created_at=datetime(2024, 2, 1, 0, 0),
                updated_at=datetime(2024, 2, 1, 0, 0),
            )
        )
        db.add(
            ConversionPath(
                conversion_id="conv-1",
                profile_id="profile-1",
                conversion_key="purchase",
                conversion_ts=datetime(2024, 2, 11, 12, 0),
                path_json={
                    "converted": True,
                    "_revenue_entries": [
                        {
                            "gross_value": 120.0,
                            "net_value": 100.0,
                            "gross_conversions": 1.0,
                            "net_conversions": 1.0,
                            "valid_leads": 1.0,
                            "value_in_base": 120.0,
                        }
                    ],
                },
                path_hash="hash-1",
                length=1,
                first_touch_ts=datetime(2024, 2, 11, 11, 0),
                last_touch_ts=datetime(2024, 2, 11, 11, 30),
            )
        )
        db.add(
            JourneyPathDaily(
                date=date(2024, 2, 11),
                journey_definition_id="def-audit",
                path_hash="hash-1",
                path_steps=["Paid", "Purchase"],
                path_length=2,
                count_journeys=1,
                count_conversions=1,
                gross_conversions_total=1.0,
                net_conversions_total=1.0,
                gross_revenue_total=120.0,
                net_revenue_total=100.0,
                view_through_conversions_total=0.0,
                click_through_conversions_total=1.0,
                mixed_path_conversions_total=0.0,
                avg_time_to_convert_sec=3600.0,
                p50_time_to_convert_sec=3600.0,
                p90_time_to_convert_sec=3600.0,
                created_at=datetime(2024, 2, 11, 0, 0),
                updated_at=datetime(2024, 2, 11, 0, 0),
            )
        )
        db.commit()

        conversion_path_totals = _date_scoped_conversion_path_totals(
            db,
            date_from="2024-02-10",
            date_to="2024-02-15",
        )
        daily_totals = _date_scoped_journey_path_daily_totals(
            db,
            date_from="2024-02-10",
            date_to="2024-02-15",
        )

        assert conversion_path_totals == {
            "count_conversions": 1.0,
            "gross_conversions": 1.0,
            "net_conversions": 1.0,
            "gross_revenue": 120.0,
            "net_revenue": 100.0,
        }
        assert daily_totals == conversion_path_totals
    finally:
        db.close()
