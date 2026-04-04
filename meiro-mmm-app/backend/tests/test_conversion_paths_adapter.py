from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import JourneyDefinition, JourneyDefinitionInstanceFact, JourneyPathDaily
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
            gross_conversions_total=8.0,
            net_conversions_total=8.0,
            gross_revenue_total=800.0,
            net_revenue_total=760.0,
            view_through_conversions_total=0.0,
            click_through_conversions_total=8.0,
            mixed_path_conversions_total=0.0,
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
            gross_conversions_total=2.0,
            net_conversions_total=1.0,
            gross_revenue_total=120.0,
            net_revenue_total=60.0,
            view_through_conversions_total=0.0,
            click_through_conversions_total=2.0,
            mixed_path_conversions_total=0.0,
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
        assert out["common_paths"][0]["gross_revenue"] == 800.0
        assert out["common_paths"][0]["avg_value"] == 100.0
        assert out["path_length_distribution"]["max"] >= 3
        assert isinstance(out["next_best_by_prefix"], dict)
    finally:
        db.close()


def test_analysis_applies_nba_thresholds_and_promoted_policy_overrides():
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
            nba_config={
                "min_prefix_support": 1,
                "min_conversion_rate": 0.85,
                "max_prefix_depth": 5,
                "min_next_support": 10,
                "max_suggestions_per_prefix": 3,
                "min_uplift_pct": 0.1,
                "excluded_channels": ["direct"],
                "promoted_journey_policies": [
                    {
                        "hypothesis_id": "hyp-adapter-policy",
                        "title": "Promote checkout continuation",
                        "journey_definition_id": "jd-adapter",
                        "prefix": "Organic Landing > Product View / Content View",
                        "prefix_steps": [
                            "Organic Landing",
                            "Product View / Content View",
                        ],
                        "step": "Checkout / Form Submit",
                        "channel": "Checkout / Form Submit",
                    }
                ],
            },
        )

        paid_prefix = "Paid Landing > Product View / Content View"
        organic_prefix = "Organic Landing > Product View / Content View"

        assert paid_prefix not in out["next_best_by_prefix"]
        assert organic_prefix in out["next_best_by_prefix"]
        kept = out["next_best_by_prefix"][organic_prefix]
        assert len(kept) == 1
        assert kept[0]["step"] == "Checkout / Form Submit"
        assert kept[0]["is_promoted_policy"] is True
        assert kept[0]["promoted_policy_hypothesis_id"] == "hyp-adapter-policy"
        assert out["nba_config"]["promoted_journey_policies"][0]["hypothesis_id"] == "hyp-adapter-policy"
    finally:
        db.close()


def test_analysis_prefers_definition_with_rows_in_requested_window():
    db = _unit_db_session()
    try:
        newer = JourneyDefinition(
            id="jd-newer-empty-window",
            name="Newer Journey",
            conversion_kpi_id="purchase",
            lookback_window_days=30,
            mode_default="conversion_only",
            created_by="test",
            updated_by="test",
            is_archived=False,
            created_at=datetime(2026, 2, 10, tzinfo=timezone.utc),
            updated_at=datetime(2026, 2, 10, tzinfo=timezone.utc),
        )
        older = JourneyDefinition(
            id="jd-older-has-window-data",
            name="Older Journey",
            conversion_kpi_id="purchase",
            lookback_window_days=30,
            mode_default="conversion_only",
            created_by="test",
            updated_by="test",
            is_archived=False,
            created_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
            updated_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        )
        db.add_all([newer, older])
        db.add(
            JourneyPathDaily(
                date=date(2026, 1, 1),
                journey_definition_id="jd-newer-empty-window",
                path_hash="newer-old",
                path_steps=["Test", "Purchase / Lead Won (conversion)"],
                path_length=2,
                count_journeys=5,
                count_conversions=5,
                gross_conversions_total=5.0,
                net_conversions_total=5.0,
                gross_revenue_total=500.0,
                net_revenue_total=500.0,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
        )
        db.add(
            JourneyPathDaily(
                date=date(2026, 2, 1),
                journey_definition_id="jd-older-has-window-data",
                path_hash="older-window",
                path_steps=["Paid Landing", "Purchase / Lead Won (conversion)"],
                path_length=2,
                count_journeys=9,
                count_conversions=9,
                gross_conversions_total=9.0,
                net_conversions_total=9.0,
                gross_revenue_total=900.0,
                net_revenue_total=900.0,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
        )
        db.commit()

        out = build_conversion_paths_analysis_from_daily(
            db,
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 2),
            direct_mode="include",
            path_scope="converted",
        )

        assert out["total_journeys"] == 9
        assert out["journey_definition_id"] == "jd-older-has-window-data"
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
        assert out["summary"]["gross_revenue"] == 800.0
        assert out["summary"]["avg_value"] == 100.0
        assert out["step_breakdown"]
    finally:
        db.close()


def test_analysis_falls_back_to_definition_facts_when_daily_rows_absent():
    db = _unit_db_session()
    try:
        jd = JourneyDefinition(
            id="jd-adapter-fallback",
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
        db.add(
            JourneyDefinitionInstanceFact(
                date=date(2026, 2, 1),
                journey_definition_id="jd-adapter-fallback",
                conversion_id="conv-1",
                profile_id="p-1",
                conversion_key="purchase",
                conversion_ts=datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc),
                path_hash="p1",
                steps_json=["Paid Landing", "Product View / Content View", "Purchase / Lead Won (conversion)"],
                path_length=3,
                channel_group="paid",
                last_touch_channel="email",
                campaign_id="cmp-1",
                device="mobile",
                country="US",
                interaction_path_type="click_through",
                time_to_convert_sec=172800.0,
                gross_conversions_total=1.0,
                net_conversions_total=1.0,
                gross_revenue_total=100.0,
                net_revenue_total=95.0,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
        )
        db.commit()

        out = build_conversion_paths_analysis_from_daily(
            db,
            definition_id="jd-adapter-fallback",
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 28),
            direct_mode="include",
            path_scope="all",
        )

        assert out["source"] == "journey_definition_facts"
        assert out["total_journeys"] == 1
        assert out["common_paths"][0]["gross_revenue"] == 100.0
    finally:
        db.close()


def test_analysis_without_definition_prefers_active_definition_with_data():
    db = _unit_db_session()
    try:
        newer_empty = JourneyDefinition(
            id="jd-empty-newer",
            name="New Empty Journey",
            conversion_kpi_id="purchase",
            lookback_window_days=30,
            mode_default="conversion_only",
            created_by="test",
            updated_by="test",
            is_archived=False,
            created_at=datetime(2026, 2, 5, tzinfo=timezone.utc),
            updated_at=datetime(2026, 2, 5, tzinfo=timezone.utc),
        )
        older_with_data = JourneyDefinition(
            id="jd-with-data",
            name="Journey With Data",
            conversion_kpi_id="purchase",
            lookback_window_days=30,
            mode_default="conversion_only",
            created_by="test",
            updated_by="test",
            is_archived=False,
            created_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
            updated_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        )
        db.add_all([newer_empty, older_with_data])
        db.add(
            JourneyPathDaily(
                date=date(2026, 2, 2),
                journey_definition_id="jd-with-data",
                path_hash="p-seeded",
                path_steps=["Paid Landing", "Checkout", "Purchase / Lead Won (conversion)"],
                path_length=3,
                count_journeys=9,
                count_conversions=4,
                gross_conversions_total=4.0,
                net_conversions_total=4.0,
                gross_revenue_total=400.0,
                net_revenue_total=400.0,
                view_through_conversions_total=0.0,
                click_through_conversions_total=4.0,
                mixed_path_conversions_total=0.0,
                avg_time_to_convert_sec=86400.0,
                p50_time_to_convert_sec=80000.0,
                p90_time_to_convert_sec=120000.0,
                channel_group="paid",
                campaign_id="cmp-seeded",
                device="mobile",
                country="US",
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
        )
        db.commit()

        out = build_conversion_paths_analysis_from_daily(db)

        assert out["journey_definition_id"] == "jd-with-data"
        assert out["total_journeys"] == 9
        assert out["common_paths"][0]["path"] == "Paid Landing > Checkout > Purchase / Lead Won (conversion)"
    finally:
        db.close()
