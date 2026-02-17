from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import ConversionPath, JourneyDefinition
from app.services_journey_aggregates import _build_journey_steps, _path_hash
from app.services_journey_attribution import build_journey_attribution_summary


def _unit_db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _v2_payload(*, customer_id: str, value: float, conv_name: str, touchpoints: list) -> dict:
    return {
        "journey_id": f"j-{customer_id}",
        "customer": {"id": customer_id, "type": "profile_id"},
        "defaults": {"timezone": "UTC", "currency": "USD"},
        "touchpoints": touchpoints,
        "conversions": [{"id": f"cv-{customer_id}", "name": conv_name, "value": value, "currency": "USD"}],
        "device": "mobile",
        "country": "US",
    }


def test_attribution_summary_reconciles_with_observed_totals_within_tolerance():
    db = _unit_db_session()
    try:
        jd = JourneyDefinition(
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
        db.add(jd)

        payload1 = _v2_payload(
            customer_id="c1",
            value=120.0,
            conv_name="purchase",
            touchpoints=[
                {"ts": "2026-02-01T08:00:00Z", "channel": "google_ads", "campaign": {"id": "cmp-a", "name": "A"}},
                {"ts": "2026-02-02T08:00:00Z", "channel": "email"},
            ],
        )
        payload2 = _v2_payload(
            customer_id="c2",
            value=80.0,
            conv_name="purchase",
            touchpoints=[
                {"ts": "2026-02-03T08:00:00Z", "channel": "direct"},
                {"ts": "2026-02-04T08:00:00Z", "channel": "meta_ads", "campaign": {"id": "cmp-b", "name": "B"}},
            ],
        )
        row1 = ConversionPath(
            conversion_id="cv-1",
            profile_id="c1",
            conversion_key="purchase",
            conversion_ts=datetime(2026, 2, 2, 9, 0, 0),
            path_json=payload1,
            path_hash="h1",
            length=2,
            first_touch_ts=datetime(2026, 2, 1, 8, 0, 0),
            last_touch_ts=datetime(2026, 2, 2, 8, 0, 0),
        )
        row2 = ConversionPath(
            conversion_id="cv-2",
            profile_id="c2",
            conversion_key="purchase",
            conversion_ts=datetime(2026, 2, 4, 9, 0, 0),
            path_json=payload2,
            path_hash="h2",
            length=2,
            first_touch_ts=datetime(2026, 2, 3, 8, 0, 0),
            last_touch_ts=datetime(2026, 2, 4, 8, 0, 0),
        )
        db.add_all([row1, row2])
        db.commit()

        out = build_journey_attribution_summary(
            db,
            definition=jd,
            date_from="2026-02-01",
            date_to="2026-02-10",
            model="linear",
            include_campaign=True,
        )
        assert out["model"] == "linear"
        assert "by_channel_group" in out and isinstance(out["by_channel_group"], list)
        assert "totals" in out and isinstance(out["totals"], dict)
        observed = out["totals"]["total_value_observed"]
        attributed = out["totals"]["total_value_attributed"]
        assert observed == 200.0
        assert abs(attributed - observed) <= max(1e-6, observed * 0.05)
        assert len(out["by_campaign"]) >= 1
    finally:
        db.close()


def test_attribution_summary_path_hash_scopes_subset():
    db = _unit_db_session()
    try:
        jd = JourneyDefinition(
            id="jd-2",
            name="J2",
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
        payload = _v2_payload(
            customer_id="c3",
            value=55.0,
            conv_name="purchase",
            touchpoints=[
                {"ts": "2026-02-05T08:00:00Z", "channel": "google_ads"},
                {"ts": "2026-02-06T08:00:00Z", "channel": "email"},
            ],
        )
        conversion_ts = datetime(2026, 2, 6, 9, 0, 0, tzinfo=timezone.utc)
        steps, _, _ = _build_journey_steps(payload, conversion_ts=conversion_ts, lookback_window_days=30)
        canonical_hash = _path_hash(steps)
        db.add(
            ConversionPath(
                conversion_id="cv-3",
                profile_id="c3",
                conversion_key="purchase",
                conversion_ts=conversion_ts.replace(tzinfo=None),
                path_json=payload,
                path_hash="h3",
                length=2,
                first_touch_ts=datetime(2026, 2, 5, 8, 0, 0),
                last_touch_ts=datetime(2026, 2, 6, 8, 0, 0),
            )
        )
        db.commit()

        out = build_journey_attribution_summary(
            db,
            definition=jd,
            date_from="2026-02-01",
            date_to="2026-02-10",
            model="first_touch",
            path_hash=canonical_hash,
        )
        assert out["path_hash"] == canonical_hash
        assert out["totals"]["journeys"] == 1
        assert out["totals"]["total_value_observed"] == 55.0
    finally:
        db.close()
