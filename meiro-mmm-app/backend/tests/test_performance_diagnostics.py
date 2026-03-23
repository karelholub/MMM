from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import ConversionPath
from app.services_performance_diagnostics import build_scope_diagnostics


def _unit_db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def test_build_scope_diagnostics_for_channel_roles_and_funnel():
    db = _unit_db_session()
    try:
        db.add(
            ConversionPath(
                conversion_id="conv-1",
                profile_id="p-1",
                conversion_key="purchase",
                conversion_ts=datetime(2026, 2, 5, 12, 0),
                path_json={
                    "conversions": [{"name": "purchase", "value": 100}],
                    "touchpoints": [
                        {"channel": "google_ads", "timestamp": "2026-02-01T09:00:00Z"},
                        {"channel": "email", "event_name": "product_view", "timestamp": "2026-02-03T09:00:00Z"},
                        {"channel": "direct", "event_name": "form_submit", "timestamp": "2026-02-05T09:00:00Z"},
                    ],
                    "_revenue_entries": [{"value_in_base": 100, "dedup_key": "conv-1"}],
                },
                path_hash="hash-1",
                length=3,
                first_touch_ts=datetime(2026, 2, 1, 9, 0),
                last_touch_ts=datetime(2026, 2, 5, 9, 0),
            )
        )
        db.commit()

        out = build_scope_diagnostics(
            db=db,
            scope_type="channel",
            date_from="2026-02-01",
            date_to="2026-02-10",
            conversion_key="purchase",
            channels=None,
        )

        assert out["google_ads"]["roles"]["first_touch_conversions"] == 1
        assert out["direct"]["roles"]["last_touch_conversions"] == 1
        assert out["email"]["roles"]["assist_conversions"] == 1
        assert out["email"]["funnel"]["content_journeys"] == 1
        assert out["direct"]["funnel"]["checkout_journeys"] == 1
        assert out["google_ads"]["funnel"]["converted_journeys"] == 1
    finally:
        db.close()
