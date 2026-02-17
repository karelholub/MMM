from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import ConversionPath, FunnelDefinition, JourneyDefinition
from app.services_funnels import get_funnel_diagnostics


def _unit_db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _mk_payload(
    *,
    customer_id: str,
    browser: str,
    country: str,
    device: str,
    consent_opt_out: bool,
    landing_group: str,
    has_error: bool,
    base_ts: str,
    add_to_cart_ts: str | None,
) -> dict:
    tps = [
        {"ts": base_ts, "channel": "google_ads", "event_name": "landing"},
        {"ts": "2026-02-10T10:10:00Z" if "2026-02" in base_ts else "2026-01-10T10:10:00Z", "channel": "google_ads", "event_name": "page_view"},
    ]
    if has_error:
        tps.append(
            {
                "ts": "2026-02-10T10:15:00Z" if "2026-02" in base_ts else "2026-01-10T10:15:00Z",
                "channel": "google_ads",
                "event_name": "checkout_error",
            }
        )
    if add_to_cart_ts:
        tps.append({"ts": add_to_cart_ts, "channel": "google_ads", "event_name": "add_to_cart"})
    return {
        "journey_id": f"j-{customer_id}",
        "customer": {"id": customer_id, "type": "profile_id"},
        "touchpoints": tps,
        "conversions": [{"id": f"cv-{customer_id}", "name": "purchase", "value": 100.0, "currency": "USD"}],
        "browser": browser,
        "country": country,
        "device": device,
        "consent_opt_out": consent_opt_out,
        "landing_page_group": landing_group,
    }


def test_funnel_diagnostics_evidence_fields_present():
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
        funnel = FunnelDefinition(
            id="f-1",
            journey_definition_id="jd-1",
            workspace_id="default",
            user_id="u1",
            name="Checkout funnel",
            steps_json=[
                "Paid Landing",
                "Product View / Content View",
                "Add to Cart / Form Start",
                "Purchase / Lead Won (conversion)",
            ],
            counting_method="ordered",
            window_days=30,
            is_archived=False,
            created_by="test",
            updated_by="test",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add(jd)
        db.add(funnel)

        # Current period (2026-02-10..2026-02-12): slower + skew changes
        curr_rows = [
            ConversionPath(
                conversion_id="c1",
                profile_id="u1",
                conversion_key="purchase",
                conversion_ts=datetime(2026, 2, 10, 13, 0, 0),
                path_json=_mk_payload(
                    customer_id="u1",
                    browser="safari",
                    country="US",
                    device="mobile",
                    consent_opt_out=True,
                    landing_group="pricing",
                    has_error=True,
                    base_ts="2026-02-10T10:00:00Z",
                    add_to_cart_ts="2026-02-10T12:20:00Z",  # big lag from product step
                ),
                path_hash="h1",
                length=3,
                first_touch_ts=datetime(2026, 2, 10, 10, 0, 0),
                last_touch_ts=datetime(2026, 2, 10, 12, 20, 0),
            ),
            ConversionPath(
                conversion_id="c2",
                profile_id="u2",
                conversion_key="purchase",
                conversion_ts=datetime(2026, 2, 11, 13, 0, 0),
                path_json=_mk_payload(
                    customer_id="u2",
                    browser="safari",
                    country="US",
                    device="mobile",
                    consent_opt_out=True,
                    landing_group="pricing",
                    has_error=False,
                    base_ts="2026-02-11T10:00:00Z",
                    add_to_cart_ts=None,  # drop before next step
                ),
                path_hash="h2",
                length=2,
                first_touch_ts=datetime(2026, 2, 11, 10, 0, 0),
                last_touch_ts=datetime(2026, 2, 11, 10, 10, 0),
            ),
        ]

        # Previous equal period (2026-02-07..2026-02-09): faster, different skew
        prev_rows = [
            ConversionPath(
                conversion_id="p1",
                profile_id="p1",
                conversion_key="purchase",
                conversion_ts=datetime(2026, 2, 8, 11, 0, 0),
                path_json=_mk_payload(
                    customer_id="p1",
                    browser="chrome",
                    country="CA",
                    device="desktop",
                    consent_opt_out=False,
                    landing_group="content",
                    has_error=False,
                    base_ts="2026-02-08T10:00:00Z",
                    add_to_cart_ts="2026-02-08T10:18:00Z",
                ),
                path_hash="p1",
                length=3,
                first_touch_ts=datetime(2026, 2, 8, 10, 0, 0),
                last_touch_ts=datetime(2026, 2, 8, 10, 18, 0),
            ),
            ConversionPath(
                conversion_id="p2",
                profile_id="p2",
                conversion_key="purchase",
                conversion_ts=datetime(2026, 2, 9, 11, 0, 0),
                path_json=_mk_payload(
                    customer_id="p2",
                    browser="chrome",
                    country="CA",
                    device="desktop",
                    consent_opt_out=False,
                    landing_group="content",
                    has_error=False,
                    base_ts="2026-02-09T10:00:00Z",
                    add_to_cart_ts="2026-02-09T10:20:00Z",
                ),
                path_hash="p2",
                length=3,
                first_touch_ts=datetime(2026, 2, 9, 10, 0, 0),
                last_touch_ts=datetime(2026, 2, 9, 10, 20, 0),
            ),
        ]

        db.add_all(curr_rows + prev_rows)
        db.commit()

        out = get_funnel_diagnostics(
            db,
            funnel=funnel,
            journey_definition=jd,
            step="Product View / Content View",
            date_from=date(2026, 2, 10),
            date_to=date(2026, 2, 12),
        )
        assert isinstance(out, list)
        assert len(out) >= 1
        for item in out:
            assert isinstance(item.get("title"), str) and item["title"].strip()
            assert isinstance(item.get("evidence"), list) and item["evidence"]
            assert isinstance(item.get("impact_estimate"), dict)
            assert isinstance(item.get("confidence"), str) and item["confidence"] in {"low", "medium", "high"}
            assert isinstance(item.get("next_action"), str) and item["next_action"].strip()
    finally:
        db.close()


def test_funnel_diagnostics_insufficient_when_step_not_found():
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
        funnel = FunnelDefinition(
            id="f-2",
            journey_definition_id="jd-2",
            workspace_id="default",
            user_id="u1",
            name="F2",
            steps_json=["Paid Landing", "Product View / Content View"],
            counting_method="ordered",
            window_days=30,
            is_archived=False,
            created_by="test",
            updated_by="test",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add(jd)
        db.add(funnel)
        db.commit()

        out = get_funnel_diagnostics(
            db,
            funnel=funnel,
            journey_definition=jd,
            step="Unknown Step",
            date_from=date(2026, 2, 1),
            date_to=date(2026, 2, 7),
        )
        assert len(out) == 1
        assert "Insufficient evidence" in out[0]["title"]
        assert out[0]["evidence"]
    finally:
        db.close()
