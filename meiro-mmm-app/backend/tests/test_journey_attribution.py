from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import ConversionPath, JourneyDefinition, JourneyInstanceFact, JourneyRoleFact, JourneyStepFact, SilverConversionFact, SilverTouchpointFact
from app.services_conversions import persist_journeys_as_conversion_paths
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


def test_attribution_summary_compresses_repeated_paths_for_linear_model():
    db = _unit_db_session()
    try:
        jd = JourneyDefinition(
            id="jd-3",
            name="J3",
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

        for idx in range(3):
            payload = _v2_payload(
                customer_id=f"dup-{idx}",
                value=50.0,
                conv_name="purchase",
                touchpoints=[
                    {"ts": f"2026-02-0{idx + 1}T08:00:00Z", "channel": "google_ads"},
                    {"ts": f"2026-02-0{idx + 1}T09:00:00Z", "channel": "email"},
                ],
            )
            db.add(
                ConversionPath(
                    conversion_id=f"cv-dup-{idx}",
                    profile_id=f"dup-{idx}",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 2, idx + 1, 10, 0, 0),
                    path_json=payload,
                    path_hash=f"h-dup-{idx}",
                    length=2,
                    first_touch_ts=datetime(2026, 2, idx + 1, 8, 0, 0),
                    last_touch_ts=datetime(2026, 2, idx + 1, 9, 0, 0),
                )
            )
        db.commit()

        out = build_journey_attribution_summary(
            db,
            definition=jd,
            date_from="2026-02-01",
            date_to="2026-02-10",
            model="linear",
            include_campaign=False,
        )

        assert out["totals"]["journeys"] == 3
        assert out["totals"]["total_value_observed"] == 150.0
        assert out["approximation"]["method"] == "compressed_journey_instances"
        assert out["approximation"]["compressed_journeys"] == 1
        by_channel = {row["channel"]: row["attributed_value"] for row in out["by_channel"]}
        assert by_channel["google_ads"] == 75.0
        assert by_channel["email"] == 75.0
    finally:
        db.close()


def test_attribution_summary_uses_silver_when_conversion_paths_absent():
    db = _unit_db_session()
    try:
        jd = JourneyDefinition(
            id="jd-silver",
            name="Silver Attribution",
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
        db.commit()

        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-1"},
                    "touchpoints": [
                        {"ts": "2026-02-01T08:00:00Z", "channel": "google_ads"},
                        {"ts": "2026-02-02T08:00:00Z", "channel": "email"},
                    ],
                    "conversions": [{"id": "conv-1", "name": "purchase", "ts": "2026-02-02T09:00:00Z", "value": 120.0}],
                },
                {
                    "_schema": "v2",
                    "customer": {"id": "cust-2"},
                    "touchpoints": [
                        {"ts": "2026-02-03T08:00:00Z", "channel": "direct"},
                        {"ts": "2026-02-04T08:00:00Z", "channel": "meta_ads", "campaign": "B"},
                    ],
                    "conversions": [{"id": "conv-2", "name": "purchase", "ts": "2026-02-04T09:00:00Z", "value": 80.0}],
                },
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="silver-attribution-batch",
        )
        assert inserted == 2

        db.query(ConversionPath).delete(synchronize_session=False)
        db.commit()

        out = build_journey_attribution_summary(
            db,
            definition=jd,
            date_from="2026-02-01",
            date_to="2026-02-10",
            model="linear",
            include_campaign=True,
        )

        assert out["totals"]["journeys"] == 2
        assert out["totals"]["total_value_observed"] == 200.0
        assert abs(out["totals"]["total_value_attributed"] - 200.0) <= 10.0
        assert out["by_channel"]
    finally:
        db.close()


def test_attribution_summary_uses_instance_facts_when_conversion_and_silver_paths_absent():
    db = _unit_db_session()
    try:
        jd = JourneyDefinition(
            id="jd-instance",
            name="Instance Journey",
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
        db.commit()

        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                _v2_payload(
                    customer_id="i1",
                    value=120.0,
                    conv_name="purchase",
                    touchpoints=[
                        {"ts": "2026-02-01T08:00:00Z", "channel": "google_ads", "campaign": {"id": "cmp-a", "name": "A"}},
                        {"ts": "2026-02-02T08:00:00Z", "channel": "email"},
                    ],
                ),
                _v2_payload(
                    customer_id="i2",
                    value=80.0,
                    conv_name="purchase",
                    touchpoints=[
                        {"ts": "2026-02-03T08:00:00Z", "channel": "direct"},
                        {"ts": "2026-02-04T08:00:00Z", "channel": "meta_ads", "campaign": {"id": "cmp-b", "name": "B"}},
                    ],
                ),
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="instance-attribution-batch",
        )
        assert inserted == 2

        db.query(ConversionPath).delete(synchronize_session=False)
        db.query(SilverTouchpointFact).delete(synchronize_session=False)
        db.query(SilverConversionFact).delete(synchronize_session=False)
        db.commit()

        out = build_journey_attribution_summary(
            db,
            definition=jd,
            date_from="2026-02-01",
            date_to="2026-02-10",
            model="linear",
            include_campaign=True,
        )
        assert out["totals"]["journeys"] == 2
        assert out["totals"]["total_value_observed"] == 200.0
        assert out["by_channel"]
    finally:
        db.close()


def test_attribution_summary_uses_role_facts_for_first_touch_when_paths_and_instances_absent():
    db = _unit_db_session()
    try:
        jd = JourneyDefinition(
            id="jd-role",
            name="Role Journey",
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
        db.commit()

        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                _v2_payload(
                    customer_id="r1",
                    value=120.0,
                    conv_name="purchase",
                    touchpoints=[
                        {"ts": "2026-02-01T08:00:00Z", "channel": "google_ads", "campaign": {"id": "cmp-a", "name": "A"}},
                        {"ts": "2026-02-02T08:00:00Z", "channel": "email"},
                    ],
                ),
                _v2_payload(
                    customer_id="r2",
                    value=80.0,
                    conv_name="purchase",
                    touchpoints=[
                        {"ts": "2026-02-03T08:00:00Z", "channel": "direct"},
                        {"ts": "2026-02-04T08:00:00Z", "channel": "meta_ads", "campaign": {"id": "cmp-b", "name": "B"}},
                    ],
                ),
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="role-attribution-batch",
        )
        assert inserted == 2
        db.query(ConversionPath).delete(synchronize_session=False)
        db.query(SilverTouchpointFact).delete(synchronize_session=False)
        db.query(SilverConversionFact).delete(synchronize_session=False)
        db.query(JourneyStepFact).delete(synchronize_session=False)
        db.query(JourneyInstanceFact).delete(synchronize_session=False)
        db.commit()

        out = build_journey_attribution_summary(
            db,
            definition=jd,
            date_from="2026-02-01",
            date_to="2026-02-10",
            model="first_touch",
            include_campaign=True,
        )

        assert db.query(JourneyRoleFact).count() >= 4
        assert out["approximation"]["method"] == "role_facts_exact"
        assert out["totals"]["journeys"] == 2
        assert out["totals"]["total_value_observed"] == 200.0
        by_channel = {row["channel"]: row["attributed_value"] for row in out["by_channel"]}
        assert by_channel["google_ads"] == 120.0
        assert by_channel["direct"] == 80.0
        assert any(row["campaign"] == "A" for row in out["by_campaign"])
    finally:
        db.close()


def test_attribution_summary_uses_step_facts_for_linear_when_paths_and_instances_absent():
    db = _unit_db_session()
    try:
        jd = JourneyDefinition(
            id="jd-step-linear",
            name="Step Linear Journey",
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
        db.commit()

        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                _v2_payload(
                    customer_id="sl1",
                    value=120.0,
                    conv_name="purchase",
                    touchpoints=[
                        {"ts": "2026-02-01T08:00:00Z", "channel": "google_ads"},
                        {"ts": "2026-02-02T08:00:00Z", "channel": "email"},
                    ],
                )
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="step-linear-batch",
        )
        assert inserted == 1
        db.query(ConversionPath).delete(synchronize_session=False)
        db.query(SilverTouchpointFact).delete(synchronize_session=False)
        db.query(SilverConversionFact).delete(synchronize_session=False)
        db.query(JourneyRoleFact).delete(synchronize_session=False)
        db.query(JourneyInstanceFact).update({JourneyInstanceFact.gross_revenue_total: 120.0}, synchronize_session=False)
        db.commit()

        out = build_journey_attribution_summary(
            db,
            definition=jd,
            date_from="2026-02-01",
            date_to="2026-02-10",
            model="linear",
            include_campaign=False,
        )

        assert out["approximation"]["method"] == "step_facts_exact"
        by_channel = {row["channel"]: row["attributed_value"] for row in out["by_channel"]}
        assert by_channel["google_ads"] == 60.0
        assert by_channel["email"] == 60.0
    finally:
        db.close()


def test_attribution_summary_uses_step_facts_for_position_based_when_paths_and_instances_absent():
    db = _unit_db_session()
    try:
        jd = JourneyDefinition(
            id="jd-step-position",
            name="Step Position Journey",
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
        db.commit()

        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                _v2_payload(
                    customer_id="sp1",
                    value=100.0,
                    conv_name="purchase",
                    touchpoints=[
                        {"ts": "2026-02-01T08:00:00Z", "channel": "google_ads"},
                        {"ts": "2026-02-01T09:00:00Z", "channel": "email", "event_name": "add_to_cart"},
                        {"ts": "2026-02-01T10:00:00Z", "channel": "direct", "action": "checkout"},
                    ],
                )
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="step-position-batch",
        )
        assert inserted == 1
        db.query(ConversionPath).delete(synchronize_session=False)
        db.query(SilverTouchpointFact).delete(synchronize_session=False)
        db.query(SilverConversionFact).delete(synchronize_session=False)
        db.query(JourneyRoleFact).delete(synchronize_session=False)
        db.commit()

        out = build_journey_attribution_summary(
            db,
            definition=jd,
            date_from="2026-02-01",
            date_to="2026-02-10",
            model="position_based",
            include_campaign=False,
        )

        assert out["approximation"]["method"] == "step_facts_exact"
        by_channel = {row["channel"]: row["attributed_value"] for row in out["by_channel"]}
        assert by_channel["google_ads"] == 40.0
        assert by_channel["email"] == 20.0
        assert by_channel["direct"] == 40.0
    finally:
        db.close()


def test_attribution_summary_uses_step_facts_for_time_decay_when_paths_and_instances_absent():
    db = _unit_db_session()
    try:
        jd = JourneyDefinition(
            id="jd-step-decay",
            name="Step Decay Journey",
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
        db.commit()

        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                _v2_payload(
                    customer_id="sd1",
                    value=100.0,
                    conv_name="purchase",
                    touchpoints=[
                        {"ts": "2026-02-01T08:00:00Z", "channel": "google_ads"},
                        {"ts": "2026-02-05T08:00:00Z", "channel": "email"},
                    ],
                )
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="step-decay-batch",
        )
        assert inserted == 1
        db.query(ConversionPath).delete(synchronize_session=False)
        db.query(SilverTouchpointFact).delete(synchronize_session=False)
        db.query(SilverConversionFact).delete(synchronize_session=False)
        db.query(JourneyRoleFact).delete(synchronize_session=False)
        db.commit()

        out = build_journey_attribution_summary(
            db,
            definition=jd,
            date_from="2026-02-01",
            date_to="2026-02-10",
            model="time_decay",
            include_campaign=False,
        )

        assert out["approximation"]["method"] == "step_facts_exact"
        by_channel = {row["channel"]: row["attributed_value"] for row in out["by_channel"]}
        assert by_channel["email"] > by_channel["google_ads"]
    finally:
        db.close()


def test_attribution_summary_uses_step_facts_for_markov_when_paths_and_instances_absent():
    db = _unit_db_session()
    try:
        jd = JourneyDefinition(
            id="jd-step-markov",
            name="Step Markov Journey",
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
        db.commit()

        inserted = persist_journeys_as_conversion_paths(
            db,
            [
                _v2_payload(
                    customer_id="sm1",
                    value=100.0,
                    conv_name="purchase",
                    touchpoints=[
                        {"ts": "2026-02-01T08:00:00Z", "channel": "google_ads"},
                        {"ts": "2026-02-01T09:00:00Z", "channel": "email"},
                    ],
                ),
                _v2_payload(
                    customer_id="sm2",
                    value=80.0,
                    conv_name="purchase",
                    touchpoints=[
                        {"ts": "2026-02-02T08:00:00Z", "channel": "google_ads"},
                        {"ts": "2026-02-02T09:00:00Z", "channel": "direct"},
                    ],
                ),
            ],
            replace=True,
            import_source="meiro_events_replay",
            import_batch_id="step-markov-batch",
        )
        assert inserted == 2
        db.query(ConversionPath).delete(synchronize_session=False)
        db.query(SilverTouchpointFact).delete(synchronize_session=False)
        db.query(SilverConversionFact).delete(synchronize_session=False)
        db.query(JourneyRoleFact).delete(synchronize_session=False)
        db.commit()

        out = build_journey_attribution_summary(
            db,
            definition=jd,
            date_from="2026-02-01",
            date_to="2026-02-10",
            model="markov",
            include_campaign=False,
        )

        assert out["approximation"]["method"] == "step_facts_exact"
        assert out["totals"]["journeys"] == 2
        assert out["by_channel"]
        assert abs(out["totals"]["total_value_attributed"] - 180.0) <= 0.1
    finally:
        db.close()
