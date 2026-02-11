"""
Unit tests for the Alerts engine: anomaly (z-score) and data_freshness evaluators.
Deterministic: fixed timestamps and seeded data where needed.
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_overview_alerts import AlertEvent, AlertRule, MetricSnapshot
from app.services_alerts_engine import (
    make_fingerprint,
    period_id_for_schedule,
    _zscore,
    get_kpi_series_for_baseline,
    evaluate_anomaly_kpi,
    evaluate_data_freshness,
    evaluate_threshold,
    evaluate_pipeline_health,
    run_alerts_engine,
)


# In-memory SQLite for deterministic tests
@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def fixed_now():
    return datetime(2024, 2, 15, 14, 30, 0)


# ---------------------------------------------------------------------------
# Fingerprint and period (deterministic)
# ---------------------------------------------------------------------------


def test_make_fingerprint_deterministic():
    fp1 = make_fingerprint(1, {"channel": "paid"}, "2024-02-15")
    fp2 = make_fingerprint(1, {"channel": "paid"}, "2024-02-15")
    assert fp1 == fp2
    assert len(fp1) <= 64
    fp_other = make_fingerprint(1, {"channel": "organic"}, "2024-02-15")
    assert fp1 != fp_other


def test_period_id_for_schedule():
    t = datetime(2024, 2, 15, 14, 30)
    assert period_id_for_schedule(t, "daily") == "2024-02-15"
    assert period_id_for_schedule(t, "hourly") == "2024-02-15T14"


# ---------------------------------------------------------------------------
# Z-score (pure function, deterministic)
# ---------------------------------------------------------------------------


def test_zscore_basic():
    baseline = [10.0, 12.0, 11.0, 13.0, 10.0]  # mean=11.2, std ~1.17
    expected, observed, z = _zscore(14.0, baseline)
    assert expected == 11.2
    assert observed == 14.0
    assert z > 2.0  # above mean


def test_zscore_zero_std():
    baseline = [5.0, 5.0, 5.0]
    expected, observed, z = _zscore(5.0, baseline)
    assert expected == 5.0
    assert z == 0.0


def test_zscore_empty_baseline():
    expected, observed, z = _zscore(10.0, [])
    assert expected == 0.0
    assert observed == 10.0
    assert z == 0.0


# ---------------------------------------------------------------------------
# Anomaly evaluator (with seeded MetricSnapshot)
# ---------------------------------------------------------------------------


def test_evaluate_anomaly_kpi_insufficient_baseline(db_session, fixed_now):
    """No baseline => no outcome (no spam)."""
    rule = AlertRule(
        id=1,
        name="Test anomaly",
        scope="default",
        rule_type="anomaly_kpi",
        kpi_key="conversions",
        params_json={"zscore_threshold": 2.5, "lookback_periods": 28},
        schedule="daily",
        severity="warn",
        is_enabled=True,
        created_by="test",
    )
    db_session.add(rule)
    db_session.commit()

    outcomes = evaluate_anomaly_kpi(db_session, rule, "default", fixed_now)
    # No MetricSnapshot data => no current value or no baseline
    assert isinstance(outcomes, list)
    assert len(outcomes) == 0


def test_evaluate_anomaly_kpi_with_baseline_triggers(db_session, fixed_now):
    """Seed 28 same-weekday values; current period high => triggered."""
    # Create rule
    rule = AlertRule(
        id=1,
        name="Conversions anomaly",
        scope="default",
        rule_type="anomaly_kpi",
        kpi_key="conversions",
        params_json={"zscore_threshold": 2.5, "lookback_periods": 28, "min_volume": 1},
        schedule="daily",
        severity="warn",
        is_enabled=True,
        created_by="test",
    )
    db_session.add(rule)
    db_session.flush()

    # Same weekday as 2024-02-15 is Thursday (weekday=3). Seed 28 Thursdays with slight variance so std > 0.
    base = datetime(2023, 8, 1)
    for i in range(28):
        d = base + timedelta(days=7 * i)
        while d.weekday() != 3:
            d += timedelta(days=1)
        ts = datetime(d.year, d.month, d.day, 12, 0, 0)
        val = 10.0 + (i % 3)  # 10, 11, 12, 10, 11, ... so mean ~11, std > 0
        snap = MetricSnapshot(
            ts=ts,
            scope="default",
            kpi_key="conversions",
            kpi_value=val,
            dimensions_json=None,
            computed_from="raw",
        )
        db_session.add(snap)
    # Current period (2024-02-15) with value 25 => z-score high
    snap_today = MetricSnapshot(
        ts=fixed_now,
        scope="default",
        kpi_key="conversions",
        kpi_value=25.0,
        dimensions_json=None,
        computed_from="raw",
    )
    db_session.add(snap_today)
    db_session.commit()

    outcomes = evaluate_anomaly_kpi(db_session, rule, "default", fixed_now)
    assert len(outcomes) == 1
    triggered, context, fingerprint, period_id = outcomes[0]
    assert period_id == "2024-02-15"
    assert context["kpi_key"] == "conversions"
    assert context["observed"] == 25.0
    assert 10.0 <= context["expected"] <= 12.0  # baseline mean ~11
    assert context["zscore"] >= 2.5
    assert triggered is True


def test_evaluate_anomaly_kpi_below_min_volume_no_trigger(db_session, fixed_now):
    """Below min_volume => no outcome (guardrail)."""
    rule = AlertRule(
        id=1,
        name="Anomaly min volume",
        scope="default",
        rule_type="anomaly_kpi",
        kpi_key="conversions",
        params_json={"zscore_threshold": 2.5, "min_volume": {"conversions": 100}},
        schedule="daily",
        severity="warn",
        is_enabled=True,
        created_by="test",
    )
    db_session.add(rule)
    db_session.commit()
    # No snapshots => observed would be None or from paths; with min_volume 100 and no data we get no outcome
    outcomes = evaluate_anomaly_kpi(db_session, rule, "default", fixed_now)
    assert len(outcomes) == 0


# ---------------------------------------------------------------------------
# Data freshness evaluator (deterministic with mocked get_freshness)
# ---------------------------------------------------------------------------


def test_evaluate_data_freshness_stale_triggers(db_session, fixed_now):
    """Last touchpoint older than max_age_minutes => triggered."""
    rule = AlertRule(
        id=1,
        name="Freshness 1h",
        scope="default",
        rule_type="data_freshness",
        params_json={"max_age_minutes": 60},
        schedule="hourly",
        severity="warn",
        is_enabled=True,
        created_by="test",
    )
    db_session.add(rule)
    db_session.commit()

    # Mock: last touch 2 hours ago
    last_ts = fixed_now - timedelta(hours=2)
    def get_freshness():
        return {
            "last_touchpoint_ts": last_ts.isoformat(),
            "last_conversion_ts": last_ts.isoformat(),
        }

    outcomes = evaluate_data_freshness(db_session, rule, "default", fixed_now, get_freshness_fn=get_freshness)
    assert len(outcomes) == 1
    triggered, context, fp, period_id = outcomes[0]
    assert triggered is True
    assert context["age_minutes"] >= 119
    assert context["max_age_minutes"] == 60


def test_evaluate_data_freshness_fresh_no_trigger(db_session, fixed_now):
    """Last touchpoint within max_age => not triggered."""
    rule = AlertRule(
        id=1,
        name="Freshness 24h",
        scope="default",
        rule_type="data_freshness",
        params_json={"max_age_minutes": 60 * 24},
        schedule="hourly",
        severity="warn",
        is_enabled=True,
        created_by="test",
    )
    db_session.add(rule)
    db_session.commit()

    last_ts = fixed_now - timedelta(minutes=30)
    def get_freshness():
        return {
            "last_touchpoint_ts": last_ts.isoformat(),
            "last_conversion_ts": last_ts.isoformat(),
        }

    outcomes = evaluate_data_freshness(db_session, rule, "default", fixed_now, get_freshness_fn=get_freshness)
    assert len(outcomes) == 1
    triggered, context, _, _ = outcomes[0]
    assert triggered is False
    assert context["age_minutes"] == 30.0


def test_evaluate_data_freshness_no_data_triggers(db_session, fixed_now):
    """No touchpoint/conversion data => triggered."""
    rule = AlertRule(
        id=1,
        name="Freshness no data",
        scope="default",
        rule_type="data_freshness",
        params_json={"max_age_minutes": 60},
        schedule="daily",
        severity="critical",
        is_enabled=True,
        created_by="test",
    )
    db_session.add(rule)
    db_session.commit()

    def get_freshness():
        return {"last_touchpoint_ts": None, "last_conversion_ts": None}

    outcomes = evaluate_data_freshness(db_session, rule, "default", fixed_now, get_freshness_fn=get_freshness)
    assert len(outcomes) == 1
    triggered, context, _, _ = outcomes[0]
    assert triggered is True
    assert context.get("message") or context.get("age_minutes") is None


# ---------------------------------------------------------------------------
# Threshold evaluator
# ---------------------------------------------------------------------------


def test_evaluate_threshold_no_current_value(db_session, fixed_now):
    """No KPI data from snapshots; fallback from paths gives 0 => one outcome (0 < 10 triggered)."""
    rule = AlertRule(
        id=1,
        name="Threshold",
        scope="default",
        rule_type="threshold",
        kpi_key="conversions",
        params_json={"threshold_value": 10, "operator": "<"},
        schedule="daily",
        severity="warn",
        is_enabled=True,
        created_by="test",
    )
    db_session.add(rule)
    db_session.commit()
    outcomes = evaluate_threshold(db_session, rule, "default", fixed_now)
    # With empty DB, get_current_kpi_from_paths_and_expenses returns 0 for conversions => 0 < 10 triggers
    assert len(outcomes) == 1
    triggered, context, _, _ = outcomes[0]
    assert context["observed"] == 0.0
    assert triggered is True


def test_evaluate_threshold_with_snapshot(db_session, fixed_now):
    """Snapshot value below threshold => triggered."""
    rule = AlertRule(
        id=1,
        name="Conv below 5",
        scope="default",
        rule_type="threshold",
        kpi_key="conversions",
        params_json={"threshold_value": 5, "operator": "<"},
        schedule="daily",
        severity="warn",
        is_enabled=True,
        created_by="test",
    )
    db_session.add(rule)
    db_session.flush()
    snap = MetricSnapshot(
        ts=fixed_now,
        scope="default",
        kpi_key="conversions",
        kpi_value=3.0,
        dimensions_json=None,
        computed_from="raw",
    )
    db_session.add(snap)
    db_session.commit()

    outcomes = evaluate_threshold(db_session, rule, "default", fixed_now)
    assert len(outcomes) == 1
    triggered, context, _, _ = outcomes[0]
    assert triggered is True
    assert context["observed"] == 3.0
    assert context["operator"] == "<"


# ---------------------------------------------------------------------------
# Pipeline health (deterministic with no expenses/conversions)
# ---------------------------------------------------------------------------


def test_evaluate_pipeline_health_no_mismatch(db_session, fixed_now):
    """No spend and no conversions => not triggered (or both zero)."""
    rule = AlertRule(
        id=1,
        name="Pipeline health",
        scope="default",
        rule_type="pipeline_health",
        params_json={"tolerance_hours": 24},
        schedule="hourly",
        severity="warn",
        is_enabled=True,
        created_by="test",
    )
    db_session.add(rule)
    db_session.commit()
    outcomes = evaluate_pipeline_health(db_session, rule, "default", fixed_now, expenses=None)
    assert len(outcomes) == 1
    triggered, context, _, _ = outcomes[0]
    # spend=0, conv_count=0 => no mismatch
    assert triggered is False
    assert context["spend"] == 0
    assert context["conversions"] == 0


# ---------------------------------------------------------------------------
# run_alerts_engine: dedupe and metrics
# ---------------------------------------------------------------------------


def test_run_alerts_engine_metrics_and_dedupe(db_session, fixed_now):
    """Engine runs without error; returns metrics; duplicate fingerprint updates not creates."""
    rule = AlertRule(
        id=1,
        name="Freshness test",
        scope="default",
        rule_type="data_freshness",
        params_json={"max_age_minutes": 60},
        schedule="hourly",
        severity="warn",
        is_enabled=True,
        created_by="test",
    )
    db_session.add(rule)
    db_session.commit()

    def get_freshness():
        return {
            "last_touchpoint_ts": (fixed_now - timedelta(hours=2)).isoformat(),
            "last_conversion_ts": (fixed_now - timedelta(hours=2)).isoformat(),
        }

    # Patch overview_get_freshness so it returns our stale data (engine calls it with db, import_runs).
    import app.services_alerts_engine as engine_mod
    def _get_freshness_mock(db, import_runs_get_last_successful=None):
        return get_freshness()
    original = engine_mod.overview_get_freshness
    try:
        engine_mod.overview_get_freshness = _get_freshness_mock
        metrics = run_alerts_engine(db_session, "default", now=fixed_now)
    finally:
        engine_mod.overview_get_freshness = original

    assert metrics["rules_evaluated"] == 1
    assert metrics["events_created"] == 1
    assert metrics["scope"] == "default"

    # Second run: same condition => update, not create
    import app.services_alerts_engine as engine_mod
    original = engine_mod.overview_get_freshness
    try:
        engine_mod.overview_get_freshness = _get_freshness_mock
        metrics2 = run_alerts_engine(db_session, "default", now=fixed_now)
    finally:
        engine_mod.overview_get_freshness = original

    assert metrics2["events_updated"] == 1
    assert metrics2["events_created"] == 0

    # Resolve: freshness ok => event resolved
    def get_freshness_ok():
        return {
            "last_touchpoint_ts": fixed_now.isoformat(),
            "last_conversion_ts": fixed_now.isoformat(),
        }
    def _get_freshness_ok_mock(db, import_runs_get_last_successful=None):
        return get_freshness_ok()
    import app.services_alerts_engine as engine_mod
    original = engine_mod.overview_get_freshness
    try:
        engine_mod.overview_get_freshness = _get_freshness_ok_mock
        metrics3 = run_alerts_engine(db_session, "default", now=fixed_now)
    finally:
        engine_mod.overview_get_freshness = original

    assert metrics3["events_resolved"] == 1
