import math

from app.attribution_engine import (
    last_touch,
    first_touch,
    linear,
    time_decay,
    position_based,
    markov,
    run_attribution,
)


def _simple_journeys():
    # Two customers, three channels, simple values
    return [
        {
            "customer_id": "c1",
            "touchpoints": [
                {"channel": "google"},
                {"channel": "meta"},
            ],
            "conversion_value": 100.0,
            "converted": True,
        },
        {
            "customer_id": "c2",
            "touchpoints": [
                {"channel": "email"},
                {"channel": "google"},
            ],
            "conversion_value": 50.0,
            "converted": True,
        },
    ]


def test_last_touch_simple_all_credit_to_last_channel():
    journeys = _simple_journeys()
    credit = last_touch(journeys)
    # c1 -> meta, c2 -> google
    assert credit == {"meta": 100.0, "google": 50.0}


def test_first_touch_simple_all_credit_to_first_channel():
    journeys = _simple_journeys()
    credit = first_touch(journeys)
    # c1 -> google, c2 -> email
    assert credit == {"google": 100.0, "email": 50.0}


def test_linear_distributes_evenly_across_touchpoints():
    journeys = _simple_journeys()
    credit = linear(journeys)
    # c1: 100 split over 2 steps, c2: 50 split over 2 steps
    # google: 100/2 + 50/2 = 75, meta: 50, email: 25
    assert credit["google"] == 75.0
    assert credit["meta"] == 50.0
    assert credit["email"] == 25.0
    assert math.isclose(sum(credit.values()), 150.0)


def test_time_decay_handles_missing_timestamps_and_positive_credit():
    # Use journeys without timestamps to exercise position-based fallback
    journeys = _simple_journeys()
    credit = time_decay(journeys, half_life_days=7.0)
    # All value should still be attributed somewhere and sum to total conversions value
    total = sum(credit.values())
    assert total > 0
    assert math.isclose(total, 150.0, rel_tol=1e-6)


def test_position_based_single_and_multi_touch_paths():
    journeys = [
        {
            "customer_id": "single",
            "touchpoints": [{"channel": "google"}],
            "conversion_value": 100.0,
            "converted": True,
        },
        {
            "customer_id": "double",
            "touchpoints": [{"channel": "email"}, {"channel": "meta"}],
            "conversion_value": 200.0,
            "converted": True,
        },
        {
            "customer_id": "triple",
            "touchpoints": [
                {"channel": "email"},
                {"channel": "google"},
                {"channel": "meta"},
            ],
            "conversion_value": 300.0,
            "converted": True,
        },
    ]
    credit = position_based(journeys, first_pct=0.4, last_pct=0.4)
    # Sanity: total credit equals total value
    assert math.isclose(sum(credit.values()), 600.0, rel_tol=1e-6)
    # Single-touch path should attribute full value to that channel
    assert credit["google"] >= 100.0


def test_markov_falls_back_or_produces_non_negative_credit():
    journeys = _simple_journeys()
    credit = markov(journeys)
    # Either Markov works and we get non-negative credit, or it falls back to linear()
    assert credit
    assert all(v >= 0 for v in credit.values())
    assert math.isclose(sum(credit.values()), 150.0, rel_tol=1e-6)


def test_run_attribution_wraps_core_models_and_shapes_output():
    journeys = _simple_journeys()
    result = run_attribution(journeys, model="linear")
    assert result["model"] == "linear"
    assert result["total_conversions"] == 2
    assert result["total_value"] == 150.0
    # channel_credit should match rounded linear output
    channel_credit = result["channel_credit"]
    assert math.isclose(sum(channel_credit.values()), 150.0, rel_tol=1e-6)
    assert isinstance(result["channels"], list)
