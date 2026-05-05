from app.services_overview import get_overview_trend_insights


def test_overview_trend_insights_returns_decomposition_momentum_and_mix_shift(monkeypatch):
    current = {
        "paid_social": {"visits": 10.0, "conversions": 2.0, "revenue": 100.0},
        "email": {"visits": 8.0, "conversions": 3.0, "revenue": 250.0},
    }
    previous = {
        "paid_social": {"visits": 7.0, "conversions": 1.0, "revenue": 90.0},
        "organic_search": {"visits": 4.0, "conversions": 1.0, "revenue": 40.0},
    }
    daily = {
        "email": {
            "2024-02-08": 0.0,
            "2024-02-09": 0.0,
            "2024-02-10": 0.0,
            "2024-02-11": 80.0,
            "2024-02-12": 90.0,
            "2024-02-13": 80.0,
            "2024-02-14": 0.0,
        },
        "paid_social": {
            "2024-02-08": 20.0,
            "2024-02-09": 20.0,
            "2024-02-10": 10.0,
            "2024-02-11": 0.0,
            "2024-02-12": 0.0,
            "2024-02-13": 0.0,
            "2024-02-14": 0.0,
        },
    }

    calls = {"count": 0}

    def _fake_aggregate_channel_metrics(db, *, dt_from, dt_to, conversion_key=None, **kwargs):
        calls["count"] += 1
        return current if calls["count"] == 1 else previous

    monkeypatch.setattr("app.services_overview._aggregate_channel_metrics", _fake_aggregate_channel_metrics)
    monkeypatch.setattr("app.services_overview._aggregate_daily_channel_revenue", lambda *args, **kwargs: daily)

    out = get_overview_trend_insights(
        db=None,
        date_from="2024-02-08",
        date_to="2024-02-14",
    )

    assert out["decomposition"]["current"]["revenue"] == 350.0
    assert out["decomposition"]["previous"]["revenue"] == 130.0
    assert len(out["decomposition"]["factors"]) == 4
    assert out["momentum"]["window_days"] >= 3
    assert out["momentum"]["rising"][0]["channel"] == "email"
    assert any(row["channel"] == "paid_social" for row in out["mix_shift"])
