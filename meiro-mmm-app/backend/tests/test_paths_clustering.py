from app.services_paths import compute_path_anomalies, compute_path_archetypes


def _mk_journey(path: str, ts: str = "2024-01-01T00:00:00", converted: bool = True):
    steps = [s.strip() for s in path.split(">")]
    return {
        "customer_id": f"c-{path}-{ts}",
        "converted": converted,
        "touchpoints": [{"channel": s, "timestamp": ts} for s in steps],
    }


def test_compute_path_archetypes_fixed_k_shapes_and_totals():
    journeys = []
    journeys += [_mk_journey("google > email > direct") for _ in range(30)]
    journeys += [_mk_journey("google > direct") for _ in range(10)]
    journeys += [_mk_journey("meta > email > direct") for _ in range(20)]
    journeys += [_mk_journey("direct") for _ in range(5)]

    out = compute_path_archetypes(journeys, k_mode="fixed", k=2)
    assert "clusters" in out
    assert "total_converted" in out
    assert out["total_converted"] == 65

    clusters = out["clusters"]
    assert len(clusters) == 2
    for c in clusters:
        assert "id" in c
        assert "name" in c
        assert "size" in c
        assert "share" in c
        assert "avg_length" in c
        assert "top_paths" in c and isinstance(c["top_paths"], list)
        assert "representative_path" in c


def test_compute_path_anomalies_flags_unknown_share_and_missing_timestamps():
    journeys = []
    # 10 journeys with unknown channels
    for _ in range(10):
        journeys.append(
            {
                "customer_id": "u",
                "converted": True,
                "touchpoints": [{"channel": "unknown", "timestamp": ""}, {"channel": "unknown", "timestamp": ""}],
            }
        )
    # 10 journeys with valid channels/timestamps
    journeys += [_mk_journey("google > email", ts="2024-01-02T00:00:00") for _ in range(10)]

    anomalies = compute_path_anomalies(journeys)
    types = {a["type"] for a in anomalies}
    assert "high_unknown_share" in types
    assert "missing_timestamps" in types
