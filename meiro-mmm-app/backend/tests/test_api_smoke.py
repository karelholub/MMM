from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_health_endpoint_ok():
    resp = client.get("/api/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("status") == "ok"
    assert "attribution_models" in body


def test_attribution_models_listed():
    resp = client.get("/api/attribution/models")
    assert resp.status_code == 200
    data = resp.json()
    assert "models" in data
    assert isinstance(data["models"], list)
    assert "linear" in data["models"]


def test_data_quality_run_smoke():
    # This should run end-to-end using whatever local SQLite / data files are available.
    resp = client.post("/api/data-quality/run")
    # In minimal setups, this may still succeed with zero snapshots if no data files exist.
    assert resp.status_code == 200
    payload = resp.json()
    assert "snapshots_created" in payload
    assert "alerts_created" in payload


def test_data_quality_snapshots_list_empty_or_more():
    resp = client.get("/api/data-quality/snapshots")
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body, list)
