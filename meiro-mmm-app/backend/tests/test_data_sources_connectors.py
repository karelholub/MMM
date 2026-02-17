from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.connectors.data_sources.registry import get_connector, list_connector_types
from app.db import Base, get_db
from app.main import app


def _bq_secret() -> str:
    return '{"type":"service_account","project_id":"demo-proj","client_email":"x@example.com","private_key":"-----BEGIN PRIVATE KEY-----\\\\nMIIBVwIBADANBgkq\\\\n-----END PRIVATE KEY-----\\\\n"}'


@pytest.fixture
def client():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)

    def override_get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides.clear()
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()
    engine.dispose()


def test_registry_exposes_expected_connectors():
    types = list_connector_types()
    assert "bigquery" in types
    assert "snowflake" in types
    assert get_connector("bigquery").connector_type == "bigquery"


def test_bigquery_validate_config_errors():
    conn = get_connector("bigquery")
    with pytest.raises(ValueError):
        conn.validate_config({"name": "BQ", "project_id": "p"}, {"service_account_json": "not-json"})


def test_test_connection_error_handling(client: TestClient):
    res = client.post(
        "/api/data-sources/test",
        json={
            "type": "bigquery",
            "config_json": {"name": "BQ Test", "project_id": "proj"},
            "secrets": {"service_account_json": "not-json"},
        },
    )
    assert res.status_code == 400
    assert "Service account JSON" in res.json()["detail"]


def test_api_never_returns_secrets(client: TestClient):
    created = client.post(
        "/api/data-sources",
        headers={"X-User-Role": "editor", "X-User-Id": "qa"},
        json={
            "category": "warehouse",
            "type": "bigquery",
            "name": "BQ Prod",
            "config_json": {"project_id": "demo-proj", "default_dataset": "analytics"},
            "secrets": {"service_account_json": _bq_secret()},
        },
    )
    assert created.status_code == 200, created.text
    body = created.json()
    assert body["type"] == "bigquery"
    assert "service_account_json" not in str(body)
    assert body.get("has_secret") is True

    listed = client.get("/api/data-sources?category=warehouse")
    assert listed.status_code == 200
    txt = str(listed.json())
    assert "service_account_json" not in txt
    assert "private_key" not in txt
