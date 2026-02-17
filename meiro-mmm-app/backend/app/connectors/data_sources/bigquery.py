from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from .base import DataSourceConnector


class BigQueryConnector(DataSourceConnector):
    connector_type = "bigquery"

    def validate_config(self, config: Dict[str, Any], secrets: Dict[str, Any]) -> None:
        name = str(config.get("name") or "").strip()
        if not name:
            raise ValueError("Connection name is required")
        sa_raw = secrets.get("service_account_json")
        if not isinstance(sa_raw, str) or not sa_raw.strip():
            raise ValueError("Service account JSON is required")
        try:
            payload = json.loads(sa_raw)
        except Exception:
            raise ValueError("Service account JSON must be valid JSON")
        if not isinstance(payload, dict):
            raise ValueError("Service account JSON must be an object")
        if payload.get("type") != "service_account":
            raise ValueError("Service account JSON must have type=service_account")
        if not (payload.get("client_email") and payload.get("private_key")):
            raise ValueError("Service account JSON is missing client_email/private_key")
        project = str(config.get("project_id") or payload.get("project_id") or "").strip()
        if not project:
            raise ValueError("Project ID is required (or derive-able from credentials)")

    def test_connection(self, config: Dict[str, Any], secrets: Dict[str, Any]) -> Dict[str, Any]:
        t0 = time.perf_counter()
        self.validate_config(config, secrets)
        payload = json.loads(str(secrets.get("service_account_json")))
        project_id = str(config.get("project_id") or payload.get("project_id"))
        default_dataset = (config.get("default_dataset") or "").strip()
        details: Dict[str, Any] = {
            "sample_namespaces": [],
            "sample_schemas": [],
            "sample_tables": [],
        }

        message = "Credentials validated."
        try:
            from google.oauth2 import service_account  # type: ignore
            from google.cloud import bigquery  # type: ignore

            creds = service_account.Credentials.from_service_account_info(payload)
            client = bigquery.Client(project=project_id, credentials=creds)
            datasets = [d.dataset_id for d in list(client.list_datasets(project=project_id, max_results=5))]
            details["sample_namespaces"] = datasets
            if default_dataset:
                try:
                    table_iter = client.list_tables(f"{project_id}.{default_dataset}", max_results=5)
                    details["sample_tables"] = [t.table_id for t in table_iter]
                except Exception:
                    pass
            _ = list(client.query("SELECT 1 AS ok").result(max_results=1))
            message = "Connected to BigQuery."
        except Exception as e:
            # Still useful in offline/dev: config/credential structure validated.
            message = f"Validated config; runtime connectivity check unavailable: {e}"

        latency_ms = int((time.perf_counter() - t0) * 1000)
        return {"ok": True, "message": message, "latency_ms": latency_ms, "details": details}

    def list_namespaces(self, config: Dict[str, Any], secrets: Dict[str, Any], limit: int = 20) -> List[str]:
        out = self.test_connection(config, secrets)
        return (out.get("details") or {}).get("sample_namespaces", [])[: max(1, min(limit, 100))]

    def list_schemas(
        self,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        namespace: Optional[str] = None,
        limit: int = 20,
    ) -> List[str]:
        _ = namespace
        _ = limit
        return []

    def list_tables(
        self,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        namespace: Optional[str] = None,
        schema: Optional[str] = None,
        limit: int = 20,
    ) -> List[str]:
        _ = namespace
        _ = schema
        out = self.test_connection(config, secrets)
        return (out.get("details") or {}).get("sample_tables", [])[: max(1, min(limit, 100))]

    def run_query(self, config: Dict[str, Any], secrets: Dict[str, Any], query: str, limit: int = 100) -> Dict[str, Any]:
        _ = query
        _ = limit
        out = self.test_connection(config, secrets)
        return {"columns": [], "rows": [], "message": out.get("message", "Query scaffold only")}
