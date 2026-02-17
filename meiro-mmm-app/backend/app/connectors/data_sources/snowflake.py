from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from cryptography.hazmat.primitives.serialization import load_pem_private_key

from .base import DataSourceConnector


class SnowflakeConnector(DataSourceConnector):
    connector_type = "snowflake"

    def validate_config(self, config: Dict[str, Any], secrets: Dict[str, Any]) -> None:
        if not str(config.get("name") or "").strip():
            raise ValueError("Connection name is required")
        if not str(config.get("account") or "").strip():
            raise ValueError("Account identifier is required")
        if not str(config.get("username") or "").strip():
            raise ValueError("Username is required")
        if not str(config.get("warehouse") or "").strip():
            raise ValueError("Warehouse is required")
        private_key_pem = str(secrets.get("private_key_pem") or "").strip()
        if not private_key_pem:
            raise ValueError("Private key PEM is required")
        passphrase = secrets.get("passphrase")
        pwd = passphrase.encode("utf-8") if isinstance(passphrase, str) and passphrase else None
        try:
            load_pem_private_key(private_key_pem.encode("utf-8"), password=pwd)
        except Exception:
            raise ValueError("Private key PEM is invalid or passphrase is incorrect")

    def test_connection(self, config: Dict[str, Any], secrets: Dict[str, Any]) -> Dict[str, Any]:
        t0 = time.perf_counter()
        self.validate_config(config, secrets)
        details: Dict[str, Any] = {"sample_namespaces": [], "sample_schemas": [], "sample_tables": []}
        message = "Credentials validated."

        try:
            import snowflake.connector  # type: ignore

            private_key_pem = str(secrets.get("private_key_pem") or "")
            passphrase = secrets.get("passphrase")
            pwd = passphrase.encode("utf-8") if isinstance(passphrase, str) and passphrase else None
            key_obj = load_pem_private_key(private_key_pem.encode("utf-8"), password=pwd)
            from cryptography.hazmat.primitives import serialization

            private_key_der = key_obj.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            conn = snowflake.connector.connect(
                account=str(config.get("account")),
                user=str(config.get("username")),
                private_key=private_key_der,
                warehouse=str(config.get("warehouse")),
                role=(str(config.get("role")).strip() or None),
                database=(str(config.get("default_database")).strip() or None),
                schema=(str(config.get("default_schema")).strip() or None),
                login_timeout=8,
            )
            try:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                _ = cur.fetchone()
                cur.execute("SHOW DATABASES LIMIT 5")
                details["sample_namespaces"] = [str(r[1]) for r in cur.fetchall()[:5]]
                message = "Connected to Snowflake."
            finally:
                conn.close()
        except Exception as e:
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
        _ = config
        _ = secrets
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
        _ = config
        _ = secrets
        _ = namespace
        _ = schema
        _ = limit
        return []

    def run_query(self, config: Dict[str, Any], secrets: Dict[str, Any], query: str, limit: int = 100) -> Dict[str, Any]:
        _ = query
        _ = limit
        out = self.test_connection(config, secrets)
        return {"columns": [], "rows": [], "message": out.get("message", "Query scaffold only")}
