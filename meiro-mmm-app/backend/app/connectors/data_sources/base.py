"""Connector interface for warehouse/source integrations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class DataSourceConnector(ABC):
    connector_type: str = ""

    @abstractmethod
    def validate_config(self, config: Dict[str, Any], secrets: Dict[str, Any]) -> None:
        """Validate config + secret payload or raise ValueError."""

    @abstractmethod
    def test_connection(self, config: Dict[str, Any], secrets: Dict[str, Any]) -> Dict[str, Any]:
        """Return {ok, message, latency_ms, details}."""

    @abstractmethod
    def list_namespaces(self, config: Dict[str, Any], secrets: Dict[str, Any], limit: int = 20) -> List[str]:
        """List namespaces (datasets / databases)."""

    @abstractmethod
    def list_schemas(
        self,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        namespace: Optional[str] = None,
        limit: int = 20,
    ) -> List[str]:
        """List schemas."""

    @abstractmethod
    def list_tables(
        self,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        namespace: Optional[str] = None,
        schema: Optional[str] = None,
        limit: int = 20,
    ) -> List[str]:
        """List tables."""

    @abstractmethod
    def run_query(self, config: Dict[str, Any], secrets: Dict[str, Any], query: str, limit: int = 100) -> Dict[str, Any]:
        """Execute query and return preview rows/columns metadata."""

    # Forward compatibility hooks (MVP stubs only).
    def ensure_schema(self, version_id: str) -> None:
        raise NotImplementedError("TODO: ensureSchema(versionId)")

    def write_events(self, batch: List[Dict[str, Any]]) -> None:
        raise NotImplementedError("TODO: writeEvents(batch)")

    def write_identity_map(self, batch: List[Dict[str, Any]]) -> None:
        raise NotImplementedError("TODO: writeIdentityMap(batch)")

    def materialize_journeys(self, version_id: str) -> None:
        raise NotImplementedError("TODO: materializeJourneys(versionId)")
