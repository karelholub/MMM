from __future__ import annotations

from typing import Dict

from .base import DataSourceConnector
from .bigquery import BigQueryConnector
from .snowflake import SnowflakeConnector


_CONNECTORS: Dict[str, DataSourceConnector] = {
    "bigquery": BigQueryConnector(),
    "snowflake": SnowflakeConnector(),
}


def get_connector(connector_type: str) -> DataSourceConnector:
    key = (connector_type or "").strip().lower()
    if key not in _CONNECTORS:
        raise ValueError(f"Unsupported connector type: {connector_type}")
    return _CONNECTORS[key]


def list_connector_types() -> list[str]:
    return sorted(_CONNECTORS.keys())
