"""Data source connectors."""

from typing import Any, Dict

from ducklake.connectors.base import BaseConnector
from ducklake.connectors.csv_connector import CSVConnector
from ducklake.connectors.mysql import MySQLConnector

_CONNECTOR_MAP: Dict[str, type] = {
    "mysql": MySQLConnector,
    "csv": CSVConnector,
}


def get_connector(config: Dict[str, Any]) -> BaseConnector:
    """Factory: create a connector instance from source config."""
    connector_type = config["type"]
    if connector_type not in _CONNECTOR_MAP:
        raise ValueError(
            f"Connector type '{connector_type}' not supported. "
            f"Available: {list(_CONNECTOR_MAP.keys())}"
        )
    return _CONNECTOR_MAP[connector_type](config)
