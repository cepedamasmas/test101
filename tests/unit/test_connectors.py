"""Tests para conectores."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest

from ducklake.connectors.csv_connector import CSVConnector
from ducklake.connectors.mysql import MySQLConnector


class TestCSVConnector:
    def test_validate_connection_with_files(self, sample_csv):
        config = {
            "name": "test_csv",
            "type": "csv",
            "path": sample_csv,
            "connection": {"delimiter": ",", "header": True},
            "tables": [],
            "extract": {"mode": "full"},
        }
        connector = CSVConnector(config)
        assert connector.validate_connection() is True

    def test_validate_connection_no_files(self):
        config = {
            "name": "test_csv",
            "type": "csv",
            "path": "/nonexistent/path/*.csv",
            "connection": {},
            "tables": [],
            "extract": {"mode": "full"},
        }
        connector = CSVConnector(config)
        assert connector.validate_connection() is False

    def test_extract_to_parquet(self, sample_csv, tmp_path):
        config = {
            "name": "test_csv",
            "type": "csv",
            "path": sample_csv,
            "connection": {"delimiter": ",", "header": True},
            "tables": [],
            "extract": {"mode": "full"},
        }
        connector = CSVConnector(config)
        output = str(tmp_path / "output.parquet")
        result = connector.extract("test_table", output)

        assert Path(result).exists()
        table = pq.read_table(result)
        assert table.num_rows == 5
        assert "_ingestion_timestamp" in table.column_names
        assert "_source_name" in table.column_names

    def test_get_schema(self, sample_csv):
        config = {
            "name": "test_csv",
            "type": "csv",
            "path": sample_csv,
            "connection": {"delimiter": ",", "header": True},
            "tables": [],
            "extract": {"mode": "full"},
        }
        connector = CSVConnector(config)
        schema = connector.get_schema("test")
        assert "id" in schema
        assert "nombre" in schema


class TestMySQLConnector:
    def test_init_with_dict_config(self):
        config = {
            "name": "test_mysql",
            "type": "mysql",
            "connection": {
                "host": "localhost",
                "port": 3306,
                "database": "testdb",
                "user": "root",
                "password": "secret",
            },
            "tables": ["users"],
            "extract": {"mode": "full"},
        }
        connector = MySQLConnector(config)
        assert connector.name == "test_mysql"
        assert connector.connection_params["host"] == "localhost"
        assert connector.connection_params["database"] == "testdb"

    def test_get_tables(self):
        config = {
            "name": "test_mysql",
            "type": "mysql",
            "connection": {"host": "localhost"},
            "tables": ["clientes", "pedidos"],
            "extract": {"mode": "full"},
        }
        connector = MySQLConnector(config)
        assert connector.get_tables() == ["clientes", "pedidos"]

    def test_get_extract_mode(self):
        config = {
            "name": "test_mysql",
            "type": "mysql",
            "connection": {"host": "localhost"},
            "tables": [],
            "extract": {"mode": "incremental", "key_column": "updated_at"},
        }
        connector = MySQLConnector(config)
        assert connector.get_extract_mode() == "incremental"

    @patch("ducklake.connectors.mysql.pymysql")
    def test_validate_connection_success(self, mock_pymysql):
        mock_conn = MagicMock()
        mock_pymysql.connect.return_value = mock_conn

        config = {
            "name": "test_mysql",
            "type": "mysql",
            "connection": {"host": "localhost", "port": 3306, "database": "db", "user": "u", "password": "p"},
            "tables": [],
            "extract": {"mode": "full"},
        }
        connector = MySQLConnector(config)
        assert connector.validate_connection() is True
        mock_conn.close.assert_called_once()

    @patch("ducklake.connectors.mysql.pymysql")
    def test_validate_connection_failure(self, mock_pymysql):
        mock_pymysql.connect.side_effect = Exception("Connection refused")

        config = {
            "name": "test_mysql",
            "type": "mysql",
            "connection": {"host": "badhost", "port": 3306, "database": "db", "user": "u", "password": "p"},
            "tables": [],
            "extract": {"mode": "full"},
        }
        connector = MySQLConnector(config)
        assert connector.validate_connection() is False
