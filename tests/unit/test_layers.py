"""Tests para las capas RAW, STAGING, CONSUME."""

from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from ducklake.layers.raw import RawLayer
from ducklake.layers.staging import StagingLayer
from ducklake.layers.consume import ConsumeLayer


class TestRawLayer:
    def test_write_creates_partitioned_file(self, tmp_data_dir, sample_parquet):
        raw = RawLayer(tmp_data_dir)
        result_path = raw.write(sample_parquet, {"source": "test_src", "table": "users"})

        assert Path(result_path).exists()
        assert "raw/test_src/users/year=" in result_path
        assert result_path.endswith("data.parquet")

        # Verificar que el parquet es legible
        table = pq.read_table(result_path)
        assert table.num_rows == 5

    def test_read_builds_query(self, tmp_data_dir):
        raw = RawLayer(tmp_data_dir)
        query = raw.read({"domain": "test_src", "table": "users"})
        assert "read_parquet" in query
        assert "raw/test_src/users" in query

    def test_list_sources_empty(self, tmp_data_dir):
        raw = RawLayer(tmp_data_dir)
        assert raw.list_sources() == []

    def test_list_sources_after_write(self, tmp_data_dir, sample_parquet):
        raw = RawLayer(tmp_data_dir)
        raw.write(sample_parquet, {"source": "my_source", "table": "t1"})
        sources = raw.list_sources()
        assert "my_source" in sources


class TestStagingLayer:
    def test_write_creates_file(self, tmp_data_dir, duckdb_conn, sample_parquet):
        staging = StagingLayer(tmp_data_dir, duckdb_conn)
        relation = duckdb_conn.sql(f"SELECT * FROM read_parquet('{sample_parquet}')")
        result = staging.write(relation, {"domain": "ventas", "table": "clientes"})

        assert Path(result).exists()
        assert "staging/ventas/clientes/data.parquet" in result

    def test_read_builds_query(self, tmp_data_dir, duckdb_conn):
        staging = StagingLayer(tmp_data_dir, duckdb_conn)
        query = staging.read({"domain": "ventas", "table": "clientes"})
        assert "staging/ventas/clientes/data.parquet" in query

    def test_process_with_filter(self, tmp_data_dir, duckdb_conn, sample_parquet):
        staging = StagingLayer(tmp_data_dir, duckdb_conn)

        raw_query = f"SELECT * FROM read_parquet('{sample_parquet}')"
        pipeline_config = {
            "name": "test_pipeline",
            "source": {"layer": "raw", "domain": "test", "table": "t"},
            "destination": {"layer": "staging", "domain": "test", "table": "filtered"},
            "transforms": [
                {"type": "filter", "condition": "total > 150"}
            ],
            "quality_checks": [],
        }

        result = staging.process(pipeline_config, raw_query)
        assert result["status"] == "success"
        assert result["rows"] == 3  # Bob(200), Diana(300), Eve(250)
        assert Path(result["path"]).exists()


class TestConsumeLayer:
    def test_write_creates_file(self, tmp_data_dir, duckdb_conn, sample_parquet):
        consume = ConsumeLayer(tmp_data_dir, duckdb_conn)
        relation = duckdb_conn.sql(f"SELECT * FROM read_parquet('{sample_parquet}')")
        result = consume.write(relation, {"domain": "bi", "table": "summary"})

        assert Path(result).exists()
        assert "consume/bi/summary/data.parquet" in result

    def test_process_with_custom_sql(self, tmp_data_dir, duckdb_conn, sample_parquet):
        consume = ConsumeLayer(tmp_data_dir, duckdb_conn)

        staging_query = f"SELECT * FROM read_parquet('{sample_parquet}')"
        pipeline_config = {
            "name": "test_consume",
            "destination": {"domain": "bi", "table": "totals"},
            "transforms": [
                {
                    "type": "custom_sql",
                    "sql": "SELECT estado, COUNT(*) AS cnt, SUM(total) AS total FROM __INPUT__ GROUP BY estado",
                }
            ],
        }

        result = consume.process(pipeline_config, staging_query)
        assert result["status"] == "success"
        assert result["rows"] == 3  # ACTIVO, INACTIVO, DELETED
