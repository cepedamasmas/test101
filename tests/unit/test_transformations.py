"""Tests para transformaciones."""

import duckdb
import pytest

from ducklake.transformations.cleaning import (
    build_cast_sql,
    build_dedup_sql,
    build_filter_sql,
    build_rename_sql,
)
from ducklake.transformations.validation import (
    get_column_stats,
    validate_not_null,
    validate_unique,
)


@pytest.fixture
def conn_with_data():
    """Conexión DuckDB con tabla de prueba."""
    conn = duckdb.connect()
    conn.execute("""
        CREATE TABLE test_data AS SELECT * FROM (VALUES
            (1, 'Alice', 100.0, 'ACTIVO'),
            (2, 'Bob', 200.0, 'ACTIVO'),
            (2, 'Bob', 200.0, 'ACTIVO'),
            (3, NULL, 150.0, 'INACTIVO'),
            (4, 'Diana', 300.0, 'DELETED')
        ) AS t(id, nombre, total, estado)
    """)
    yield conn
    conn.close()


class TestCleaningSQL:
    def test_rename(self, conn_with_data):
        sql = build_rename_sql({"id": "user_id", "nombre": "name"}, "test_data")
        result = conn_with_data.execute(sql).fetchdf()
        assert "user_id" in result.columns
        assert "name" in result.columns
        assert "id" not in result.columns

    def test_filter(self, conn_with_data):
        sql = build_filter_sql("total > 150", "test_data")
        result = conn_with_data.execute(sql).fetchdf()
        assert len(result) == 2  # Bob(200) and Diana(300)

    def test_dedup(self, conn_with_data):
        sql = build_dedup_sql(["id"], "id ASC", "test_data")
        result = conn_with_data.execute(sql).fetchdf()
        assert len(result) == 4  # id=2 deduplicado


class TestValidation:
    def test_not_null(self, conn_with_data):
        results = validate_not_null(conn_with_data, "test_data", ["nombre", "id"])
        assert results["nombre"] == 1  # 1 null
        assert results["id"] == 0

    def test_unique(self, conn_with_data):
        dups = validate_unique(conn_with_data, "test_data", ["id"])
        assert dups == 1  # id=2 duplicado

    def test_column_stats(self, conn_with_data):
        stats = get_column_stats(conn_with_data, "test_data", "total")
        assert stats["min"] == 100.0
        assert stats["max"] == 300.0
        assert stats["total"] == 5
        assert stats["nulls"] == 0
