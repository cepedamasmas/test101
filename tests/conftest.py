"""Fixtures compartidas para tests."""

import os
import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def tmp_data_dir(tmp_path):
    """Directorio temporal con estructura de data lake."""
    for layer in ["raw", "staging", "consume"]:
        (tmp_path / layer).mkdir()
    return str(tmp_path)


@pytest.fixture
def duckdb_conn():
    """Conexión DuckDB in-memory."""
    conn = duckdb.connect()
    yield conn
    conn.close()


@pytest.fixture
def sample_parquet(tmp_path):
    """Crear un archivo parquet de ejemplo."""
    table = pa.table({
        "id": [1, 2, 3, 4, 5],
        "nombre": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "email": ["alice@test.com", "bob@test.com", None, "diana@test.com", "eve@test.com"],
        "total": [100.0, 200.0, 150.0, 300.0, 250.0],
        "estado": ["ACTIVO", "ACTIVO", "INACTIVO", "ACTIVO", "DELETED"],
        "_ingestion_timestamp": pa.array(
            ["2024-01-01 10:00:00"] * 5, type=pa.string()
        ),
        "_source_name": ["test_source"] * 5,
    })
    path = str(tmp_path / "sample.parquet")
    pq.write_table(table, path)
    return path


@pytest.fixture
def sample_csv(tmp_path):
    """Crear un archivo CSV de ejemplo."""
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text(
        "id,nombre,email,total\n"
        "1,Alice,alice@test.com,100.0\n"
        "2,Bob,bob@test.com,200.0\n"
        "3,Charlie,,150.0\n"
        "4,Diana,diana@test.com,300.0\n"
        "5,Eve,eve@test.com,250.0\n",
        encoding="utf-8",
    )
    return str(csv_path)


@pytest.fixture
def config_dir(tmp_path):
    """Directorio de configuración temporal."""
    config_path = tmp_path / "config"
    config_path.mkdir()

    (config_path / "sources.yaml").write_text(
        """
sources:
  - name: test_csv
    type: csv
    enabled: true
    path: "*.csv"
    connection:
      delimiter: ","
      header: true
    tables:
      - test_table
    extract:
      mode: full
""",
        encoding="utf-8",
    )

    (config_path / "pipelines.yaml").write_text(
        """
pipelines:
  - name: test_pipeline
    description: "Test pipeline"
    source:
      layer: raw
      domain: test_csv
      table: test_table
    destination:
      layer: staging
      domain: test
      table: test_table
    transforms:
      - type: filter
        condition: "total > 0"
    quality_checks:
      - type: not_null
        columns: [id]
""",
        encoding="utf-8",
    )

    (config_path / "settings.yaml").write_text(
        """
settings:
  data_path: ./data
  config_path: ./config
  log_level: DEBUG
  duckdb_memory_limit: 1GB
  duckdb_threads: 2
""",
        encoding="utf-8",
    )

    return str(config_path)
