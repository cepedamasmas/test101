"""Exportador: crea schemas y vistas/tablas en DuckDB para consulta con DBeaver."""

from pathlib import Path

import duckdb


class DuckDBExporter:
    """Crea vistas RAW y registra tablas staging/consume en DuckDB."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, data_dir: Path):
        self.conn = conn
        self.data_dir = data_dir

    def export_raw_views(self, raw_tables: dict[str, tuple[str, str]]) -> int:
        """Crea vistas en schema 'raw' apuntando a parquets con hive partitioning."""
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        count = 0
        for tbl_name, (source, folder) in raw_tables.items():
            glob = self.data_dir / "raw" / source / folder / "*" / "*" / "*" / "data.parquet"
            self.conn.execute(
                f"CREATE OR REPLACE VIEW raw.{tbl_name} AS "
                f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true)"
            )
            count += 1
        return count

    def verify_dbt_tables(self) -> dict[str, int]:
        """Verifica que dbt haya creado las tablas staging/consume y retorna conteos."""
        counts = {}
        for schema in ["staging", "consume"]:
            tables = self.conn.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
            ).fetchall()
            counts[schema] = len(tables)
        return counts
