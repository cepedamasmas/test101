"""Exportador: replica tablas de DuckDB a PostgreSQL para acceso remoto."""

import duckdb
from sqlalchemy import create_engine, text


class PostgresExporter:
    """Exporta schemas raw/staging/consume de DuckDB a PostgreSQL."""

    def __init__(self, pg_config: dict, duckdb_conn: duckdb.DuckDBPyConnection):
        self.pg_config = pg_config
        self.duckdb_conn = duckdb_conn
        self._engine = None

    def _get_engine(self):
        if not self._engine:
            cfg = self.pg_config
            url = f"postgresql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
            self._engine = create_engine(url)
        return self._engine

    def export_all(self) -> dict[str, int]:
        """Exporta todas las tablas de los 3 schemas a PostgreSQL."""
        engine = self._get_engine()

        with engine.begin() as conn:
            conn.execute(text("DROP SCHEMA IF EXISTS raw CASCADE"))
            conn.execute(text("DROP SCHEMA IF EXISTS staging CASCADE"))
            conn.execute(text("DROP SCHEMA IF EXISTS consume CASCADE"))
            conn.execute(text("CREATE SCHEMA raw"))
            conn.execute(text("CREATE SCHEMA staging"))
            conn.execute(text("CREATE SCHEMA consume"))

        results = {}
        for schema in ["raw", "staging", "consume"]:
            tables = self.duckdb_conn.execute(
                f"SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{schema}' "
                f"UNION ALL "
                f"SELECT table_name FROM information_schema.views "
                f"WHERE table_schema = '{schema}'"
            ).fetchall()

            count = 0
            for (tbl_name,) in tables:
                df = self.duckdb_conn.execute(f"SELECT * FROM {schema}.{tbl_name}").fetchdf()
                with engine.begin() as conn:
                    df.to_sql(tbl_name, conn, schema=schema, if_exists="replace", index=False)
                count += 1
            results[schema] = count

        return results

    def close(self):
        if self._engine:
            self._engine.dispose()
            self._engine = None
