"""Exportador: replica tablas de DuckDB a PostgreSQL via DuckDB postgres ATTACH."""

import duckdb
from sqlalchemy import create_engine, text


class PostgresExporter:
    """Exporta raw/staging/consume de DuckDB a PostgreSQL via ATTACH nativo."""

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

    def _attach(self):
        cfg = self.pg_config
        conn_str = (
            f"host={cfg['host']} port={cfg['port']} "
            f"dbname={cfg['database']} user={cfg['user']} password={cfg['password']}"
        )
        self.duckdb_conn.execute("LOAD postgres")
        self.duckdb_conn.execute(f"ATTACH '{conn_str}' AS pg (TYPE POSTGRES)")

    def export_all(self) -> dict[str, int]:
        """Exporta raw, staging y consume a PostgreSQL via DuckDB ATTACH."""
        schemas = ["raw", "staging", "consume"]

        # DDL: drop y recrear schemas limpios
        engine = self._get_engine()
        with engine.begin() as conn:
            for schema in schemas:
                conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
                conn.execute(text(f"CREATE SCHEMA {schema}"))

        self._attach()

        results = {}
        for schema in schemas:
            tables = self.duckdb_conn.execute(
                f"SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{schema}' "
                f"UNION ALL "
                f"SELECT table_name FROM information_schema.views "
                f"WHERE table_schema = '{schema}'"
            ).fetchall()

            count = 0
            for (tbl_name,) in tables:
                self.duckdb_conn.execute(
                    f"CREATE TABLE pg.{schema}.{tbl_name} AS "
                    f"SELECT * FROM {schema}.{tbl_name}"
                )
                count += 1
            results[schema] = count

        return results

    def close(self):
        try:
            self.duckdb_conn.execute("DETACH pg")
        except Exception:
            pass
        if self._engine:
            self._engine.dispose()
            self._engine = None
