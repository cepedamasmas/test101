"""Exportador: replica tablas de DuckDB a PostgreSQL via bulk COPY (CSV)."""

import io
import duckdb
import psycopg2
from sqlalchemy import create_engine, text


class PostgresExporter:
    """Exporta raw/staging/consume de DuckDB a PostgreSQL via psycopg2 COPY."""

    def __init__(self, pg_config: dict, duckdb_conn: duckdb.DuckDBPyConnection):
        self.pg_config = pg_config
        self.duckdb_conn = duckdb_conn
        self._engine = None
        self._pg_conn = None

    def _get_engine(self):
        if not self._engine:
            cfg = self.pg_config
            url = f"postgresql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
            self._engine = create_engine(url)
        return self._engine

    def _get_pg_conn(self):
        if not self._pg_conn or self._pg_conn.closed:
            cfg = self.pg_config
            self._pg_conn = psycopg2.connect(
                host=cfg["host"],
                port=cfg["port"],
                dbname=cfg["database"],
                user=cfg["user"],
                password=cfg["password"],
            )
        return self._pg_conn

    def _create_table_ddl(self, schema: str, table: str) -> str:
        """Genera DDL CREATE TABLE en PostgreSQL inferido desde DuckDB."""
        cols = self.duckdb_conn.execute(
            f"SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
            f"ORDER BY ordinal_position"
        ).fetchall()

        type_map = {
            "BIGINT": "BIGINT",
            "INTEGER": "INTEGER",
            "HUGEINT": "NUMERIC",
            "DOUBLE": "DOUBLE PRECISION",
            "FLOAT": "REAL",
            "DECIMAL": "NUMERIC",
            "NUMERIC": "NUMERIC",
            "BOOLEAN": "BOOLEAN",
            "VARCHAR": "TEXT",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
            "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
            "INTERVAL": "INTERVAL",
            "BLOB": "BYTEA",
        }

        col_defs = []
        for col_name, duck_type in cols:
            # Normalize: strip precision/scale for lookup
            base_type = duck_type.split("(")[0].upper()
            pg_type = type_map.get(base_type, "TEXT")
            col_defs.append(f'  "{col_name}" {pg_type}')

        return f'CREATE TABLE "{schema}"."{table}" (\n' + ",\n".join(col_defs) + "\n)"

    def _copy_table(self, schema: str, table: str, pg_conn) -> None:
        """Exporta una tabla via psycopg2 copy_expert (bulk COPY desde CSV en memoria)."""
        df = self.duckdb_conn.execute(
            f'SELECT * FROM "{schema}"."{table}"'
        ).df()

        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)

        with pg_conn.cursor() as cur:
            cur.execute(self._create_table_ddl(schema, table))
            cur.copy_expert(
                f'COPY "{schema}"."{table}" FROM STDIN CSV HEADER',
                buf,
            )

    def export_all(self) -> dict[str, int]:
        """Exporta raw, staging y consume a PostgreSQL via bulk COPY."""
        schemas = ["raw", "staging", "consume"]

        # DDL: drop y recrear schemas limpios en PostgreSQL
        engine = self._get_engine()
        with engine.begin() as conn:
            for schema in schemas:
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
                conn.execute(text(f'CREATE SCHEMA "{schema}"'))

        pg_conn = self._get_pg_conn()
        pg_conn.autocommit = False

        results = {}
        for schema in schemas:
            tables = self.duckdb_conn.execute(
                f"SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{schema}'"
            ).fetchall()

            count = 0
            for (tbl_name,) in tables:
                self._copy_table(schema, tbl_name, pg_conn)
                count += 1

            pg_conn.commit()
            results[schema] = count

        return results

    def close(self):
        if self._pg_conn and not self._pg_conn.closed:
            self._pg_conn.close()
            self._pg_conn = None
        if self._engine:
            self._engine.dispose()
            self._engine = None
