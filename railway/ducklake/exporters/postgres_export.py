"""Exportador: DuckDB → PostgreSQL via bulk COPY paralelo e incremental."""

import io
import concurrent.futures
import duckdb
import psycopg2
from sqlalchemy import create_engine, text

MAX_WORKERS = 8


class PostgresExporter:
    """Exporta raw/staging/consume de DuckDB a PostgreSQL.

    Estrategia:
    - Incremental: salta tablas cuyo row-count no cambió en PG.
    - Paralelo: múltiples tablas simultáneas via ThreadPoolExecutor.
    - Bulk load: psycopg2 COPY FROM STDIN, orders of magnitude más rápido que INSERT.
    """

    DUCK_TO_PG: dict[str, str] = {
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

    def __init__(
        self,
        pg_config: dict,
        duckdb_conn: duckdb.DuckDBPyConnection,
        duckdb_path: str | None = None,
    ):
        self.pg_config = pg_config
        self.duckdb_conn = duckdb_conn
        # Path del archivo .duckdb — necesario para abrir conexiones read-only por thread.
        # Si es None (in-memory) el export corre en serie con la conexión compartida.
        self._duckdb_path = duckdb_path
        self._engine = None

    def _get_engine(self):
        if not self._engine:
            cfg = self.pg_config
            url = (
                f"postgresql://{cfg['user']}:{cfg['password']}"
                f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
            )
            self._engine = create_engine(url)
        return self._engine

    def _pg_connect(self) -> psycopg2.extensions.connection:
        cfg = self.pg_config
        return psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database"],
            user=cfg["user"],
            password=cfg["password"],
        )

    def _duck_connect(self) -> tuple[duckdb.DuckDBPyConnection, bool]:
        """Devuelve (conn, should_close). Cada thread necesita su propia conexión."""
        if self._duckdb_path:
            return duckdb.connect(self._duckdb_path, read_only=True), True
        return self.duckdb_conn, False  # in-memory: reutilizar (solo sin paralelo)

    def _pg_row_counts(self, schemas: list[str]) -> dict[str, int]:
        """Snapshot de {schema.table: row_count} en PostgreSQL para detección de cambios."""
        counts: dict[str, int] = {}
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                for schema in schemas:
                    tables = conn.execute(
                        text(
                            "SELECT table_name FROM information_schema.tables "
                            "WHERE table_schema = :s AND table_type = 'BASE TABLE'"
                        ),
                        {"s": schema},
                    ).fetchall()
                    for (tbl,) in tables:
                        n = conn.execute(
                            text(f'SELECT COUNT(*) FROM "{schema}"."{tbl}"')
                        ).scalar()
                        counts[f"{schema}.{tbl}"] = n
        except Exception:
            pass  # PG vacío o primer run → full export
        return counts

    def _build_ddl(self, duck_conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> str:
        """Genera CREATE TABLE DDL para PostgreSQL basado en el schema DuckDB."""
        cols = duck_conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [schema, table],
        ).fetchall()
        col_defs = []
        for col_name, duck_type in cols:
            base = duck_type.split("(")[0].upper()
            pg_type = self.DUCK_TO_PG.get(base, "TEXT")
            col_defs.append(f'  "{col_name}" {pg_type}')
        return f'CREATE TABLE "{schema}"."{table}" (\n' + ",\n".join(col_defs) + "\n)"

    def _export_table(
        self, schema: str, table: str, pg_counts: dict[str, int]
    ) -> str:
        """Exporta una tabla individual. Retorna: 'skip' | 'create' | 'update'."""
        duck_conn, should_close = self._duck_connect()
        try:
            duck_count = duck_conn.execute(
                f'SELECT COUNT(*) FROM "{schema}"."{table}"'
            ).fetchone()[0]

            key = f"{schema}.{table}"
            pg_count = pg_counts.get(key)

            # Incremental: skip si el row-count coincide
            if pg_count is not None and pg_count == duck_count:
                return "skip"

            df = duck_conn.execute(f'SELECT * FROM "{schema}"."{table}"').df()
            ddl = self._build_ddl(duck_conn, schema, table)
        finally:
            if should_close:
                duck_conn.close()

        # CSV en memoria con \N como marcador de NULL (convención PostgreSQL COPY)
        buf = io.BytesIO()
        df.to_csv(buf, index=False, na_rep=r"\N")
        buf.seek(0)

        pg_conn = self._pg_connect()
        try:
            pg_conn.autocommit = False
            with pg_conn.cursor() as cur:
                if pg_count is not None:
                    cur.execute(f'TRUNCATE TABLE "{schema}"."{table}"')
                    action = "update"
                else:
                    cur.execute(ddl)
                    action = "create"
                cur.copy_expert(
                    f"COPY \"{schema}\".\"{table}\" FROM STDIN WITH (FORMAT CSV, HEADER, NULL '\\N')",
                    buf,
                )
            pg_conn.commit()
        except Exception:
            pg_conn.rollback()
            raise
        finally:
            pg_conn.close()

        return action

    def export_all(self, workers: int = MAX_WORKERS) -> dict[str, dict]:
        """Exporta raw, staging y consume a PostgreSQL en paralelo con modo incremental.

        Returns:
            {schema: {"exported": int, "skipped": int, "failed": int}}
        """
        schemas = ["raw", "staging", "consume"]

        # Modo incremental: crear schemas si no existen (no hacemos DROP CASCADE)
        engine = self._get_engine()
        with engine.begin() as conn:
            for schema in schemas:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

        # Row counts en PG actuales para detectar tablas sin cambios
        pg_counts = self._pg_row_counts(schemas)

        # Lista completa de (schema, table) desde DuckDB
        work: list[tuple[str, str]] = []
        for schema in schemas:
            tables = self.duckdb_conn.execute(
                "SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{schema}'"
            ).fetchall()
            work.extend((schema, tbl) for (tbl,) in tables)

        results: dict[str, dict] = {
            s: {"exported": 0, "skipped": 0, "failed": 0} for s in schemas
        }

        # Paralelo si tenemos path (conexiones read-only por thread), serie si in-memory
        effective_workers = workers if self._duckdb_path else 1

        errors: list[str] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=effective_workers) as executor:
            futures = {
                executor.submit(self._export_table, schema, tbl, pg_counts): (schema, tbl)
                for schema, tbl in work
            }
            for future in concurrent.futures.as_completed(futures):
                schema, tbl = futures[future]
                try:
                    action = future.result()
                    if action == "skip":
                        results[schema]["skipped"] += 1
                    else:
                        results[schema]["exported"] += 1
                except Exception as exc:
                    results[schema]["failed"] += 1
                    errors.append(f"{schema}.{tbl}: {exc}")

        if errors:
            raise RuntimeError("Fallos en export:\n" + "\n".join(errors))

        return results

    def close(self):
        if self._engine:
            self._engine.dispose()
            self._engine = None
