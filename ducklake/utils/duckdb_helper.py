"""Helpers para operaciones con DuckDB."""

from typing import Any

import duckdb
from loguru import logger


def create_connection(
    database: str = ":memory:",
    memory_limit: str = "4GB",
    threads: int = 4,
) -> duckdb.DuckDBPyConnection:
    """Crear conexión DuckDB con configuración optimizada.

    Args:
        database: Path a la base de datos o ":memory:".
        memory_limit: Límite de memoria para DuckDB.
        threads: Número de threads.

    Returns:
        Conexión DuckDB configurada.
    """
    conn = duckdb.connect(database)
    conn.execute(f"SET memory_limit = '{memory_limit}'")
    conn.execute(f"SET threads = {threads}")
    logger.debug(f"DuckDB connection created: {database} (mem={memory_limit}, threads={threads})")
    return conn


def query_parquet(
    conn: duckdb.DuckDBPyConnection,
    pattern: str,
    columns: list[str] | None = None,
    where: str | None = None,
    limit: int | None = None,
) -> Any:
    """Ejecutar query sobre archivos Parquet.

    Args:
        conn: Conexión DuckDB.
        pattern: Glob pattern de archivos parquet.
        columns: Columnas a seleccionar (None = todas).
        where: Cláusula WHERE opcional.
        limit: Límite de filas opcional.

    Returns:
        Resultado de la query (DuckDB relation).
    """
    cols = ", ".join(columns) if columns else "*"
    query = f"SELECT {cols} FROM read_parquet('{pattern}')"
    if where:
        query += f" WHERE {where}"
    if limit:
        query += f" LIMIT {limit}"
    return conn.execute(query)


def get_parquet_schema(conn: duckdb.DuckDBPyConnection, path: str) -> dict[str, str]:
    """Obtener schema de un archivo Parquet.

    Args:
        conn: Conexión DuckDB.
        path: Path al archivo parquet.

    Returns:
        Dict con {columna: tipo}.
    """
    result = conn.execute(
        f"SELECT column_name, column_type FROM parquet_schema('{path}')"
    ).fetchall()
    return {row[0]: row[1] for row in result}


def get_row_count(conn: duckdb.DuckDBPyConnection, path: str) -> int:
    """Contar filas de un archivo Parquet sin cargarlo completo.

    Args:
        conn: Conexión DuckDB.
        path: Path al archivo parquet.

    Returns:
        Número de filas.
    """
    result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()
    return result[0] if result else 0
