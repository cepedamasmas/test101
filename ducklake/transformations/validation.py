"""Funciones de validación de datos."""

from typing import Any, Dict, List

import duckdb


def validate_not_null(conn: duckdb.DuckDBPyConnection, table: str, columns: List[str]) -> Dict[str, int]:
    """Contar nulos por columna.

    Args:
        conn: Conexión DuckDB.
        table: Nombre de la tabla/view.
        columns: Columnas a validar.

    Returns:
        Dict con {columna: count_nulos}.
    """
    results = {}
    for col in columns:
        count = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL").fetchone()[0]
        results[col] = count
    return results


def validate_unique(conn: duckdb.DuckDBPyConnection, table: str, columns: List[str]) -> int:
    """Contar duplicados en columnas.

    Args:
        conn: Conexión DuckDB.
        table: Nombre de la tabla/view.
        columns: Columnas que deben ser únicas juntas.

    Returns:
        Número de filas duplicadas.
    """
    cols = ", ".join(columns)
    result = conn.execute(
        f"SELECT COUNT(*) - COUNT(DISTINCT ({cols})) FROM {table}"
    ).fetchone()
    return result[0]


def get_column_stats(conn: duckdb.DuckDBPyConnection, table: str, column: str) -> Dict[str, Any]:
    """Obtener estadísticas básicas de una columna.

    Args:
        conn: Conexión DuckDB.
        table: Nombre de la tabla.
        column: Nombre de la columna.

    Returns:
        Dict con min, max, count, nulls, distinct.
    """
    result = conn.execute(f"""
        SELECT
            MIN({column}) AS min_val,
            MAX({column}) AS max_val,
            COUNT(*) AS total,
            COUNT(*) - COUNT({column}) AS nulls,
            COUNT(DISTINCT {column}) AS distinct_count
        FROM {table}
    """).fetchone()
    return {
        "min": result[0],
        "max": result[1],
        "total": result[2],
        "nulls": result[3],
        "distinct": result[4],
    }
