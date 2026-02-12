"""Funciones de limpieza de datos."""

from typing import Any, Dict, List


def build_rename_sql(columns: Dict[str, str], source: str = "input") -> str:
    """Construir SQL para renombrar columnas.

    Args:
        columns: Mapeo {nombre_viejo: nombre_nuevo}.
        source: Nombre de la tabla/CTE fuente.

    Returns:
        Query SQL.
    """
    renames = ", ".join(f"{old} AS {new}" for old, new in columns.items())
    excludes = ", ".join(columns.keys())
    return f"SELECT {renames}, * EXCLUDE ({excludes}) FROM {source}"


def build_cast_sql(columns: Dict[str, str], source: str = "input") -> str:
    """Construir SQL para castear tipos.

    Args:
        columns: Mapeo {columna: tipo_destino}.
        source: Nombre de la tabla/CTE fuente.

    Returns:
        Query SQL.
    """
    casts = ", ".join(f"CAST({col} AS {dtype}) AS {col}" for col, dtype in columns.items())
    excludes = ", ".join(columns.keys())
    return f"SELECT {casts}, * EXCLUDE ({excludes}) FROM {source}"


def build_dedup_sql(keys: List[str], order_by: str = "_ingestion_timestamp DESC", source: str = "input") -> str:
    """Construir SQL para deduplicar.

    Args:
        keys: Columnas clave para deduplicación.
        order_by: Orden para elegir qué fila mantener.
        source: Nombre de la tabla/CTE fuente.

    Returns:
        Query SQL.
    """
    keys_str = ", ".join(keys)
    return f"""
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY {keys_str} ORDER BY {order_by}) AS __rn
        FROM {source}
    ) sub WHERE __rn = 1
    """


def build_filter_sql(condition: str, source: str = "input") -> str:
    """Construir SQL para filtrar registros.

    Args:
        condition: Condición WHERE.
        source: Nombre de la tabla/CTE fuente.

    Returns:
        Query SQL.
    """
    return f"SELECT * FROM {source} WHERE {condition}"
