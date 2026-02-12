"""Funciones de enriquecimiento de datos."""


def build_date_parts_sql(column: str, source: str = "input") -> str:
    """Agregar columnas de partes de fecha (year, month, day, dow).

    Args:
        column: Columna de fecha a descomponer.
        source: Tabla/CTE fuente.

    Returns:
        Query SQL.
    """
    return f"""
    SELECT *,
        YEAR({column}) AS {column}_year,
        MONTH({column}) AS {column}_month,
        DAY({column}) AS {column}_day,
        DAYOFWEEK({column}) AS {column}_dow
    FROM {source}
    """


def build_hash_key_sql(columns: list[str], key_name: str = "hash_key", source: str = "input") -> str:
    """Generar columna de hash key a partir de múltiples columnas.

    Args:
        columns: Columnas a hashear.
        key_name: Nombre de la columna hash resultante.
        source: Tabla/CTE fuente.

    Returns:
        Query SQL.
    """
    concat = " || '|' || ".join(f"COALESCE(CAST({c} AS VARCHAR), '')" for c in columns)
    return f"SELECT *, MD5({concat}) AS {key_name} FROM {source}"
