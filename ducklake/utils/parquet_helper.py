"""Helpers para operaciones con Parquet via PyArrow."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger


def write_parquet(
    data: pa.Table,
    path: str,
    compression: str = "snappy",
    row_group_size: int = 100_000,
) -> str:
    """Escribir tabla PyArrow a Parquet.

    Args:
        data: Tabla PyArrow a escribir.
        path: Path destino.
        compression: Algoritmo de compresión.
        row_group_size: Tamaño de row groups.

    Returns:
        Path del archivo escrito.
    """
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(data, path, compression=compression, row_group_size=row_group_size)
    logger.debug(f"Parquet written: {path} ({data.num_rows} rows)")
    return path


def read_parquet(path: str, columns: list[str] | None = None) -> pa.Table:
    """Leer archivo Parquet.

    Args:
        path: Path al archivo.
        columns: Columnas a leer (None = todas).

    Returns:
        Tabla PyArrow.
    """
    return pq.read_table(path, columns=columns)


def get_metadata(path: str) -> dict[str, Any]:
    """Obtener metadata de un archivo Parquet.

    Args:
        path: Path al archivo.

    Returns:
        Dict con metadata (num_rows, num_columns, schema, size_bytes).
    """
    pf = pq.ParquetFile(path)
    metadata = pf.metadata
    file_size = Path(path).stat().st_size

    return {
        "num_rows": metadata.num_rows,
        "num_columns": metadata.num_columns,
        "num_row_groups": metadata.num_row_groups,
        "schema": {
            field.name: str(field.type) for field in pf.schema_arrow
        },
        "size_bytes": file_size,
        "compression": metadata.row_group(0).column(0).compression if metadata.num_row_groups > 0 else None,
    }


def merge_parquet_files(paths: list[str], output_path: str, compression: str = "snappy") -> str:
    """Merge múltiples archivos Parquet en uno.

    Args:
        paths: Lista de paths a archivos parquet.
        output_path: Path destino del merge.
        compression: Algoritmo de compresión.

    Returns:
        Path del archivo mergeado.
    """
    tables = [pq.read_table(p) for p in paths]
    merged = pa.concat_tables(tables)
    return write_parquet(merged, output_path, compression=compression)
