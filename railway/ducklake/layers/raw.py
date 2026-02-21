"""RAW Layer: ingesta append-only particionada por fecha."""

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


class RawLayer:
    """Guarda archivos Parquet en la capa RAW (append-only, particionado por fecha)."""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir

    def save_from_path(self, src_path: str, source: str, table_name: str) -> int:
        """Copia un parquet temporal a RAW layer añadiendo columnas de metadata.

        Lee el archivo fuente por row groups para mantener el pico de RAM acotado.

        Args:
            src_path: Path al parquet temporal producido por el conector.
            source: Nombre de la fuente (e.g. 'sftp').
            table_name: Nombre lógico de la tabla.

        Returns:
            Cantidad de filas guardadas.
        """
        now = datetime.now()
        raw_dir = (
            self.data_dir / "raw" / source / table_name
            / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
        )
        raw_dir.mkdir(parents=True, exist_ok=True)
        dest_path = str(raw_dir / "data.parquet")

        now_str = now.isoformat()
        pf = pq.ParquetFile(src_path)

        # Construir schema final añadiendo las columnas de metadata
        base_schema = pf.schema_arrow
        final_schema = base_schema.append(pa.field("_ingestion_timestamp", pa.string()))
        final_schema = final_schema.append(pa.field("_source_name", pa.string()))

        writer = pq.ParquetWriter(dest_path, final_schema, compression="snappy")
        total_rows = 0
        try:
            for batch in pf.iter_batches():
                tbl = pa.Table.from_batches([batch])
                tbl = tbl.append_column(
                    "_ingestion_timestamp", pa.array([now_str] * len(tbl), type=pa.string())
                )
                tbl = tbl.append_column(
                    "_source_name", pa.array([source] * len(tbl), type=pa.string())
                )
                writer.write_table(tbl)
                total_rows += len(tbl)
                del tbl
        finally:
            writer.close()

        return total_rows

    def get_path(self, source: str, table_name: str) -> str:
        """Retorna glob path para leer parquets con hive partitioning."""
        return str(self.data_dir / "raw" / source / table_name / "*" / "*" / "*" / "data.parquet")
