"""RAW Layer: ingesta append-only particionada por fecha."""

from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class RawLayer:
    """Guarda DataFrames como parquet en la capa RAW (append-only, particionado por fecha)."""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir

    def save(self, df: pd.DataFrame, source: str, table_name: str) -> int:
        """Guarda DataFrame en RAW layer con metadata de ingestion.

        Returns:
            Cantidad de filas guardadas.
        """
        now = datetime.now()
        raw_dir = (
            self.data_dir / "raw" / source / table_name
            / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
        )
        raw_dir.mkdir(parents=True, exist_ok=True)

        df["_ingestion_timestamp"] = now.isoformat()
        df["_source_name"] = source

        table = pa.Table.from_pandas(df)
        pq.write_table(table, str(raw_dir / "data.parquet"), compression="snappy")
        return len(df)

    def get_path(self, source: str, table_name: str) -> str:
        """Retorna glob path para leer parquets con hive partitioning."""
        return str(self.data_dir / "raw" / source / table_name / "*" / "*" / "*" / "data.parquet")
