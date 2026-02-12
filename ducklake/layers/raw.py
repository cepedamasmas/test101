"""RAW Layer (Bronze): Datos crudos append-only, particionados por fecha."""

import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from loguru import logger

from ducklake.core.base import BaseLayer


class RawLayer(BaseLayer):
    """RAW Layer: recibe datos de conectores y los almacena como Parquet particionado.

    Filosofía: append-only, nunca borrar, máxima fidelidad con la fuente.
    Path: data/raw/{source}/{table}/year=YYYY/month=MM/day=DD/data.parquet
    """

    def write(self, source_path: str, destination: Dict[str, Any]) -> str:
        """Escribir datos a RAW layer copiando el parquet extraído.

        Args:
            source_path: Path del parquet origen (extraído por un connector).
            destination: Dict con 'source' y 'table'.

        Returns:
            Path donde se guardó el archivo.
        """
        source_name = destination["source"]
        table = destination["table"]
        date = datetime.now()

        base = f"{self.base_path}/raw/{source_name}/{table}"
        partition_path = self.get_partition_path(base, date)
        Path(partition_path).mkdir(parents=True, exist_ok=True)

        dest_file = f"{partition_path}/data.parquet"
        shutil.copy2(source_path, dest_file)

        logger.info(f"RAW write: {dest_file}")
        return dest_file

    def read(self, source: Dict[str, Any]) -> str:
        """Construir query DuckDB para leer datos de RAW.

        Args:
            source: Dict con 'source', 'table' y opcionalmente 'date_from'/'date_to'.

        Returns:
            Query SQL string para DuckDB read_parquet.
        """
        pattern = f"{self.base_path}/raw/{source['domain']}/{source['table']}/**/*.parquet"

        query = f"SELECT * FROM read_parquet('{pattern}', hive_partitioning=true)"

        filters = []
        if "date_from" in source and source["date_from"]:
            filters.append(f"_ingestion_timestamp >= '{source['date_from']}'")
        if "date_to" in source and source["date_to"]:
            filters.append(f"_ingestion_timestamp <= '{source['date_to']}'")

        if filters:
            query += " WHERE " + " AND ".join(filters)

        return query

    def list_sources(self) -> list[str]:
        """Listar fuentes disponibles en RAW."""
        raw_path = Path(self.base_path) / "raw"
        if not raw_path.exists():
            return []
        return [p.name for p in raw_path.iterdir() if p.is_dir()]

    def list_tables(self, source: str) -> list[str]:
        """Listar tablas de una fuente en RAW."""
        source_path = Path(self.base_path) / "raw" / source
        if not source_path.exists():
            return []
        return [p.name for p in source_path.iterdir() if p.is_dir()]
