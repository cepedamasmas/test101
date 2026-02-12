"""Conector para archivos CSV."""

import glob
from pathlib import Path
from typing import Any, Dict

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from ducklake.core.base import BaseConnector


class CSVConnector(BaseConnector):
    """Conector para extraer datos de archivos CSV a Parquet.

    Soporta archivos individuales y patrones glob.
    Usa DuckDB para lectura eficiente de CSV grandes.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.csv_path = config.get("path", "")
        conn_cfg = config.get("connection", {})
        if isinstance(conn_cfg, dict):
            self.delimiter = conn_cfg.get("delimiter", ",")
            self.encoding = conn_cfg.get("encoding", "utf-8")
            self.header = conn_cfg.get("header", True)
            self.skip_rows = conn_cfg.get("skip_rows", 0)
        else:
            self.delimiter = getattr(conn_cfg, "delimiter", ",")
            self.encoding = getattr(conn_cfg, "encoding", "utf-8")
            self.header = getattr(conn_cfg, "header", True)
            self.skip_rows = getattr(conn_cfg, "skip_rows", 0)

    def validate_connection(self) -> bool:
        """Validar que los archivos CSV existen."""
        files = self._resolve_files()
        if not files:
            logger.error(f"CSV connector ({self.name}): no files found at {self.csv_path}")
            return False
        logger.info(f"CSV connection OK: {self.name} ({len(files)} files)")
        return True

    def extract(self, table: str, output_path: str, **kwargs: Any) -> str:
        """Extraer CSV a Parquet usando DuckDB.

        Args:
            table: Nombre lógico (usado como label, el path real viene de config).
            output_path: Path donde guardar el parquet.

        Returns:
            Path del archivo parquet creado.
        """
        # Si hay tablas configuradas, buscar por nombre; sino usar el path global
        csv_path = self._get_csv_path(table)
        files = glob.glob(csv_path)
        if not files:
            raise FileNotFoundError(f"No CSV files found: {csv_path}")

        conn = duckdb.connect()
        try:
            # DuckDB lee CSV de forma eficiente
            options = [
                f"delim='{self.delimiter}'",
                f"header={'true' if self.header else 'false'}",
            ]
            if self.skip_rows > 0:
                options.append(f"skip={self.skip_rows}")

            opts_str = ", ".join(options)

            if len(files) == 1:
                read_expr = f"read_csv('{files[0]}', {opts_str})"
            else:
                file_list = ", ".join(f"'{f}'" for f in files)
                read_expr = f"read_csv([{file_list}], {opts_str})"

            # Agregar metadata
            query = f"""
                SELECT *,
                    CURRENT_TIMESTAMP AS _ingestion_timestamp,
                    '{self.name}' AS _source_name
                FROM {read_expr}
            """

            result = conn.execute(query).arrow()
        finally:
            conn.close()

        # Escribir a Parquet
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(result, output_path, compression="snappy")

        logger.info(f"CSV extract: {table} -> {result.num_rows} rows -> {output_path}")
        return output_path

    def get_schema(self, table: str) -> Dict[str, str]:
        """Obtener schema de un CSV leyendo las primeras filas."""
        csv_path = self._get_csv_path(table)
        files = glob.glob(csv_path)
        if not files:
            return {}

        conn = duckdb.connect()
        try:
            opts = f"delim='{self.delimiter}', header={'true' if self.header else 'false'}"
            result = conn.execute(
                f"SELECT * FROM read_csv('{files[0]}', {opts}) LIMIT 0"
            ).description
            return {col[0]: col[1] for col in result}
        finally:
            conn.close()

    def _resolve_files(self) -> list[str]:
        """Resolver el glob pattern a archivos concretos."""
        if self.csv_path:
            return glob.glob(self.csv_path)
        # Si hay tablas, resolver cada una
        all_files = []
        tables_cfg = self.config.get("tables", [])
        if isinstance(tables_cfg, list):
            for t in tables_cfg:
                if isinstance(t, dict):
                    all_files.extend(glob.glob(t.get("path", "")))
                else:
                    all_files.extend(glob.glob(str(t)))
        return all_files

    def _get_csv_path(self, table: str) -> str:
        """Obtener el path CSV para una tabla específica."""
        # Buscar en tables config por nombre
        tables_cfg = self.config.get("tables", [])
        if isinstance(tables_cfg, list):
            for t in tables_cfg:
                if isinstance(t, dict) and t.get("name") == table:
                    return t.get("path", "")
        # Fallback al path global
        return self.csv_path or ""
