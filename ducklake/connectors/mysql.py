"""Conector para MySQL."""

from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from ducklake.core.base import BaseConnector


class MySQLConnector(BaseConnector):
    """Conector para extraer datos de MySQL a Parquet."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        conn_cfg = config.get("connection", {})
        if isinstance(conn_cfg, dict):
            self.connection_params = {
                "host": conn_cfg.get("host", "localhost"),
                "port": int(conn_cfg.get("port", 3306)),
                "database": conn_cfg.get("database", ""),
                "user": conn_cfg.get("user", ""),
                "password": conn_cfg.get("password", ""),
            }
        else:
            self.connection_params = {
                "host": conn_cfg.host,
                "port": conn_cfg.port,
                "database": conn_cfg.database,
                "user": conn_cfg.user,
                "password": conn_cfg.password,
            }

    def _get_connection(self) -> Any:
        """Crear conexión pymysql."""
        import pymysql

        return pymysql.connect(**self.connection_params)

    def validate_connection(self) -> bool:
        """Validar conexión a MySQL."""
        try:
            conn = self._get_connection()
            conn.close()
            logger.info(f"MySQL connection OK: {self.name}")
            return True
        except Exception as e:
            logger.error(f"MySQL connection failed ({self.name}): {e}")
            return False

    def extract(self, table: str, output_path: str, **kwargs: Any) -> str:
        """Extraer datos de MySQL a Parquet.

        Args:
            table: Nombre de la tabla.
            output_path: Path donde guardar el parquet.

        Returns:
            Path del archivo parquet creado.
        """
        mode = self.get_extract_mode()
        extract_cfg = self.config.get("extract", {})
        if isinstance(extract_cfg, dict):
            batch_size = extract_cfg.get("batch_size", 10_000)
            key_column = extract_cfg.get("key_column")
        else:
            batch_size = extract_cfg.batch_size
            key_column = extract_cfg.key_column

        # Construir query
        if mode == "incremental" and key_column:
            last_value = kwargs.get("last_value", "1970-01-01")
            query = f"SELECT * FROM {table} WHERE {key_column} > %s"
            params = [last_value]
        else:
            query = f"SELECT * FROM {table}"
            params = None

        conn = self._get_connection()
        try:
            if params:
                df = pd.read_sql(query, conn, params=params)
            else:
                df = pd.read_sql(query, conn)
        finally:
            conn.close()

        # Agregar metadata de ingestión
        df["_ingestion_timestamp"] = pd.Timestamp.now()
        df["_source_name"] = self.name

        # Escribir a Parquet
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        pa_table = pa.Table.from_pandas(df)
        pq.write_table(pa_table, output_path, compression="snappy")

        logger.info(f"MySQL extract: {table} -> {len(df)} rows -> {output_path}")
        return output_path

    def get_schema(self, table: str) -> Dict[str, str]:
        """Obtener schema de una tabla MySQL."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"DESCRIBE `{table}`")
            schema = {row[0]: row[1] for row in cursor.fetchall()}
        finally:
            conn.close()
        return schema
