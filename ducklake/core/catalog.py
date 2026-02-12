"""Metadata catalog usando DuckDB."""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
from loguru import logger


class Catalog:
    """Catálogo de metadata para tracking de extracciones, pipelines y calidad.

    Usa DuckDB como almacenamiento persistente para metadata del data lake.
    """

    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(db_path)
        self._init_tables()

    def _init_tables(self) -> None:
        """Crear tablas de metadata si no existen."""
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_extractions START 1;
            CREATE TABLE IF NOT EXISTS extractions (
                id INTEGER DEFAULT nextval('seq_extractions') PRIMARY KEY,
                source_name VARCHAR NOT NULL,
                table_name VARCHAR NOT NULL,
                extraction_date TIMESTAMP NOT NULL,
                rows_extracted INTEGER DEFAULT 0,
                file_path VARCHAR,
                file_size_bytes BIGINT DEFAULT 0,
                status VARCHAR NOT NULL,
                error_message VARCHAR,
                duration_seconds FLOAT DEFAULT 0
            );
        """)

        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_pipeline_runs START 1;
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id INTEGER DEFAULT nextval('seq_pipeline_runs') PRIMARY KEY,
                pipeline_name VARCHAR NOT NULL,
                execution_date TIMESTAMP NOT NULL,
                source_layer VARCHAR,
                destination_layer VARCHAR,
                rows_processed INTEGER DEFAULT 0,
                duration_seconds FLOAT DEFAULT 0,
                status VARCHAR NOT NULL,
                error_message VARCHAR
            );
        """)

        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_data_quality START 1;
            CREATE TABLE IF NOT EXISTS data_quality (
                id INTEGER DEFAULT nextval('seq_data_quality') PRIMARY KEY,
                pipeline_name VARCHAR,
                table_name VARCHAR NOT NULL,
                check_type VARCHAR NOT NULL,
                check_date TIMESTAMP NOT NULL,
                passed BOOLEAN NOT NULL,
                details VARCHAR
            );
        """)

    def register_extraction(
        self,
        source: str,
        table: str,
        rows: int,
        path: str,
        status: str,
        file_size: int = 0,
        duration: float = 0.0,
        error: str | None = None,
    ) -> None:
        """Registrar una extracción en el catálogo."""
        self.conn.execute(
            """
            INSERT INTO extractions
                (source_name, table_name, extraction_date, rows_extracted,
                 file_path, file_size_bytes, status, error_message, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [source, table, datetime.now(), rows, path, file_size, status, error, duration],
        )

    def register_pipeline_run(
        self,
        pipeline_name: str,
        source_layer: str,
        dest_layer: str,
        rows: int,
        status: str,
        duration: float = 0.0,
        error: str | None = None,
    ) -> None:
        """Registrar la ejecución de un pipeline."""
        self.conn.execute(
            """
            INSERT INTO pipeline_runs
                (pipeline_name, execution_date, source_layer, destination_layer,
                 rows_processed, duration_seconds, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [pipeline_name, datetime.now(), source_layer, dest_layer, rows, duration, status, error],
        )

    def register_quality_check(
        self,
        pipeline_name: str,
        table_name: str,
        check_type: str,
        passed: bool,
        details: str = "",
    ) -> None:
        """Registrar resultado de un quality check."""
        self.conn.execute(
            """
            INSERT INTO data_quality
                (pipeline_name, table_name, check_type, check_date, passed, details)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [pipeline_name, table_name, check_type, datetime.now(), passed, details],
        )

    def get_last_extraction(self, source: str, table: str) -> Optional[datetime]:
        """Obtener timestamp de la última extracción exitosa."""
        result = self.conn.execute(
            """
            SELECT MAX(extraction_date)
            FROM extractions
            WHERE source_name = ? AND table_name = ? AND status = 'success'
            """,
            [source, table],
        ).fetchone()
        return result[0] if result and result[0] else None

    def get_recent_extractions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Obtener las extracciones más recientes."""
        rows = self.conn.execute(
            """
            SELECT source_name, table_name, extraction_date,
                   rows_extracted, status, duration_seconds
            FROM extractions
            ORDER BY extraction_date DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [
            {
                "source": r[0],
                "table": r[1],
                "date": r[2],
                "rows": r[3],
                "status": r[4],
                "duration": r[5],
            }
            for r in rows
        ]

    def get_recent_pipeline_runs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Obtener las ejecuciones de pipelines más recientes."""
        rows = self.conn.execute(
            """
            SELECT pipeline_name, execution_date, source_layer,
                   destination_layer, rows_processed, status, duration_seconds
            FROM pipeline_runs
            ORDER BY execution_date DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [
            {
                "pipeline": r[0],
                "date": r[1],
                "source_layer": r[2],
                "dest_layer": r[3],
                "rows": r[4],
                "status": r[5],
                "duration": r[6],
            }
            for r in rows
        ]

    def close(self) -> None:
        """Cerrar conexión al catálogo."""
        self.conn.close()
