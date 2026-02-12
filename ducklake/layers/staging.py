"""STAGING Layer (Silver): Limpieza, normalización y calidad de datos."""

import time
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import pyarrow.parquet as pq
from loguru import logger

from ducklake.core.base import BaseLayer
from ducklake.core.quality import QualityChecker


class StagingLayer(BaseLayer):
    """STAGING Layer: limpia, normaliza y valida datos provenientes de RAW.

    Filosofía: reemplazar particiones, idempotente.
    Path: data/staging/{domain}/{table}/data.parquet
    """

    def write(self, data: Any, destination: Dict[str, Any]) -> str:
        """Escribir datos procesados a STAGING.

        Args:
            data: DuckDB relation o DataFrame.
            destination: Dict con 'domain' y 'table'.

        Returns:
            Path del parquet generado.
        """
        domain = destination.get("domain", "default")
        table = destination["table"]
        dest_path = f"{self.base_path}/staging/{domain}/{table}/data.parquet"
        Path(dest_path).parent.mkdir(parents=True, exist_ok=True)

        if isinstance(data, duckdb.DuckDBPyRelation):
            data.write_parquet(dest_path, compression="snappy")
        else:
            # Asumir DataFrame de pandas
            import pyarrow as pa
            pa_table = pa.Table.from_pandas(data)
            pq.write_table(pa_table, dest_path, compression="snappy")

        logger.info(f"STAGING write: {dest_path}")
        return dest_path

    def read(self, source: Dict[str, Any]) -> str:
        """Construir query para leer datos de STAGING.

        Args:
            source: Dict con 'domain' y 'table'.

        Returns:
            Query SQL string.
        """
        domain = source.get("domain", "default")
        table = source["table"]
        path = f"{self.base_path}/staging/{domain}/{table}/data.parquet"
        return f"SELECT * FROM read_parquet('{path}')"

    def process(self, pipeline_config: Dict[str, Any], raw_query: str) -> Dict[str, Any]:
        """Procesar datos de RAW a STAGING aplicando transformaciones y quality checks.

        Args:
            pipeline_config: Config completa del pipeline.
            raw_query: Query SQL para leer desde RAW.

        Returns:
            Dict con status, path, rows, duration.
        """
        start = time.time()
        pipeline_name = pipeline_config["name"]
        transforms = pipeline_config.get("transforms", [])
        quality_checks = pipeline_config.get("quality_checks", [])
        destination = pipeline_config["destination"]

        # Aplicar transformaciones via SQL
        final_query = self._apply_transforms(raw_query, transforms)

        # Ejecutar y materializar
        result = self.conn.execute(final_query)
        relation = self.conn.sql(final_query)
        row_count = self.conn.execute(f"SELECT COUNT(*) FROM ({final_query})").fetchone()[0]

        # Quality checks
        quality_results = []
        if quality_checks:
            self.conn.execute(f"CREATE OR REPLACE TEMP VIEW __staging_check AS {final_query}")
            checker = QualityChecker(self.conn)
            quality_results = checker.run_checks("__staging_check", quality_checks)
            failed = [r for r in quality_results if not r["passed"]]
            if failed:
                logger.warning(f"Pipeline {pipeline_name}: {len(failed)} quality checks failed")

        # Escribir resultado
        dest_path = self.write(relation, destination)
        duration = time.time() - start

        logger.success(
            f"STAGING pipeline '{pipeline_name}' done: {row_count} rows in {duration:.1f}s"
        )
        return {
            "status": "success",
            "path": dest_path,
            "rows": row_count,
            "duration": duration,
            "quality": quality_results,
        }

    def _apply_transforms(self, base_query: str, transforms: List[Dict[str, Any]]) -> str:
        """Construir query SQL encadenando transformaciones como CTEs.

        Args:
            base_query: Query SQL base (lectura de RAW).
            transforms: Lista de transformaciones.

        Returns:
            Query SQL completa con CTEs.
        """
        if not transforms:
            return base_query

        query = f"WITH base AS ({base_query})"
        prev_step = "base"

        for i, transform in enumerate(transforms):
            step = f"step_{i}"
            t_type = transform["type"]

            if t_type == "rename":
                columns = transform.get("columns", {})
                renames = ", ".join(f"{old} AS {new}" for old, new in columns.items())
                excludes = ", ".join(columns.keys())
                query += f"""
                , {step} AS (
                    SELECT {renames}, * EXCLUDE ({excludes})
                    FROM {prev_step}
                )"""

            elif t_type == "cast":
                columns = transform.get("columns", {})
                casts = ", ".join(
                    f"CAST({col} AS {dtype}) AS {col}" for col, dtype in columns.items()
                )
                excludes = ", ".join(columns.keys())
                query += f"""
                , {step} AS (
                    SELECT {casts}, * EXCLUDE ({excludes})
                    FROM {prev_step}
                )"""

            elif t_type == "filter":
                condition = transform["condition"]
                query += f"""
                , {step} AS (
                    SELECT * FROM {prev_step}
                    WHERE {condition}
                )"""

            elif t_type == "deduplicate":
                keys = ", ".join(transform["keys"])
                query += f"""
                , {step} AS (
                    SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER (
                            PARTITION BY {keys}
                            ORDER BY _ingestion_timestamp DESC
                        ) AS __rn
                        FROM {prev_step}
                    ) sub WHERE __rn = 1
                )"""

            elif t_type == "custom_sql":
                sql = transform["sql"]
                query += f"""
                , {step} AS (
                    {sql.replace('__INPUT__', prev_step)}
                )"""

            else:
                logger.warning(f"Transform type desconocido: {t_type}, skipping")
                continue

            prev_step = step

        # Query final: excluir columnas internas
        query += f" SELECT * EXCLUDE (__rn) FROM {prev_step}"
        # Si no hubo dedup, __rn no existe, usar try
        if not any(t.get("type") == "deduplicate" for t in transforms):
            query = query.replace(" EXCLUDE (__rn)", "")

        return query
