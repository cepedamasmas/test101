"""CONSUME Layer (Gold): Tablas listas para BI, ML, LLM y exports."""

import time
from pathlib import Path
from typing import Any, Dict

import duckdb
import pyarrow.parquet as pq
from loguru import logger

from ducklake.core.base import BaseLayer


class ConsumeLayer(BaseLayer):
    """CONSUME Layer: tablas agregadas y optimizadas para consumo final.

    Sub-paths:
    - data/consume/bi/      - Agregaciones para dashboards
    - data/consume/ml/      - Features para modelos
    - data/consume/llm/     - Datos para embeddings/RAG
    - data/consume/exports/ - Salidas para otros sistemas
    """

    def write(self, data: Any, destination: Dict[str, Any]) -> str:
        """Escribir datos a CONSUME layer.

        Args:
            data: DuckDB relation o DataFrame.
            destination: Dict con 'use_case' (bi/ml/llm/exports) y 'table'.

        Returns:
            Path del parquet generado.
        """
        use_case = destination.get("domain", destination.get("use_case", "bi"))
        table = destination["table"]
        dest_path = f"{self.base_path}/consume/{use_case}/{table}/data.parquet"
        Path(dest_path).parent.mkdir(parents=True, exist_ok=True)

        if isinstance(data, duckdb.DuckDBPyRelation):
            data.write_parquet(dest_path, compression="snappy")
        else:
            import pyarrow as pa
            pa_table = pa.Table.from_pandas(data)
            pq.write_table(pa_table, dest_path, compression="snappy")

        logger.info(f"CONSUME write: {dest_path}")
        return dest_path

    def read(self, source: Dict[str, Any]) -> str:
        """Construir query para leer datos de CONSUME.

        Args:
            source: Dict con 'use_case'/'domain' y 'table'.

        Returns:
            Query SQL string.
        """
        use_case = source.get("domain", source.get("use_case", "bi"))
        table = source["table"]
        path = f"{self.base_path}/consume/{use_case}/{table}/data.parquet"
        return f"SELECT * FROM read_parquet('{path}')"

    def process(self, pipeline_config: Dict[str, Any], staging_query: str) -> Dict[str, Any]:
        """Procesar datos de STAGING a CONSUME.

        Args:
            pipeline_config: Config del pipeline.
            staging_query: Query SQL para leer desde STAGING.

        Returns:
            Dict con status, path, rows, duration.
        """
        start = time.time()
        pipeline_name = pipeline_config["name"]
        destination = pipeline_config["destination"]
        transforms = pipeline_config.get("transforms", [])

        # Aplicar transformaciones custom si las hay
        if transforms:
            final_query = self._apply_consume_transforms(staging_query, transforms)
        else:
            final_query = staging_query

        relation = self.conn.sql(final_query)
        row_count = self.conn.execute(f"SELECT COUNT(*) FROM ({final_query})").fetchone()[0]

        dest_path = self.write(relation, destination)
        duration = time.time() - start

        logger.success(
            f"CONSUME pipeline '{pipeline_name}' done: {row_count} rows in {duration:.1f}s"
        )
        return {
            "status": "success",
            "path": dest_path,
            "rows": row_count,
            "duration": duration,
        }

    def _apply_consume_transforms(
        self, base_query: str, transforms: list[Dict[str, Any]]
    ) -> str:
        """Aplicar transformaciones para CONSUME (agregaciones, custom SQL)."""
        query = f"WITH source AS ({base_query})"
        prev = "source"

        for i, t in enumerate(transforms):
            step = f"consume_step_{i}"
            if t["type"] == "custom_sql":
                sql = t["sql"].replace("__INPUT__", prev)
                query += f", {step} AS ({sql})"
            elif t["type"] == "aggregate":
                group_by = ", ".join(t["group_by"])
                aggs = ", ".join(t["aggregations"])
                query += f"""
                , {step} AS (
                    SELECT {group_by}, {aggs}
                    FROM {prev}
                    GROUP BY {group_by}
                )"""
            else:
                continue
            prev = step

        query += f" SELECT * FROM {prev}"
        return query

    def list_use_cases(self) -> list[str]:
        """Listar use cases disponibles en CONSUME."""
        consume_path = Path(self.base_path) / "consume"
        if not consume_path.exists():
            return []
        return [p.name for p in consume_path.iterdir() if p.is_dir()]
