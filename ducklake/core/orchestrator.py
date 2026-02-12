"""Pipeline orchestrator."""

import time
from pathlib import Path
from typing import Any, Dict

from loguru import logger

from ducklake.connectors import get_connector
from ducklake.core.catalog import Catalog
from ducklake.core.config import DuckLakeConfig, load_config
from ducklake.layers import ConsumeLayer, RawLayer, StagingLayer
from ducklake.utils.duckdb_helper import create_connection


class Orchestrator:
    """Orquestador de pipelines: coordina extracciones y transformaciones.

    Responsabilidades:
    - Ejecutar extracciones de fuentes a RAW
    - Ejecutar pipelines de RAW -> STAGING -> CONSUME
    - Registrar todo en el Catalog
    """

    def __init__(self, config_path: str = "./config", data_path: str = "./data"):
        self.config = load_config(config_path)
        self.data_path = data_path
        self.catalog = Catalog(f"{data_path}/catalog.duckdb")

        # Conexión DuckDB compartida
        settings = self.config.settings
        self.conn = create_connection(
            memory_limit=settings.duckdb_memory_limit,
            threads=settings.duckdb_threads,
        )

        # Inicializar layers
        self.raw = RawLayer(data_path, self.conn)
        self.staging = StagingLayer(data_path, self.conn)
        self.consume = ConsumeLayer(data_path, self.conn)

    def run_extraction(self, source_name: str) -> Dict[str, Any]:
        """Ejecutar extracción de una fuente configurada.

        Args:
            source_name: Nombre de la fuente (como en sources.yaml).

        Returns:
            Dict con resultados por tabla.
        """
        logger.info(f"Starting extraction: {source_name}")

        source_config = self._get_source_config(source_name)
        if not source_config:
            raise ValueError(f"Source '{source_name}' not found in config")

        connector = get_connector(source_config)

        if not connector.validate_connection():
            raise ConnectionError(f"Cannot connect to source: {source_name}")

        tables = connector.get_tables()
        if not tables:
            # Para CSV puede ser un solo archivo
            tables = [source_name]

        results: Dict[str, Any] = {}
        for table in tables:
            start = time.time()
            try:
                # Extraer a temporal
                temp_dir = Path(self.data_path) / "_tmp"
                temp_dir.mkdir(parents=True, exist_ok=True)
                temp_path = str(temp_dir / f"{source_name}_{table}.parquet")

                # Pasar last_value para incremental
                kwargs: Dict[str, Any] = {}
                if connector.get_extract_mode() == "incremental":
                    last_date = self.catalog.get_last_extraction(source_name, table)
                    if last_date:
                        kwargs["last_value"] = str(last_date)

                connector.extract(table, temp_path, **kwargs)

                # Mover a RAW layer
                raw_path = self.raw.write(temp_path, {"source": source_name, "table": table})

                # Contar filas
                row_count = self.conn.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{raw_path}')"
                ).fetchone()[0]

                duration = time.time() - start

                # Registrar en catalog
                self.catalog.register_extraction(
                    source=source_name,
                    table=table,
                    rows=row_count,
                    path=raw_path,
                    status="success",
                    file_size=Path(raw_path).stat().st_size,
                    duration=duration,
                )

                results[table] = {"status": "success", "path": raw_path, "rows": row_count}
                logger.success(f"  {table}: {row_count} rows extracted")

                # Limpiar temporal
                Path(temp_path).unlink(missing_ok=True)

            except Exception as e:
                duration = time.time() - start
                self.catalog.register_extraction(
                    source=source_name,
                    table=table,
                    rows=0,
                    path="",
                    status="error",
                    error=str(e),
                    duration=duration,
                )
                results[table] = {"status": "error", "error": str(e)}
                logger.error(f"  {table}: {e}")

        return results

    def run_pipeline(self, pipeline_name: str) -> Dict[str, Any]:
        """Ejecutar un pipeline (RAW->STAGING o STAGING->CONSUME).

        Args:
            pipeline_name: Nombre del pipeline (como en pipelines.yaml).

        Returns:
            Dict con status, output path, rows, duration.
        """
        logger.info(f"Running pipeline: {pipeline_name}")

        pipeline_config = self._get_pipeline_config(pipeline_name)
        if not pipeline_config:
            raise ValueError(f"Pipeline '{pipeline_name}' not found in config")

        # Convertir Pydantic models a dict
        p_dict = pipeline_config.model_dump()
        dest_layer = p_dict["destination"]["layer"]
        source_layer = p_dict["source"]["layer"]

        try:
            if dest_layer == "staging":
                # RAW -> STAGING
                raw_query = self.raw.read(p_dict["source"])
                result = self.staging.process(p_dict, raw_query)
            elif dest_layer == "consume":
                # STAGING -> CONSUME
                staging_query = self.staging.read(p_dict["source"])
                result = self.consume.process(p_dict, staging_query)
            else:
                raise ValueError(f"Unsupported destination layer: {dest_layer}")

            self.catalog.register_pipeline_run(
                pipeline_name=pipeline_name,
                source_layer=source_layer,
                dest_layer=dest_layer,
                rows=result.get("rows", 0),
                status="success",
                duration=result.get("duration", 0),
            )

            # Registrar quality checks
            for qr in result.get("quality", []):
                self.catalog.register_quality_check(
                    pipeline_name=pipeline_name,
                    table_name=p_dict["destination"]["table"],
                    check_type=qr["type"],
                    passed=qr["passed"],
                    details=qr.get("details", ""),
                )

            return result

        except Exception as e:
            self.catalog.register_pipeline_run(
                pipeline_name=pipeline_name,
                source_layer=source_layer,
                dest_layer=dest_layer,
                rows=0,
                status="error",
                error=str(e),
            )
            logger.error(f"Pipeline {pipeline_name} failed: {e}")
            return {"status": "error", "pipeline": pipeline_name, "error": str(e)}

    def _get_source_config(self, name: str) -> Dict[str, Any] | None:
        """Buscar configuración de una fuente por nombre."""
        for src in self.config.sources:
            if src.name == name and src.enabled:
                return src.model_dump()
        return None

    def _get_pipeline_config(self, name: str) -> Any:
        """Buscar configuración de un pipeline por nombre."""
        for p in self.config.pipelines:
            if p.name == name:
                return p
        return None

    def close(self) -> None:
        """Cerrar conexiones."""
        self.catalog.close()
        self.conn.close()
