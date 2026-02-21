"""Definiciones de Dagster para el pipeline TechStore."""

from dagster import Definitions, define_asset_job, in_process_executor, load_assets_from_modules, AssetSelection
from dagster_dbt import DbtCliResource

from config import DBT_PROJECT_DIR
from . import assets

all_assets = load_assets_from_modules([assets])

resources = {
    "dbt": DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROJECT_DIR,
    ),
}

# Fase 1: ingesta y catalogo (debe correr antes que dbt)
ingestion_job = define_asset_job(
    "ingestion_job",
    selection=AssetSelection.assets(assets.raw_ingestion, assets.duckdb_catalog),
    executor_def=in_process_executor,
)

# Fase 2: transformaciones dbt y export (requiere que ingestion_job haya corrido)
dbt_export_job = define_asset_job(
    "dbt_export_job",
    selection=AssetSelection.assets(assets.dbt_techstore_assets, assets.postgres_export),
    executor_def=in_process_executor,
)

# Job completo para pipeline.py (pipeline.py ya maneja el orden en dos llamadas a materialize)
full_pipeline_job = define_asset_job(
    "full_pipeline",
    selection="*",
    executor_def=in_process_executor,
)

defs = Definitions(
    assets=all_assets,
    jobs=[ingestion_job, dbt_export_job, full_pipeline_job],
    resources=resources,
    executor=in_process_executor,
)
