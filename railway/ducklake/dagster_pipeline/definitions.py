"""Definiciones de Dagster para el pipeline TechStore."""

from dagster import Definitions, define_asset_job, in_process_executor, load_assets_from_modules
from dagster_dbt import DbtCliResource

from config import DBT_PROJECT_DIR
from . import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[
        define_asset_job("full_pipeline", selection="*", executor_def=in_process_executor),
    ],
    resources={
        "dbt": DbtCliResource(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROJECT_DIR,
        ),
    },
    executor=in_process_executor,
)
