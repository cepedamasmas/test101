"""Definiciones de Dagster para el pipeline TechStore."""

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from config import DBT_PROJECT_DIR
from . import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": DbtCliResource(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROJECT_DIR,
        ),
    },
)
