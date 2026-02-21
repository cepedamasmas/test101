"""Dagster assets para el pipeline TechStore.

Cada asset representa un paso del pipeline:
  1. raw_ingestion: Extrae datos de MySQL, SFTP, APIs -> Parquet RAW
  2. duckdb_catalog: Crea vistas RAW en DuckDB
  3. dbt_techstore_assets: Cada modelo dbt como asset individual (staging + consume)
  4. postgres_export: Replica todo a PostgreSQL
"""

import gc
from pathlib import Path

import duckdb
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

from config import (
    MYSQL_CONFIG, SFTP_CONFIG, PG_CONFIG, API_SOURCES,
    MYSQL_TABLES, SFTP_FILES, SFTP_FOLDERS, DATA, DUCKDB_PATH, DBT_PROJECT_DIR, OUTPUT,
    get_raw_tables,
)
from connectors import MySQLConnector, SFTPConnector, APIConnector
from layers import RawLayer
from exporters import DuckDBExporter, PostgresExporter
from reporter import Reporter

# --- dbt project para dagster-dbt ---
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()


@asset(group_name="ingestion", description="Extrae datos de MySQL, SFTP y APIs a RAW parquet")
def raw_ingestion(context: AssetExecutionContext) -> MaterializeResult:
    """Paso 1: Ingesta de todas las fuentes a la capa RAW."""
    OUTPUT.mkdir(parents=True, exist_ok=True)
    raw = RawLayer(DATA)
    total_rows = {}

    # MySQL
    context.log.info(f"Extrayendo MySQL ({MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']})")
    mysql = MySQLConnector(MYSQL_CONFIG, MYSQL_TABLES)
    try:
        for table, df in mysql.extract().items():
            n = raw.save(df, "mysql", table)
            total_rows[f"mysql.{table}"] = n
            context.log.info(f"  RAW: {table:<20} -> {n:>5} rows")
    finally:
        mysql.close()

    # SFTP (archivos individuales + carpetas de JSONs)
    # Usa generator para procesar una tabla a la vez y liberar RAM entre tablas
    context.log.info(f"Extrayendo SFTP ({SFTP_CONFIG['host']}:{SFTP_CONFIG['port']})")
    sftp = SFTPConnector(SFTP_CONFIG, SFTP_FILES, folders=SFTP_FOLDERS)
    try:
        for table, df in sftp.extract():
            n = raw.save(df, "sftp", table)
            total_rows[f"sftp.{table}"] = n
            context.log.info(f"  RAW: {table:<20} -> {n:>5} rows")
            del df
            gc.collect()
    finally:
        sftp.close()

    # APIs
    context.log.info("Extrayendo APIs")
    api = APIConnector(API_SOURCES)
    for table, df in api.extract().items():
        n = raw.save(df, "api", table)
        total_rows[f"api.{table}"] = n
        context.log.info(f"  RAW: {table:<20} -> {n:>5} rows")

    return MaterializeResult(
        metadata={
            "total_sources": MetadataValue.int(len(total_rows)),
            "total_rows": MetadataValue.int(sum(total_rows.values())),
            "tables": MetadataValue.json(total_rows),
        }
    )


@asset(
    group_name="catalog",
    deps=[raw_ingestion],
    description="Crea vistas RAW en DuckDB para que dbt pueda leerlas",
)
def duckdb_catalog(context: AssetExecutionContext) -> MaterializeResult:
    """Paso 2: Registra RAW parquets como vistas en DuckDB."""
    raw_tables = get_raw_tables(DATA)

    conn = duckdb.connect(str(DUCKDB_PATH))
    try:
        exporter = DuckDBExporter(conn, DATA)
        count = exporter.export_raw_views(raw_tables)
        context.log.info(f"{count} vistas RAW creadas en DuckDB")
    finally:
        conn.close()

    return MaterializeResult(
        metadata={"raw_views": MetadataValue.int(count)}
    )


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=None,
)
def dbt_techstore_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Paso 3: Cada modelo dbt como asset individual con linaje."""
    yield from dbt.cli(["build"], context=context).stream()


@asset(
    group_name="export",
    deps=[dbt_techstore_assets],
    description="Exporta DuckDB a PostgreSQL para acceso remoto",
)
def postgres_export(context: AssetExecutionContext) -> MaterializeResult:
    """Paso 4: Replica todas las tablas a PostgreSQL."""
    conn = duckdb.connect(str(DUCKDB_PATH))
    try:
        pg = PostgresExporter(PG_CONFIG, conn)
        try:
            results = pg.export_all()
            for schema, count in results.items():
                context.log.info(f"PostgreSQL {schema}: {count} tablas exportadas")
        finally:
            pg.close()
    finally:
        conn.close()

    # Reportes
    conn = duckdb.connect(str(DUCKDB_PATH))
    try:
        Reporter(conn).print_all()
    finally:
        conn.close()

    total = sum(results.values())
    return MaterializeResult(
        metadata={
            "total_tables_exported": MetadataValue.int(total),
            "by_schema": MetadataValue.json(results),
        }
    )
