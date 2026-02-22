"""Dagster assets para el pipeline TechStore.

Cada asset representa un paso del pipeline:
  1. raw_ingestion: Extrae ecomm_parquet del SFTP -> Parquet RAW
  2. duckdb_catalog: Crea vistas RAW en DuckDB
  3. dbt_techstore_assets: Cada modelo dbt como asset individual (staging + consume)
  4. postgres_export: Replica todo a PostgreSQL
"""

import duckdb
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from dagster_dbt import DbtCliResource

from config import (
    SFTP_CONFIG, PG_CONFIG,
    SFTP_FOLDERS, DATA, DUCKDB_PATH, DBT_PROJECT_DIR, OUTPUT,
    get_raw_tables,
)
from connectors import SFTPConnector
from layers import RawLayer
from exporters import DuckDBExporter, PostgresExporter


@asset(group_name="ingestion", description="Extrae ecomm_parquet del SFTP a RAW parquet")
def raw_ingestion(context: AssetExecutionContext) -> MaterializeResult:
    """Paso 1: Ingesta de ecomm_parquet SFTP a la capa RAW."""
    OUTPUT.mkdir(parents=True, exist_ok=True)
    raw = RawLayer(DATA)
    total_rows = {}

    # SFTP ecomm_parquet — generator: una tabla a la vez para minimizar pico de RAM
    context.log.info(f"Extrayendo SFTP ecomm_parquet ({SFTP_CONFIG['host']}:{SFTP_CONFIG['port']})")
    sftp = SFTPConnector(SFTP_CONFIG, SFTP_FOLDERS)
    try:
        for table, path in sftp.extract():
            n = raw.save_from_path(path, "sftp", table)
            total_rows[f"sftp.{table}"] = n
            context.log.info(f"  RAW: {table:<20} -> {n:>5} rows")
    finally:
        sftp.close()

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
    import glob as glob_module

    raw_tables = get_raw_tables(DATA)

    # Diagnóstico: mostrar qué paths existe en el volumen
    context.log.info(f"DATA dir: {DATA}")
    context.log.info(f"DATA exists: {DATA.exists()}")
    raw_base = DATA / "raw" / "sftp"
    if raw_base.exists():
        subdirs = [str(p) for p in raw_base.iterdir()]
        context.log.info(f"Carpetas en raw/sftp: {subdirs}")
    else:
        context.log.warning(f"raw/sftp NO existe: {raw_base}")

    # Mostrar cuántos parquets hay por tabla
    for tbl_name, (source, folder) in raw_tables.items():
        pattern = str(DATA / "raw" / source / folder / "*" / "*" / "*" / "data.parquet")
        matches = glob_module.glob(pattern)
        context.log.info(f"  {tbl_name}: {len(matches)} parquets ({pattern})")

    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DUCKDB_PATH))
    try:
        exporter = DuckDBExporter(conn, DATA)
        count = exporter.export_raw_views(raw_tables)
        context.log.info(f"{count} vistas RAW creadas en DuckDB")
        if count == 0:
            raise RuntimeError(
                "0 parquets encontrados en el volumen. "
                "Asegurate de correr raw_ingestion primero (full_pipeline_job)."
            )
    finally:
        conn.close()

    return MaterializeResult(
        metadata={"raw_views": MetadataValue.int(count)}
    )


@asset(
    group_name="transform",
    deps=[duckdb_catalog],
    description="Ejecuta modelos dbt (staging + consume)",
)
def dbt_techstore_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Paso 3: Transforma RAW a staging y consume via dbt."""
    dbt.cli(["build"]).wait()


@asset(
    group_name="export",
    deps=[dbt_techstore_assets],
    description="Exporta DuckDB a PostgreSQL para acceso remoto",
)
def postgres_export(context: AssetExecutionContext) -> MaterializeResult:
    """Paso 4: Replica todas las tablas a PostgreSQL."""
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
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

    total = sum(results.values())
    return MaterializeResult(
        metadata={
            "total_tables_exported": MetadataValue.int(total),
            "by_schema": MetadataValue.json(results),
        }
    )
