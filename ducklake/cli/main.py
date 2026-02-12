"""DuckLake CLI - Entry point."""

import shutil
from pathlib import Path

import click
from loguru import logger

from ducklake.utils.logger import setup_logger


@click.group()
@click.option("--config", "-c", default="./config", help="Path al directorio de configuración")
@click.option("--data", "-d", default="./data", help="Path al directorio de datos")
@click.option("--log-level", default="INFO", help="Nivel de logging")
@click.pass_context
def cli(ctx: click.Context, config: str, data: str, log_level: str) -> None:
    """DuckLake - Data Lake Framework con DuckDB."""
    setup_logger(level=log_level)
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config
    ctx.obj["data_path"] = data


@cli.command()
@click.argument("project_name")
@click.option("--path", default=".", help="Path donde crear el proyecto")
def init(project_name: str, path: str) -> None:
    """Inicializar un nuevo proyecto DuckLake."""
    project_path = Path(path) / project_name

    click.echo(f"Inicializando proyecto DuckLake: {project_name}")

    # Crear estructura de carpetas
    dirs = [
        "config",
        "data/raw",
        "data/staging",
        "data/consume",
        "tests/unit",
        "tests/integration",
        "examples",
        "docs",
    ]
    for d in dirs:
        (project_path / d).mkdir(parents=True, exist_ok=True)

    # sources.yaml
    (project_path / "config" / "sources.yaml").write_text(
        """# DuckLake Sources Configuration
sources:
  - name: example_csv
    type: csv
    enabled: true
    path: "data/raw/example/*.csv"
    connection:
      delimiter: ","
      encoding: utf-8
      header: true
    tables:
      - example_table
    extract:
      mode: full
""",
        encoding="utf-8",
    )

    # pipelines.yaml
    (project_path / "config" / "pipelines.yaml").write_text(
        """# DuckLake Pipelines Configuration
pipelines:
  - name: example_staging
    description: "Example pipeline: RAW to STAGING"
    source:
      layer: raw
      domain: example_csv
      table: example_table
    destination:
      layer: staging
      domain: example
      table: example_table
    transforms:
      - type: filter
        condition: "1=1"
    quality_checks:
      - type: not_null
        columns: []
""",
        encoding="utf-8",
    )

    # settings.yaml
    (project_path / "config" / "settings.yaml").write_text(
        """# DuckLake Settings
settings:
  data_path: ./data
  config_path: ./config
  log_level: INFO
  duckdb_memory_limit: 4GB
  duckdb_threads: 4
""",
        encoding="utf-8",
    )

    # .env
    (project_path / ".env.example").write_text(
        """# DuckLake Environment Variables
DUCKLAKE_DATA_PATH=./data
DUCKLAKE_CONFIG_PATH=./config
DUCKLAKE_LOG_LEVEL=INFO
""",
        encoding="utf-8",
    )

    # .gitignore
    (project_path / ".gitignore").write_text(
        """data/raw/
data/staging/
data/consume/
*.parquet
*.duckdb
*.duckdb.wal
__pycache__/
.env
.venv/
""",
        encoding="utf-8",
    )

    click.echo(f"Proyecto creado en: {project_path.resolve()}")
    click.echo("Estructura:")
    click.echo(f"  {project_name}/config/     - Configuraciones YAML")
    click.echo(f"  {project_name}/data/       - Datos (raw/staging/consume)")
    click.echo(f"  {project_name}/tests/      - Tests")
    click.echo(f"  {project_name}/examples/   - Ejemplos")
    click.echo("")
    click.echo("Siguiente paso: editar config/sources.yaml con tus fuentes de datos")


@cli.command()
@click.argument("source_name")
@click.pass_context
def extract(ctx: click.Context, source_name: str) -> None:
    """Extraer datos de una fuente a RAW layer."""
    from ducklake.core.orchestrator import Orchestrator

    config_path = ctx.obj["config_path"]
    data_path = ctx.obj["data_path"]

    click.echo(f"Extrayendo datos de: {source_name}")

    orch = Orchestrator(config_path, data_path)
    try:
        results = orch.run_extraction(source_name)
        for table, result in results.items():
            if result["status"] == "success":
                click.echo(f"  OK  {table}: {result.get('rows', 0)} rows -> {result['path']}")
            else:
                click.echo(f"  ERR {table}: {result['error']}", err=True)
    finally:
        orch.close()


@cli.command()
@click.argument("pipeline_name")
@click.pass_context
def run(ctx: click.Context, pipeline_name: str) -> None:
    """Ejecutar un pipeline de transformación."""
    from ducklake.core.orchestrator import Orchestrator

    config_path = ctx.obj["config_path"]
    data_path = ctx.obj["data_path"]

    click.echo(f"Ejecutando pipeline: {pipeline_name}")

    orch = Orchestrator(config_path, data_path)
    try:
        result = orch.run_pipeline(pipeline_name)
        if result["status"] == "success":
            click.echo(f"  OK  {result.get('rows', 0)} rows -> {result['path']}")
            quality = result.get("quality", [])
            if quality:
                passed = sum(1 for q in quality if q["passed"])
                click.echo(f"  Quality: {passed}/{len(quality)} checks passed")
        else:
            click.echo(f"  ERR {result.get('error', 'Unknown error')}", err=True)
    finally:
        orch.close()


@cli.command()
@click.option("--extractions", "-e", is_flag=True, help="Mostrar extracciones recientes")
@click.option("--pipelines", "-p", is_flag=True, help="Mostrar pipelines recientes")
@click.option("--limit", "-n", default=10, help="Número de registros")
@click.pass_context
def catalog(ctx: click.Context, extractions: bool, pipelines: bool, limit: int) -> None:
    """Ver catálogo de metadata."""
    from ducklake.core.catalog import Catalog

    data_path = ctx.obj["data_path"]
    cat = Catalog(f"{data_path}/catalog.duckdb")

    # Si no se especifica flag, mostrar ambos
    show_all = not extractions and not pipelines

    try:
        if extractions or show_all:
            click.echo("\n--- Extracciones Recientes ---")
            rows = cat.get_recent_extractions(limit)
            if rows:
                click.echo(f"{'Source':<20} {'Table':<20} {'Rows':>10} {'Status':<10} {'Duration':>8}")
                click.echo("-" * 72)
                for r in rows:
                    click.echo(
                        f"{r['source']:<20} {r['table']:<20} {r['rows']:>10} "
                        f"{r['status']:<10} {r['duration']:>7.1f}s"
                    )
            else:
                click.echo("  (sin datos)")

        if pipelines or show_all:
            click.echo("\n--- Pipelines Recientes ---")
            rows = cat.get_recent_pipeline_runs(limit)
            if rows:
                click.echo(
                    f"{'Pipeline':<25} {'Source':<10} {'Dest':<10} "
                    f"{'Rows':>10} {'Status':<10} {'Duration':>8}"
                )
                click.echo("-" * 78)
                for r in rows:
                    click.echo(
                        f"{r['pipeline']:<25} {r['source_layer']:<10} {r['dest_layer']:<10} "
                        f"{r['rows']:>10} {r['status']:<10} {r['duration']:>7.1f}s"
                    )
            else:
                click.echo("  (sin datos)")
    finally:
        cat.close()


@cli.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Mostrar estado general del data lake."""
    data_path = ctx.obj["data_path"]

    click.echo("DuckLake Status")
    click.echo("=" * 40)

    for layer in ["raw", "staging", "consume"]:
        layer_path = Path(data_path) / layer
        if layer_path.exists():
            parquet_files = list(layer_path.rglob("*.parquet"))
            total_size = sum(f.stat().st_size for f in parquet_files)
            size_mb = total_size / (1024 * 1024)
            click.echo(f"  {layer.upper():<10} {len(parquet_files):>5} files  {size_mb:>10.1f} MB")
        else:
            click.echo(f"  {layer.upper():<10}     0 files       0.0 MB")


def main() -> None:
    """Entry point."""
    cli()


if __name__ == "__main__":
    main()
