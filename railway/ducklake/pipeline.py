"""Pipeline TechStore - Orquestador principal.

Puede ejecutarse de 2 formas:
  1. Railway/Docker: python pipeline.py (ejecuta Dagster assets programaticamente)
  2. Local con UI: dagster dev -m dagster_pipeline (levanta webserver Dagster)
"""

import sys
from datetime import datetime

from dagster import materialize

from dagster_pipeline.assets import raw_ingestion, duckdb_catalog, dbt_techstore_assets, postgres_export
from dagster_pipeline.definitions import resources


def main():
    print("=" * 70)
    print("  TECHSTORE ARGENTINA - Pipeline DuckLake")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("  Powered by: DuckDB + dbt + Dagster")
    print("=" * 70)

    # Fase 1: ingesta → catálogo (garantiza que las vistas RAW existan antes de dbt)
    result = materialize(
        assets=[raw_ingestion, duckdb_catalog],
        resources=resources,
    )
    if not result.success:
        print("\n  Pipeline FALLO en fase de ingesta/catálogo!")
        for step in result.get_failed_step_keys():
            print(f"    Step fallido: {step}")
        return 1

    # Fase 2: dbt + export (las vistas RAW ya están creadas)
    result = materialize(
        assets=[dbt_techstore_assets, postgres_export],
        resources=resources,
    )
    if not result.success:
        print("\n  Pipeline FALLO en fase dbt/export!")
        for step in result.get_failed_step_keys():
            print(f"    Step fallido: {step}")
        return 1

    print(f"\n{'=' * 70}")
    print("  Pipeline completado exitosamente!")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 70}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
