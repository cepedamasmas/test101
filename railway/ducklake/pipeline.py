"""Pipeline TechStore - Orquestador principal.

Puede ejecutarse de 2 formas:
  1. Railway/Docker: python pipeline.py (ejecuta Dagster assets programaticamente)
  2. Local con UI: dagster dev -m dagster_pipeline (levanta webserver Dagster)
"""

import sys
from datetime import datetime

from dagster import materialize

from dagster_pipeline.assets import raw_ingestion, duckdb_catalog, dbt_models, postgres_export


def main():
    print("=" * 70)
    print("  TECHSTORE ARGENTINA - Pipeline DuckLake")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("  Powered by: DuckDB + dbt + Dagster")
    print("=" * 70)

    result = materialize(
        assets=[raw_ingestion, duckdb_catalog, dbt_models, postgres_export],
    )

    if result.success:
        print(f"\n{'=' * 70}")
        print("  Pipeline completado exitosamente!")
        print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'=' * 70}")
        return 0
    else:
        print("\n  Pipeline FALLO!")
        for event in result.get_failed_step_keys():
            print(f"    Step fallido: {event}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
