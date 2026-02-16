"""Pipeline TechStore - Orquestador principal.

Puede ejecutarse de 2 formas:
  1. Railway/Docker: python pipeline.py (ejecuta Dagster assets programaticamente)
  2. Local con UI: dagster dev -m dagster_pipeline (levanta webserver Dagster)
"""

import sys
from datetime import datetime

from dagster import materialize, in_process_executor

from dagster_pipeline.definitions import defs


def main():
    print("=" * 70)
    print("  TECHSTORE ARGENTINA - Pipeline DuckLake")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("  Powered by: DuckDB + dbt + Dagster")
    print("=" * 70)

    result = materialize(
        assets=list(defs.get_asset_graph().assets_defs),
        resources=defs.get_resource_defs(),
        executor_def=in_process_executor,
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
