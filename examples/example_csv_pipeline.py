"""
Ejemplo: Pipeline completo CSV -> RAW -> STAGING -> CONSUME

Este script demuestra cómo usar DuckLake programáticamente
sin depender del CLI, ideal para integrarlo en scripts propios.
"""

import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ducklake.connectors.csv_connector import CSVConnector
from ducklake.core.catalog import Catalog
from ducklake.core.quality import QualityChecker
from ducklake.layers import ConsumeLayer, RawLayer, StagingLayer


def main():
    # 1. Setup: crear datos de ejemplo
    tmp_dir = tempfile.mkdtemp(prefix="ducklake_example_")
    data_dir = f"{tmp_dir}/data"
    Path(f"{data_dir}/raw").mkdir(parents=True)
    Path(f"{data_dir}/staging").mkdir(parents=True)
    Path(f"{data_dir}/consume").mkdir(parents=True)

    # Crear CSV de ejemplo
    csv_path = f"{tmp_dir}/ventas.csv"
    Path(csv_path).write_text(
        "venta_id,cliente,producto,cantidad,precio_unitario,fecha\n"
        "1,Alice,Widget A,2,10.50,2024-01-15\n"
        "2,Bob,Widget B,1,25.00,2024-01-16\n"
        "3,Alice,Widget A,3,10.50,2024-01-17\n"
        "4,Charlie,Widget C,1,50.00,2024-02-01\n"
        "5,Bob,Widget A,5,10.50,2024-02-10\n"
        "6,Diana,Widget B,2,25.00,2024-02-15\n"
        "7,Alice,Widget C,1,50.00,2024-03-01\n"
        "8,Eve,Widget A,10,10.50,2024-03-05\n"
    )

    print("=" * 60)
    print("DuckLake Example: CSV Pipeline E2E")
    print("=" * 60)

    # 2. Extracción: CSV -> Parquet
    print("\n[1/4] Extracting CSV to Parquet...")
    connector = CSVConnector({
        "name": "ventas_csv",
        "type": "csv",
        "path": csv_path,
        "connection": {"delimiter": ",", "header": True},
        "tables": [],
        "extract": {"mode": "full"},
    })

    assert connector.validate_connection()
    extracted_path = f"{tmp_dir}/extracted.parquet"
    connector.extract("ventas", extracted_path)
    print(f"  Extracted to: {extracted_path}")

    # 3. RAW Layer: almacenar datos crudos
    print("\n[2/4] Writing to RAW layer...")
    conn = duckdb.connect()
    raw = RawLayer(data_dir, conn)
    raw_path = raw.write(extracted_path, {"source": "ventas_csv", "table": "ventas"})
    print(f"  RAW path: {raw_path}")

    # 4. STAGING Layer: limpiar y transformar
    print("\n[3/4] Processing RAW -> STAGING...")
    staging = StagingLayer(data_dir, conn)
    raw_query = raw.read({"domain": "ventas_csv", "table": "ventas"})

    pipeline_config = {
        "name": "ventas_staging",
        "source": {"layer": "raw", "domain": "ventas_csv", "table": "ventas"},
        "destination": {"layer": "staging", "domain": "ventas", "table": "ventas"},
        "transforms": [
            {"type": "filter", "condition": "cantidad > 0"},
        ],
        "quality_checks": [
            {"type": "not_null", "columns": ["venta_id", "cliente", "producto"]},
            {"type": "unique", "columns": ["venta_id"]},
        ],
    }

    staging_result = staging.process(pipeline_config, raw_query)
    print(f"  STAGING result: {staging_result['rows']} rows")
    print(f"  Quality checks: {len(staging_result.get('quality', []))} passed")

    # 5. CONSUME Layer: agregar para BI
    print("\n[4/4] Processing STAGING -> CONSUME (BI aggregation)...")
    consume = ConsumeLayer(data_dir, conn)
    staging_query = staging.read({"domain": "ventas", "table": "ventas"})

    consume_config = {
        "name": "bi_resumen_ventas",
        "destination": {"domain": "bi", "table": "resumen_mensual"},
        "transforms": [
            {
                "type": "custom_sql",
                "sql": """
                    SELECT
                        producto,
                        SUM(cantidad) AS total_cantidad,
                        SUM(cantidad * precio_unitario) AS total_revenue,
                        COUNT(DISTINCT cliente) AS clientes_unicos
                    FROM __INPUT__
                    GROUP BY producto
                    ORDER BY total_revenue DESC
                """,
            }
        ],
    }

    consume_result = consume.process(consume_config, staging_query)
    print(f"  CONSUME result: {consume_result['rows']} rows")
    print(f"  Output: {consume_result['path']}")

    # 6. Verificar resultado final
    print("\n" + "=" * 60)
    print("Final Result (CONSUME - BI Resumen):")
    print("=" * 60)
    final = conn.execute(
        f"SELECT * FROM read_parquet('{consume_result['path']}')"
    ).fetchdf()
    print(final.to_string(index=False))

    conn.close()
    print(f"\nData directory: {data_dir}")
    print("Done!")


if __name__ == "__main__":
    main()
