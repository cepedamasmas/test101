#!/bin/bash

mkdir -p /app/output
chmod 777 /app/output

# Pre-crear el archivo DuckDB para que dbt pueda conectarse desde el primer run
python -c "import duckdb; duckdb.connect('/app/output/techstore.duckdb').close()" 2>/dev/null || true

echo "Levantando Dagster webserver..."
exec dagster-webserver -m dagster_pipeline -h 0.0.0.0 -p 3000
