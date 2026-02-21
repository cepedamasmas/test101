#!/bin/bash

mkdir -p /app/output
mkdir -p "${DAGSTER_HOME:-/app/output/dagster_home}"
chmod 777 /app/output
chmod 777 "${DAGSTER_HOME:-/app/output/dagster_home}"

# Pre-crear el archivo DuckDB para que dbt pueda conectarse desde el primer run
python -c "import duckdb; duckdb.connect('/app/output/techstore.duckdb').close()" 2>/dev/null || true

# Generar htpasswd con las credenciales de las variables de entorno
DAGSTER_USER="${DAGSTER_USER:-admin}"
DAGSTER_PASSWORD="${DAGSTER_PASSWORD:-admin}"
htpasswd -bc /etc/nginx/.htpasswd "$DAGSTER_USER" "$DAGSTER_PASSWORD"

# Iniciar nginx (queda en background)
nginx -c /app/nginx.conf

echo "Dagster UI disponible en el puerto 3000 (usuario: $DAGSTER_USER)"

# Dagster escucha solo en localhost — nginx es el único punto de entrada
exec dagster-webserver -m dagster_pipeline -h 127.0.0.1 -p 3001
