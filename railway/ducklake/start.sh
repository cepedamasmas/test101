#!/bin/bash

mkdir -p /app/output
DAGSTER_HOME_DIR="${DAGSTER_HOME:-/app/output/dagster_home}"
mkdir -p "$DAGSTER_HOME_DIR"
chmod 777 /app/output
chmod 777 "$DAGSTER_HOME_DIR"

# Copiar dagster.yaml al DAGSTER_HOME para que Dagster use PostgreSQL storage
cp /app/dagster.yaml "$DAGSTER_HOME_DIR/dagster.yaml" 2>/dev/null || true

# --- Diagnóstico de volumen (aparece en Railway deployment logs) ---
echo "=== VOLUME DIAGNOSTICS ==="
echo "OUTPUT dir: /app/output"
ls -la /app/output/ 2>&1 || echo "(vacío o no accesible)"
PARQUET_COUNT=$(find /app/output/datalake -name "data.parquet" 2>/dev/null | wc -l)
echo "Parquets en volumen: $PARQUET_COUNT"
if [ "$PARQUET_COUNT" -gt 0 ]; then
    echo "Ejemplo de parquets:"
    find /app/output/datalake -name "data.parquet" 2>/dev/null | head -5
fi
echo "=========================="

# Pre-crear el archivo DuckDB para que dbt pueda conectarse desde el primer run
python -c "import duckdb; duckdb.connect('/app/output/techstore.duckdb').close()" 2>/dev/null || true

# Generar htpasswd con las credenciales de las variables de entorno
DAGSTER_USER="${DAGSTER_USER:-admin}"
DAGSTER_PASSWORD="${DAGSTER_PASSWORD:-admin}"
htpasswd -bc /etc/nginx/.htpasswd "$DAGSTER_USER" "$DAGSTER_PASSWORD"

# Procesar nginx.conf con el PORT asignado por Railway (usar sed, no envsubst)
PORT=3000
echo "Arrancando nginx en puerto $PORT"
sed "s/\${PORT}/$PORT/g" /app/nginx.conf > /tmp/nginx.conf
nginx -c /tmp/nginx.conf

echo "Dagster UI disponible en el puerto $PORT (usuario: $DAGSTER_USER)"

# Dagster escucha solo en localhost — nginx es el único punto de entrada
exec dagster-webserver -m dagster_pipeline -h 127.0.0.1 -p 3001
