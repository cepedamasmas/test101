#!/bin/bash

mkdir -p /app/output
chmod 777 /app/output

# Pre-crear el archivo DuckDB para que dbt pueda conectarse aunque
# duckdb_catalog no haya corrido todavia (necesario si se ejecuta desde el UI de Dagster)
python -c "import duckdb; duckdb.connect('/app/output/techstore.duckdb').close()" 2>/dev/null || true

echo "Esperando a que SFTP esté listo..."
for i in $(seq 1 30); do
    if python -c "
import paramiko
t = paramiko.Transport(('$SFTP_HOST', int('$SFTP_PORT')))
t.connect(username='$SFTP_USER', password='$SFTP_PASSWORD')
t.close()
" 2>/dev/null; then
        echo "SFTP listo!"
        break
    fi
    echo "  SFTP no disponible, reintentando ($i/30)..."
    sleep 2
done

echo "Esperando a que PostgreSQL esté listo..."
for i in $(seq 1 30); do
    if python -c "
import psycopg2
psycopg2.connect(
    host='$PG_HOST', port=int('$PG_PORT'),
    user='$PG_USER', password='$PG_PASSWORD',
    dbname='$PG_DATABASE'
).close()
" 2>/dev/null; then
        echo "PostgreSQL listo!"
        break
    fi
    echo "  PostgreSQL no disponible, reintentando ($i/30)..."
    sleep 2
done

echo ""
echo "Ejecutando pipeline DuckLake (dbt + Dagster)..."
python pipeline.py
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "Pipeline finalizado OK. Servicio en espera hasta próximo redeploy..."
    sleep infinity
else
    echo "Pipeline FALLÓ con código $EXIT_CODE"
    exit $EXIT_CODE
fi
