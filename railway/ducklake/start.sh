#!/bin/bash

mkdir -p /app/output
chmod 777 /app/output

echo "Esperando a que MySQL esté listo..."
for i in $(seq 1 60); do
    if python -c "
import pymysql
pymysql.connect(
    host='$MYSQL_HOST', port=int('$MYSQL_PORT'),
    user='$MYSQL_USER', password='$MYSQL_PASSWORD',
    database='$MYSQL_DATABASE'
).close()
" 2>/dev/null; then
        echo "MySQL listo!"
        break
    fi
    echo "  MySQL no disponible, reintentando ($i/60)..."
    sleep 3
done

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

# Seed MySQL si está vacío
echo "Verificando seed data en MySQL..."
ROWS=$(python -c "
import pymysql
conn = pymysql.connect(host='$MYSQL_HOST', port=int('$MYSQL_PORT'), user='$MYSQL_USER', password='$MYSQL_PASSWORD', database='$MYSQL_DATABASE')
with conn.cursor() as c:
    c.execute('SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=\"$MYSQL_DATABASE\" AND table_name=\"clientes\"')
    print(c.fetchone()[0])
conn.close()
" 2>/dev/null)

if [ "$ROWS" = "0" ]; then
    echo "  Tabla clientes no existe, ejecutando seed..."
    python -c "
import pymysql
conn = pymysql.connect(host='$MYSQL_HOST', port=int('$MYSQL_PORT'), user='$MYSQL_USER', password='$MYSQL_PASSWORD', database='$MYSQL_DATABASE')
with open('/app/init.sql', 'r', encoding='utf-8') as f:
    sql = f.read()
with conn.cursor() as c:
    for stmt in sql.split(';'):
        stmt = stmt.strip()
        if stmt:
            c.execute(stmt)
conn.commit()
conn.close()
print('  Seed data cargada!')
"
else
    echo "  MySQL ya tiene datos, skip seed."
fi

echo ""
echo "Ejecutando pipeline DuckLake (dbt + Dagster)..."
python pipeline.py
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "Pipeline finalizado OK. Servicio en espera hasta próximo redeploy..."
    # Mantener el contenedor vivo para que Railway no lo marque como crashed
    sleep infinity
else
    echo "Pipeline FALLÓ con código $EXIT_CODE"
    exit $EXIT_CODE
fi
