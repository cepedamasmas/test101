#!/bin/bash

# Asegurar permisos del directorio de output
mkdir -p /app/output
chmod 777 /app/output

echo "Esperando a que MySQL esté listo..."
for i in $(seq 1 60); do
    if python -c "import pymysql; pymysql.connect(host='mysql',port=3306,user='root',password='techstore123',database='techstore').close()" 2>/dev/null; then
        echo "MySQL listo!"
        break
    fi
    echo "  MySQL no disponible, reintentando ($i/60)..."
    sleep 3
done

echo "Esperando a que SFTP esté listo..."
for i in $(seq 1 30); do
    if python -c "import paramiko; t=paramiko.Transport(('sftp',22)); t.connect(username='techstore',password='techstore123'); t.close()" 2>/dev/null; then
        echo "SFTP listo!"
        break
    fi
    echo "  SFTP no disponible, reintentando ($i/30)..."
    sleep 2
done

echo "Esperando a que PostgreSQL esté listo..."
for i in $(seq 1 30); do
    if python -c "import psycopg2; psycopg2.connect(host='postgres',port=5432,user='techstore',password='techstore123',dbname='techstore_lake').close()" 2>/dev/null; then
        echo "PostgreSQL listo!"
        break
    fi
    echo "  PostgreSQL no disponible, reintentando ($i/30)..."
    sleep 2
done

echo ""
echo "Ejecutando pipeline DuckLake..."
python pipeline_ecommerce.py
