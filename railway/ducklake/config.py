"""Configuración centralizada del pipeline TechStore."""

import os
from pathlib import Path


# --- Paths ---
OUTPUT = Path(os.environ.get("OUTPUT_DIR", "/app/output"))
DATA = OUTPUT / "datalake"
DUCKDB_PATH = OUTPUT / "techstore.duckdb"
DBT_PROJECT_DIR = Path(__file__).parent / "dbt_techstore"

# --- MySQL ---
MYSQL_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "mysql"),
    "port": int(os.environ.get("MYSQL_PORT", 3306)),
    "user": os.environ.get("MYSQL_USER", "root"),
    "password": os.environ.get("MYSQL_PASSWORD", "techstore123"),
    "database": os.environ.get("MYSQL_DATABASE", "techstore"),
}

# --- SFTP ---
SFTP_CONFIG = {
    "host": os.environ.get("SFTP_HOST", "sftp"),
    "port": int(os.environ.get("SFTP_PORT", 22)),
    "username": os.environ.get("SFTP_USER", "techstore"),
    "password": os.environ.get("SFTP_PASSWORD", "techstore123"),
}

# --- PostgreSQL ---
PG_CONFIG = {
    "host": os.environ.get("PG_HOST", "postgres"),
    "port": os.environ.get("PG_PORT", "5432"),
    "user": os.environ.get("PG_USER", "techstore"),
    "password": os.environ.get("PG_PASSWORD", "techstore123"),
    "database": os.environ.get("PG_DATABASE", "techstore_lake"),
}

# --- MySQL tables to extract ---
MYSQL_TABLES = ["clientes", "productos", "pedidos", "detalle_pedidos"]

# --- SFTP files config ---
SFTP_FILES = {
    "pagos_banco": {"remote": "/upload/pagos_banco.csv", "format": "csv", "opts": {"dtype": {"cbu": str}}},
    "envios_courier": {"remote": "/upload/envios_courier.json", "format": "json"},
    "catalogo_proveedor": {"remote": "/upload/catalogo_proveedor.xml", "format": "xml", "opts": {"xpath": ".//producto"}},
    "liquidacion_mp": {"remote": "/upload/liquidacion_mp.xlsx", "format": "excel"},
    "reclamos": {"remote": "/upload/reclamos.txt", "format": "csv", "opts": {"sep": "|"}},
}

# --- APIs ---
API_SOURCES = {
    "dolar": {
        "url": "https://api.bluelytics.com.ar/v2/latest",
        "timeout": 10,
    },
    "feriados": {
        "url": "https://nolaborables.com.ar/api/v2/feriados/{year}",
        "timeout": 10,
    },
}
