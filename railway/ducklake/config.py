"""Configuración centralizada del pipeline TechStore."""

import os
from pathlib import Path


# --- Paths ---
OUTPUT = Path(os.environ.get("OUTPUT_DIR", "/app/output"))
DATA = OUTPUT / "datalake"
DUCKDB_PATH = OUTPUT / "techstore.duckdb"
DBT_PROJECT_DIR = Path(__file__).parent / "dbt_techstore"

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

# --- SFTP folders: ecomm_parquet (Parquet particionados por dia) ---
SFTP_FOLDERS = {
    "vtex_pedido":      {"remote": "/upload/ecomm_parquet/vtex_pedido",      "format": "parquet"},
    "meli_pedido":      {"remote": "/upload/ecomm_parquet/meli_pedido",      "format": "parquet"},
    "meli_pickup":      {"remote": "/upload/ecomm_parquet/meli_pickup",      "format": "parquet"},
    "meli_shipping":    {"remote": "/upload/ecomm_parquet/meli_shipping",    "format": "parquet"},
    "type_6":           {"remote": "/upload/ecomm_parquet/type_6",           "format": "parquet"},
    "type_7":           {"remote": "/upload/ecomm_parquet/type_7",           "format": "parquet"},
    "type_8":           {"remote": "/upload/ecomm_parquet/type_8",           "format": "parquet"},
    "garbarino_pedido": {"remote": "/upload/ecomm_parquet/garbarino_pedido", "format": "parquet"},
}

# --- Tablas RAW (SFTP ecomm_parquet) ---
BASE_RAW_TABLES: dict[str, tuple[str, str]] = {
    "vtex_pedido":      ("sftp", "vtex_pedido"),
    "meli_pedido":      ("sftp", "meli_pedido"),
    "meli_pickup":      ("sftp", "meli_pickup"),
    "meli_shipping":    ("sftp", "meli_shipping"),
    "type_6":           ("sftp", "type_6"),
    "type_7":           ("sftp", "type_7"),
    "type_8":           ("sftp", "type_8"),
    "garbarino_pedido": ("sftp", "garbarino_pedido"),
}


def get_raw_tables(data_dir: Path) -> dict[str, tuple[str, str]]:
    """Retorna todas las tablas RAW disponibles."""
    return dict(BASE_RAW_TABLES)
