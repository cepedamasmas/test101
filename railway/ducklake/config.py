"""Configuración centralizada del pipeline.

Todas las variables se leen desde el entorno. Los defaults están pensados
para el setup local con Docker (ver docker-compose.yml en docker/).

Variables de entorno disponibles:
    OUTPUT_DIR:    Path base donde se guardan Parquet y DuckDB (default: /app/output)
    DUCKDB_PATH:   Path completo al archivo DuckDB (default: OUTPUT_DIR/lake.duckdb)
    PG_HOST:       PostgreSQL host
    PG_PORT:       PostgreSQL port (default: 5432)
    PG_USER:       PostgreSQL user
    PG_PASSWORD:   PostgreSQL password
    PG_DATABASE:   PostgreSQL database name
    SFTP_HOST:     SFTP host
    SFTP_PORT:     SFTP port (default: 22)
    SFTP_USER:     SFTP username
    SFTP_PASSWORD: SFTP password
"""

import os
from pathlib import Path


# --- Paths ---
OUTPUT = Path(os.environ.get("OUTPUT_DIR", "/app/output"))
DATA = OUTPUT / "datalake"
DUCKDB_PATH = Path(os.environ.get("DUCKDB_PATH", str(OUTPUT / "lake.duckdb")))
DBT_PROJECT_DIR = Path(__file__).parent / "dbt_techstore"

# --- SFTP ---
SFTP_CONFIG = {
    "host":     os.environ["SFTP_HOST"],
    "port":     int(os.environ.get("SFTP_PORT", "22")),
    "username": os.environ["SFTP_USER"],
    "password": os.environ["SFTP_PASSWORD"],
}

# --- PostgreSQL ---
PG_CONFIG = {
    "host":     os.environ["PG_HOST"],
    "port":     int(os.environ.get("PG_PORT", "5432")),
    "user":     os.environ["PG_USER"],
    "password": os.environ["PG_PASSWORD"],
    "database": os.environ["PG_DATABASE"],
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
    """Retorna todas las tablas RAW registradas.

    Args:
        data_dir: Path al directorio de datos (reservado para futura detección
                  dinámica desde el filesystem).

    Returns:
        Mapa de {nombre_tabla: (fuente, carpeta)}.
    """
    return dict(BASE_RAW_TABLES)
