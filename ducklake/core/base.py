"""Base classes para conectores y capas."""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import duckdb
from loguru import logger


class BaseConnector(ABC):
    """Clase base para todos los conectores de fuentes de datos."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name: str = config["name"]
        self.source_type: str = config["type"]

    @abstractmethod
    def validate_connection(self) -> bool:
        """Validar que la conexión funciona.

        Returns:
            True si la conexión es exitosa.
        """

    @abstractmethod
    def extract(self, table: str, output_path: str, **kwargs: Any) -> str:
        """Extraer datos y guardarlos como Parquet.

        Args:
            table: Nombre de la tabla o archivo a extraer.
            output_path: Path donde guardar el parquet resultante.

        Returns:
            Path del archivo parquet creado.
        """

    @abstractmethod
    def get_schema(self, table: str) -> Dict[str, str]:
        """Obtener schema de una tabla/archivo.

        Args:
            table: Nombre de la tabla.

        Returns:
            Dict con {columna: tipo}.
        """

    def get_extract_mode(self) -> str:
        """Obtener modo de extracción configurado."""
        return self.config.get("extract", {}).get("mode", "full")

    def get_tables(self) -> list[str]:
        """Obtener lista de tablas configuradas."""
        return self.config.get("tables", [])


class BaseLayer(ABC):
    """Clase base para capas del data lake (RAW, STAGING, CONSUME)."""

    def __init__(self, base_path: str, db_conn: duckdb.DuckDBPyConnection | None = None):
        self.base_path = base_path
        self.conn = db_conn or duckdb.connect()

    @abstractmethod
    def write(self, data: Any, destination: Dict[str, Any]) -> str:
        """Escribir datos a la capa.

        Args:
            data: Datos a escribir (path o tabla).
            destination: Config de destino.

        Returns:
            Path donde se guardaron los datos.
        """

    @abstractmethod
    def read(self, source: Dict[str, Any]) -> Any:
        """Leer datos de la capa.

        Args:
            source: Config de la fuente.

        Returns:
            Query string o datos.
        """

    def get_partition_path(self, base: str, date: datetime) -> str:
        """Generar path con particionado por fecha.

        Args:
            base: Path base.
            date: Fecha para la partición.

        Returns:
            Path particionado.
        """
        return f"{base}/year={date.year}/month={date.month:02d}/day={date.day:02d}"

    def ensure_path(self, path: str) -> Path:
        """Crear directorios si no existen.

        Args:
            path: Path a asegurar.

        Returns:
            Path object.
        """
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
