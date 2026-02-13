"""Base connector interface."""

from abc import ABC, abstractmethod
import pandas as pd


class BaseConnector(ABC):
    """Interfaz base para todos los conectores de ingesta."""

    source_name: str = "unknown"

    @abstractmethod
    def extract(self) -> dict[str, pd.DataFrame]:
        """Extrae datos y retorna {table_name: DataFrame}."""
        raise NotImplementedError

    def close(self) -> None:
        """Cierra conexiones abiertas."""
        pass
