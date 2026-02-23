"""Base connector interface."""

from abc import ABC, abstractmethod
from typing import Generator, Union
import pandas as pd


class BaseConnector(ABC):
    """Interfaz base para todos los conectores de ingesta.

    Los conectores pueden implementar extract() con dos patrones según el
    volumen y el formato de la fuente:

    Patrón A — dict (fuentes pequeñas/medianas, carga directa en memoria):
        def extract(self) -> dict[str, pd.DataFrame]:
            return {"tabla": df}

        Uso en assets.py:
            for table, df in connector.extract().items():
                n = raw.save(df, source_name, table)

    Patrón B — generator (fuentes grandes, archivos externos, SFTP):
        def extract(self) -> Generator[tuple[str, str], None, None]:
            yield table_name, tmp_parquet_path

        Uso en assets.py:
            for table, path in connector.extract():
                n = raw.save_from_path(path, source_name, table)
    """

    source_name: str = "unknown"

    @abstractmethod
    def extract(self) -> Union[dict[str, pd.DataFrame], Generator[tuple[str, str], None, None]]:
        """Extrae datos de la fuente.

        Returns:
            dict[str, pd.DataFrame]: {nombre_tabla: DataFrame} para Patrón A.
            Generator[tuple[str, str]]: (nombre_tabla, path_parquet_tmp) para Patrón B.
        """
        raise NotImplementedError

    def close(self) -> None:
        """Cierra conexiones abiertas. Sobreescribir si el conector abre recursos."""
        pass
