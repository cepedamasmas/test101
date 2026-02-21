"""Conector SFTP para ingesta de carpetas Parquet."""

import json
import os
import tempfile
from typing import Generator

import pandas as pd
import paramiko
import pyarrow as pa
import pyarrow.parquet as pq

from .base import BaseConnector


class SFTPConnector(BaseConnector):
    """Extrae carpetas de archivos Parquet desde un servidor SFTP."""

    source_name = "sftp"

    def __init__(self, config: dict, folders: dict):
        self.config = config
        self.folders = folders
        self._transport = None
        self._sftp = None

    def _connect(self):
        if not self._transport:
            self._transport = paramiko.Transport((self.config["host"], self.config["port"]))
            self._transport.connect(
                username=self.config["username"], password=self.config["password"]
            )
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)

    def _extract_folder(self, remote_dir: str) -> pd.DataFrame:
        """Lee todos los archivos Parquet de una carpeta remota y los concatena.

        Usa PyArrow para la concatenacion (mas compacto que Pandas por archivo).
        Convierte campos nested (list/dict) a string JSON para compatibilidad tabular.

        Args:
            remote_dir: Ruta remota de la carpeta en el SFTP.
        """
        filenames = [f for f in self._sftp.listdir(remote_dir) if f.endswith(".parquet")]
        arrow_tables = []
        for filename in filenames:
            remote_path = f"{remote_dir}/{filename}"
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_path = tmp.name
            try:
                self._sftp.get(remote_path, tmp_path)
                table = pq.read_table(tmp_path)
                # Agregar columna de origen antes de concat para mantener row counts correctos
                table = table.append_column(
                    "_source_file", pa.array([filename] * len(table), type=pa.string())
                )
                arrow_tables.append(table)
            finally:
                os.unlink(tmp_path)

        if not arrow_tables:
            return pd.DataFrame()

        # Concatenar en PyArrow y convertir a Pandas una sola vez
        combined = pa.concat_tables(arrow_tables, promote_options="default")
        del arrow_tables
        df = combined.to_pandas()
        del combined

        # Serializar campos nested (list/dict) como string JSON
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].apply(
                lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v
            )
        return df

    def extract(self) -> Generator[tuple[str, pd.DataFrame], None, None]:
        """Genera (table_name, df) de a una tabla a la vez para minimizar pico de RAM."""
        self._connect()
        for table_name, folder_cfg in self.folders.items():
            df = self._extract_folder(folder_cfg["remote"])
            yield table_name, df

    def close(self):
        if self._sftp:
            self._sftp.close()
        if self._transport:
            self._transport.close()
        self._sftp = None
        self._transport = None
