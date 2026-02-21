"""Conector SFTP para ingesta de carpetas Parquet."""

import json
import os
import tempfile
from typing import Generator

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

    def _serialize_nested_fields(self, table: pa.Table) -> pa.Table:
        """Convierte columnas nested (list/struct/map) a JSON strings para compatibilidad tabular."""
        for i, field in enumerate(table.schema):
            if (
                pa.types.is_struct(field.type)
                or pa.types.is_list(field.type)
                or pa.types.is_large_list(field.type)
                or pa.types.is_map(field.type)
            ):
                col = table.column(i)
                json_strings = pa.array(
                    [json.dumps(v.as_py()) if v.is_valid else None for v in col],
                    type=pa.string(),
                )
                table = table.set_column(i, field.name, json_strings)
        return table

    def _extract_folder(self, remote_dir: str) -> str | None:
        """Lee todos los archivos Parquet de una carpeta remota y los escribe a un temp file.

        Procesa un archivo a la vez para mantener el pico de RAM acotado a un
        único archivo fuente, en lugar de acumular todos en memoria.

        Args:
            remote_dir: Ruta remota de la carpeta en el SFTP.

        Returns:
            Path al archivo parquet temporal con todos los datos concatenados,
            o None si la carpeta está vacía.
        """
        filenames = [f for f in self._sftp.listdir(remote_dir) if f.endswith(".parquet")]
        if not filenames:
            return None

        tmp_out = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp_out_path = tmp_out.name
        tmp_out.close()

        writer = None
        try:
            for filename in filenames:
                remote_path = f"{remote_dir}/{filename}"
                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                    tmp_path = tmp.name
                try:
                    self._sftp.get(remote_path, tmp_path)
                    table = pq.read_table(tmp_path)
                    table = table.append_column(
                        "_source_file", pa.array([filename] * len(table), type=pa.string())
                    )
                    table = self._serialize_nested_fields(table)
                    if writer is None:
                        writer = pq.ParquetWriter(tmp_out_path, table.schema)
                    writer.write_table(table)
                    del table
                finally:
                    os.unlink(tmp_path)
        except Exception:
            if writer:
                writer.close()
            os.unlink(tmp_out_path)
            raise

        if writer:
            writer.close()
        else:
            os.unlink(tmp_out_path)
            return None

        return tmp_out_path

    def extract(self) -> Generator[tuple[str, str], None, None]:
        """Genera (table_name, tmp_parquet_path) de a una tabla a la vez.

        El archivo temporal es eliminado automáticamente al avanzar el generador.
        """
        self._connect()
        for table_name, folder_cfg in self.folders.items():
            tmp_path = self._extract_folder(folder_cfg["remote"])
            if tmp_path is None:
                continue
            try:
                yield table_name, tmp_path
            finally:
                os.unlink(tmp_path)

    def close(self):
        if self._sftp:
            self._sftp.close()
        if self._transport:
            self._transport.close()
        self._sftp = None
        self._transport = None
