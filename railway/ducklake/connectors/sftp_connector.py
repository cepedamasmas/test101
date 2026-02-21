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

    def _get_serialized_schema(self, schema: pa.Schema) -> pa.Schema:
        """Retorna el schema resultante después de serializar campos nested a string."""
        fields = []
        for field in schema:
            if (
                pa.types.is_struct(field.type)
                or pa.types.is_list(field.type)
                or pa.types.is_large_list(field.type)
                or pa.types.is_map(field.type)
            ):
                fields.append(pa.field(field.name, pa.string()))
            else:
                fields.append(field)
        return pa.schema(fields)

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

    def _align_to_schema(self, table: pa.Table, target_schema: pa.Schema) -> pa.Table:
        """Reordena, completa y castea columnas para que el table coincida con el schema destino.

        Maneja diferencias entre archivos: columnas en distinto orden, columnas
        faltantes (se completan con nulls) y tipos compatibles (se castean).
        """
        arrays = []
        for field in target_schema:
            if field.name in table.schema.names:
                col = table.column(field.name)
                if col.type != field.type:
                    col = col.cast(field.type, safe=False)
                arrays.append(col)
            else:
                # Columna ausente en este archivo → llenar con nulls tipados
                arrays.append(pa.nulls(len(table), type=field.type))
        return pa.table(arrays, schema=target_schema)

    def _extract_folder(self, remote_dir: str) -> str | None:
        """Lee todos los Parquet de una carpeta remota y los escribe a un temp file.

        Fase 1: descarga todos los archivos a disco (sin RAM spike).
        Fase 2: unifica schemas y procesa un archivo a la vez (RAM = 1 archivo).

        Args:
            remote_dir: Ruta remota de la carpeta en el SFTP.

        Returns:
            Path al parquet temporal con todos los datos, o None si está vacía.
        """
        filenames = [f for f in self._sftp.listdir(remote_dir) if f.endswith(".parquet")]
        if not filenames:
            return None

        # Fase 1: descargar todos a disco (I/O, sin carga en RAM)
        tmp_files: list[tuple[str, str]] = []
        for filename in filenames:
            remote_path = f"{remote_dir}/{filename}"
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_path = tmp.name
            self._sftp.get(remote_path, tmp_path)
            tmp_files.append((filename, tmp_path))

        try:
            # Unificar schemas (lee solo metadata, sin cargar datos)
            schemas = []
            for filename, tmp_path in tmp_files:
                raw_schema = pq.read_schema(tmp_path)
                serialized = self._get_serialized_schema(raw_schema)
                serialized = serialized.append(pa.field("_source_file", pa.string()))
                schemas.append(serialized)
            unified_schema = pa.unify_schemas(schemas)

            # Fase 2: procesar un archivo a la vez contra el schema unificado
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_out:
                tmp_out_path = tmp_out.name

            writer = pq.ParquetWriter(tmp_out_path, unified_schema)
            write_ok = False
            try:
                for filename, tmp_path in tmp_files:
                    table = pq.read_table(tmp_path)
                    table = table.append_column(
                        "_source_file", pa.array([filename] * len(table), type=pa.string())
                    )
                    table = self._serialize_nested_fields(table)
                    table = self._align_to_schema(table, unified_schema)
                    writer.write_table(table)
                    del table
                write_ok = True
            finally:
                writer.close()
                if not write_ok:
                    os.unlink(tmp_out_path)
        finally:
            for _, tmp_path in tmp_files:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

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
