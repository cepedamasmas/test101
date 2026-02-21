"""Conector SFTP para ingesta de archivos multi-formato."""

import json
import os
import tempfile

import pandas as pd
import paramiko

from .base import BaseConnector


class SFTPConnector(BaseConnector):
    """Extrae archivos desde un servidor SFTP (CSV, JSON, XML, Excel, TXT)."""

    source_name = "sftp"

    def __init__(self, config: dict, files: dict, folders: dict | None = None):
        self.config = config
        self.files = files
        self.folders = folders or {}
        self._transport = None
        self._sftp = None

    def _connect(self):
        if not self._transport:
            self._transport = paramiko.Transport((self.config["host"], self.config["port"]))
            self._transport.connect(
                username=self.config["username"], password=self.config["password"]
            )
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)

    def _download_and_read(self, remote_path: str, fmt: str, opts: dict) -> pd.DataFrame:
        """Descarga archivo del SFTP y lo parsea segun formato."""
        suffix = f".{fmt}" if fmt != "excel" else ".xlsx"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp_path = tmp.name

        try:
            self._sftp.get(remote_path, tmp_path)
            return self._read_file(tmp_path, fmt, opts)
        finally:
            os.unlink(tmp_path)

    def _read_file(self, path: str, fmt: str, opts: dict) -> pd.DataFrame:
        """Lee archivo local segun formato."""
        if fmt == "csv":
            return pd.read_csv(path, **opts)
        elif fmt == "json":
            return self._read_json(path)
        elif fmt == "xml":
            return pd.read_xml(path, **opts)
        elif fmt == "excel":
            try:
                return pd.read_excel(path, engine="openpyxl")
            except Exception:
                csv_path = path.replace(".xlsx", ".csv")
                self._sftp.get(
                    opts.get("fallback_remote", path.replace(".xlsx", ".csv")), csv_path
                )
                return pd.read_csv(csv_path)
        else:
            return pd.read_csv(path, **opts)

    def _read_json(self, path: str) -> pd.DataFrame:
        """Lee JSON aplanando campos nested como string."""
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for row in data:
            for key, val in row.items():
                if isinstance(val, (list, dict)):
                    row[key] = json.dumps(val)
        return pd.DataFrame(data)

    def _extract_folder(self, remote_dir: str, fmt: str = "json") -> pd.DataFrame:
        """Lee todos los archivos de una carpeta remota y los concatena en un DataFrame.

        Soporta formato JSON (un objeto por archivo) y Parquet (tabla columnar).
        Se aplanan campos nested como strings para formato tabular.

        Args:
            remote_dir: Ruta remota de la carpeta en el SFTP.
            fmt: Formato de los archivos ('json' o 'parquet').
        """
        if fmt == "parquet":
            filenames = [f for f in self._sftp.listdir(remote_dir) if f.endswith(".parquet")]
            dfs = []
            for filename in filenames:
                remote_path = f"{remote_dir}/{filename}"
                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                    tmp_path = tmp.name
                try:
                    self._sftp.get(remote_path, tmp_path)
                    df = pd.read_parquet(tmp_path)
                    # Serializar campos nested (list/dict) como string JSON
                    for col in df.columns:
                        df[col] = df[col].apply(
                            lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v
                        )
                    df["_source_file"] = filename
                    dfs.append(df)
                finally:
                    os.unlink(tmp_path)
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        # Formato JSON: un archivo por registro
        filenames = [f for f in self._sftp.listdir(remote_dir) if f.endswith(".json")]
        rows = []
        for filename in filenames:
            remote_path = f"{remote_dir}/{filename}"
            with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
                tmp_path = tmp.name
            try:
                self._sftp.get(remote_path, tmp_path)
                with open(tmp_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # Si es lista (ej: meli_shipping), la guardamos como está
                if isinstance(data, list):
                    row = {"_payload": json.dumps(data)}
                else:
                    row = {}
                    for key, val in data.items():
                        row[key] = json.dumps(val) if isinstance(val, (list, dict)) else val
                row["_source_file"] = filename
                rows.append(row)
            finally:
                os.unlink(tmp_path)
        return pd.DataFrame(rows)

    def extract(self) -> dict[str, pd.DataFrame]:
        self._connect()
        results = {}
        for table_name, file_cfg in self.files.items():
            opts = file_cfg.get("opts", {})
            df = self._download_and_read(file_cfg["remote"], file_cfg["format"], opts)
            results[table_name] = df
        for table_name, folder_cfg in self.folders.items():
            df = self._extract_folder(folder_cfg["remote"], folder_cfg.get("format", "json"))
            results[table_name] = df
        return results

    def close(self):
        if self._sftp:
            self._sftp.close()
        if self._transport:
            self._transport.close()
        self._sftp = None
        self._transport = None
