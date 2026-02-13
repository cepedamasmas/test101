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

    def __init__(self, config: dict, files: dict):
        self.config = config
        self.files = files
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

    def extract(self) -> dict[str, pd.DataFrame]:
        self._connect()
        results = {}
        for table_name, file_cfg in self.files.items():
            opts = file_cfg.get("opts", {})
            df = self._download_and_read(file_cfg["remote"], file_cfg["format"], opts)
            results[table_name] = df
        return results

    def close(self):
        if self._sftp:
            self._sftp.close()
        if self._transport:
            self._transport.close()
        self._sftp = None
        self._transport = None
