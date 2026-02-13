"""Conector MySQL para ingesta de tablas."""

import pandas as pd
import pymysql

from .base import BaseConnector


class MySQLConnector(BaseConnector):
    """Extrae tablas completas desde MySQL."""

    source_name = "mysql"

    def __init__(self, config: dict, tables: list[str]):
        self.config = config
        self.tables = tables
        self._conn = None

    def _connect(self):
        if not self._conn:
            self._conn = pymysql.connect(
                **self.config, cursorclass=pymysql.cursors.DictCursor
            )

    def extract(self) -> dict[str, pd.DataFrame]:
        self._connect()
        results = {}
        for table in self.tables:
            with self._conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table}")
                rows = cursor.fetchall()
            results[table] = pd.DataFrame(rows)
        return results

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
