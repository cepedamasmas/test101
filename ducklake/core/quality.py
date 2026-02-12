"""Data quality checks."""

from typing import Any, Dict, List

import duckdb
from loguru import logger


class QualityChecker:
    """Ejecuta validaciones de calidad de datos sobre tablas DuckDB."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def run_checks(
        self, table_name: str, checks: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Ejecutar lista de quality checks sobre una tabla registrada en DuckDB.

        Args:
            table_name: Nombre de la tabla/view en DuckDB.
            checks: Lista de checks (type, columns, etc).

        Returns:
            Lista de resultados [{type, passed, details}].
        """
        results: List[Dict[str, Any]] = []
        for check in checks:
            check_type = check["type"]
            handler = getattr(self, f"_check_{check_type}", None)
            if handler is None:
                logger.warning(f"Quality check desconocido: {check_type}")
                results.append({"type": check_type, "passed": False, "details": "Unknown check type"})
                continue
            result = handler(table_name, check)
            results.append(result)
            status = "PASS" if result["passed"] else "FAIL"
            logger.info(f"  Quality [{status}] {check_type}: {result['details']}")
        return results

    def _check_not_null(self, table: str, check: Dict[str, Any]) -> Dict[str, Any]:
        """Verificar que columnas no tengan nulos."""
        columns = check.get("columns", [])
        failures = []
        for col in columns:
            count = self.conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
            ).fetchone()[0]
            if count > 0:
                failures.append(f"{col}({count} nulls)")
        passed = len(failures) == 0
        details = "OK" if passed else f"Nulls found: {', '.join(failures)}"
        return {"type": "not_null", "passed": passed, "details": details}

    def _check_unique(self, table: str, check: Dict[str, Any]) -> Dict[str, Any]:
        """Verificar unicidad de columnas."""
        columns = check.get("columns", [])
        cols_str = ", ".join(columns)
        result = self.conn.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT ({cols_str})) AS duplicates
            FROM {table}
        """).fetchone()
        dup_count = result[0]
        passed = dup_count == 0
        details = "OK" if passed else f"{dup_count} duplicates on ({cols_str})"
        return {"type": "unique", "passed": passed, "details": details}

    def _check_valid_values(self, table: str, check: Dict[str, Any]) -> Dict[str, Any]:
        """Verificar que una columna solo tenga valores válidos."""
        column = check["column"]
        valid = check["values"]
        placeholders = ", ".join([f"'{v}'" for v in valid])
        count = self.conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {column} NOT IN ({placeholders})"
        ).fetchone()[0]
        passed = count == 0
        details = "OK" if passed else f"{count} invalid values in {column}"
        return {"type": "valid_values", "passed": passed, "details": details}

    def _check_range(self, table: str, check: Dict[str, Any]) -> Dict[str, Any]:
        """Verificar que valores estén dentro de un rango."""
        column = check["column"]
        min_val = check.get("min_value")
        max_val = check.get("max_value")
        conditions = []
        if min_val is not None:
            conditions.append(f"{column} < {min_val}")
        if max_val is not None:
            conditions.append(f"{column} > {max_val}")
        where = " OR ".join(conditions) if conditions else "FALSE"
        count = self.conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {where}"
        ).fetchone()[0]
        passed = count == 0
        details = "OK" if passed else f"{count} out-of-range values in {column}"
        return {"type": "range", "passed": passed, "details": details}
