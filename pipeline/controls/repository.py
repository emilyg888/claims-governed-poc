"""Persistence layer for control results."""

from __future__ import annotations

from typing import Any

from pipeline.controls.models import ControlResult


class ControlRepository:
    """Writes control outcomes to CTRL.CONTROL_RESULT."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def persist(self, result: ControlResult) -> None:
        available_columns = self._control_result_columns()
        if not available_columns:
            available_columns = {
                "RUN_ID",
                "CONTROL_ID",
                "CONTROL_NAME",
                "STATUS",
                "TOTAL_COUNT",
                "FAIL_COUNT",
                "SEVERITY",
                "EXECUTED_TS",
            }

        value_by_column = {
            "RUN_ID": result.run_id,
            "BATCH_DATE": result.batch_date.isoformat(),
            "CONTROL_ID": result.control_id,
            "CONTROL_NAME": result.control_id,
            "STATUS": result.status,
            "TOTAL_COUNT": result.total_count,
            "FAIL_COUNT": result.fail_count,
            "VARIANCE": result.variance,
            "SEVERITY": result.severity,
            "BLOCKING_FLAG": result.blocking,
            "DETAILS": result.details,
            "EXECUTED_SQL_HASH": result.executed_sql_hash,
        }

        insert_columns: list[str] = []
        select_values: list[str] = []
        params: dict[str, Any] = {}
        for column, value in value_by_column.items():
            if column in available_columns:
                key = column.lower()
                insert_columns.append(column)
                select_values.append(f"%({key})s")
                params[key] = value

        if "EXECUTED_AT" in available_columns:
            insert_columns.append("EXECUTED_AT")
            select_values.append("%(executed_at)s")
            params["executed_at"] = result.executed_at
        elif "EXECUTED_TS" in available_columns:
            insert_columns.append("EXECUTED_TS")
            select_values.append("CURRENT_TIMESTAMP()")

        if not insert_columns:
            return

        sql = f"""
          INSERT INTO CTRL.CONTROL_RESULT ({", ".join(insert_columns)})
          SELECT {", ".join(select_values)}
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, params)

    def save(self, result: ControlResult) -> None:
        """Alias matching the repository API shape in the design spec."""
        self.persist(result)

    def _control_result_columns(self) -> set[str]:
        sql = """
          SELECT UPPER(column_name)
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE table_schema = 'CTRL'
            AND table_name = 'CONTROL_RESULT'
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall() or []
        except Exception:
            return set()
        return {str(row[0]).upper() for row in rows}
