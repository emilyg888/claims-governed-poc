"""Snowflake data access for the controls dashboard."""

from __future__ import annotations

from typing import Any

import pandas as pd
import snowflake.connector

from pipeline.common.snowflake_client import connection_params
from pipeline.controls.registry import ControlRegistry


def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Create a short-lived Snowflake connection for dashboard queries."""
    return snowflake.connector.connect(autocommit=True, **connection_params())


def load_runs() -> pd.DataFrame:
    """Load all runs for run selector and summary cards."""
    sql = """
      SELECT
        run_id,
        batch_date,
        status,
        record_count,
        start_ts,
        end_ts
      FROM CTRL.RUN_AUDIT
      ORDER BY start_ts DESC
    """
    return _query_dataframe(sql)


def load_control_results(run_id: str | None = None, last_n: int | None = None) -> pd.DataFrame:
    """Load control evidence rows for one run or latest N batch dates."""
    if run_id is None and last_n is None:
        raise ValueError("Provide either run_id or last_n")
    columns = _control_result_columns()
    select_sql = _build_control_result_select(columns)

    if run_id is not None:
        sql = f"""
          SELECT
            {select_sql}
          FROM CTRL.CONTROL_RESULT cr
          LEFT JOIN CTRL.RUN_AUDIT ra
            ON ra.run_id = cr.run_id
          WHERE cr.run_id = %(run_id)s
          ORDER BY cr.control_id
        """
        return _query_dataframe(sql, {"run_id": run_id})

    sql = f"""
      WITH latest_runs AS (
        SELECT
          run_id,
          batch_date,
          start_ts,
          ROW_NUMBER() OVER (
            PARTITION BY batch_date
            ORDER BY start_ts DESC
          ) AS rn
        FROM CTRL.RUN_AUDIT
      ),
      selected_runs AS (
        SELECT run_id, batch_date
        FROM latest_runs
        WHERE rn = 1
        ORDER BY batch_date DESC
        LIMIT %(last_n)s
      )
      SELECT
        {select_sql}
      FROM CTRL.CONTROL_RESULT cr
      JOIN selected_runs sr
        ON sr.run_id = cr.run_id
      LEFT JOIN CTRL.RUN_AUDIT ra
        ON ra.run_id = cr.run_id
      ORDER BY sr.batch_date, cr.control_id
    """
    return _query_dataframe(sql, {"last_n": int(last_n or 0)})


def load_control_metadata(register_path: str = "rules/controls.yaml") -> pd.DataFrame:
    """Load control metadata from YAML register as DataFrame."""
    definitions = ControlRegistry(register_path).load()
    records = [
        {
            "CONTROL_ID": item.control_id,
            "CONTROL_TYPE": item.type.upper(),
            "DESCRIPTION": item.description,
            "REGISTER_SEVERITY": item.severity,
            "REGISTER_BLOCKING": bool(item.blocking),
            "ENABLED": bool(item.enabled),
            "SQL_PATH": item.sql_path,
        }
        for item in definitions
    ]
    return pd.DataFrame(records)


def _query_dataframe(sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            rows = cur.fetchall() or []
            cols = [str(col[0]).upper() for col in (cur.description or [])]
    return pd.DataFrame(rows, columns=cols)


def _control_result_columns() -> set[str]:
    sql = """
      SELECT UPPER(column_name) AS COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE table_schema = 'CTRL'
        AND table_name = 'CONTROL_RESULT'
    """
    frame = _query_dataframe(sql)
    if frame.empty:
        return set()
    # Defensive: Snowflake/Pandas can expose expression names differently if aliasing changes.
    if "COLUMN_NAME" in frame.columns:
        column_series = frame["COLUMN_NAME"]
    else:
        column_series = frame.iloc[:, 0]
    return set(column_series.astype(str).tolist())


def _build_control_result_select(columns: set[str]) -> str:
    fields = [
        "cr.run_id AS RUN_ID",
        "COALESCE(cr.batch_date, ra.batch_date) AS BATCH_DATE"
        if "BATCH_DATE" in columns
        else "ra.batch_date AS BATCH_DATE",
        "cr.control_id AS CONTROL_ID",
        "cr.status AS STATUS",
        (
            "cr.total_count AS TOTAL_COUNT"
            if "TOTAL_COUNT" in columns
            else "cr.control_value AS TOTAL_COUNT"
            if "CONTROL_VALUE" in columns
            else "NULL AS TOTAL_COUNT"
        ),
        "cr.fail_count AS FAIL_COUNT" if "FAIL_COUNT" in columns else "NULL AS FAIL_COUNT",
        "cr.severity AS SEVERITY" if "SEVERITY" in columns else "NULL AS SEVERITY",
        "cr.control_name AS CONTROL_NAME" if "CONTROL_NAME" in columns else "NULL AS CONTROL_NAME",
        "cr.variance AS VARIANCE" if "VARIANCE" in columns else "NULL AS VARIANCE",
        "cr.blocking_flag AS BLOCKING_FLAG" if "BLOCKING_FLAG" in columns else "NULL AS BLOCKING_FLAG",
        "cr.details AS DETAILS" if "DETAILS" in columns else "NULL AS DETAILS",
        (
            "cr.executed_sql_hash AS EXECUTED_SQL_HASH"
            if "EXECUTED_SQL_HASH" in columns
            else "NULL AS EXECUTED_SQL_HASH"
        ),
        (
            "cr.executed_at AS EXECUTED_AT"
            if "EXECUTED_AT" in columns
            else "cr.executed_ts AS EXECUTED_AT"
            if "EXECUTED_TS" in columns
            else "NULL AS EXECUTED_AT"
        ),
    ]
    return ",\n        ".join(fields)
