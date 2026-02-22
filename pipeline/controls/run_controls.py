"""Run declarative controls and return summarized control outcomes."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from pipeline.common.raw_columns import events_expressions, snapshot_expressions
from pipeline.common.snowflake_client import execute_scalar


@dataclass
class ControlResult:
    """Control execution result."""

    control_id: str
    severity: str
    fail_count: int
    status: str


def load_controls(path: str = "rules/controls.yaml") -> dict[str, Any]:
    """Load controls configuration YAML."""
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError("rules/controls.yaml must contain a mapping")
    return payload


def run_sql_controls(conn, run_id: str, batch_date: str) -> list[ControlResult]:
    """Execute SQL controls from controls.yaml for a specific batch date."""
    config = load_controls()
    # Map logical field names to real SQL expressions for whichever RAW schema
    # is present (named columns or legacy COL_* columns).
    snapshot_cols = snapshot_expressions(conn)
    event_cols = events_expressions(conn)
    # Replace selected YAML queries with dynamic SQL so controls remain stable
    # across schema variants without manual YAML edits.
    query_overrides = {
        "C2_DQ_NON_NEGATIVE": f"""
          SELECT COUNT(*) AS fail_count
          FROM RAW.CLAIMS_SNAPSHOT_NIGHTLY
          WHERE {snapshot_cols["batch_date"]} = %(batch_date)s::DATE
            AND (
              {snapshot_cols["claim_amount_incurred"]} < 0
              OR {snapshot_cols["paid_amount_to_date"]} < 0
              OR {snapshot_cols["reserve_amount"]} < 0
            )
        """,
        "C4_CLASSIFICATION_DOMAIN": f"""
          SELECT COUNT(*) AS fail_count
          FROM RAW.CLAIMS_SNAPSHOT_NIGHTLY
          WHERE {snapshot_cols["batch_date"]} = %(batch_date)s::DATE
            AND {snapshot_cols["pii_class"]} NOT IN ('NONE', 'LOW', 'MEDIUM', 'HIGH')
        """,
        "C5_EVENT_DOMAIN": f"""
          SELECT COUNT(*) AS fail_count
          FROM RAW.CLAIMS_EVENTS_NIGHTLY
          WHERE {event_cols["batch_date"]} = %(batch_date)s::DATE
            AND {event_cols["event_type"]} NOT IN (
              'CREATED', 'UPDATED', 'STATUS_CHANGE', 'PAYMENT', 'NOTE'
            )
        """,
    }
    results: list[ControlResult] = []

    for control in config.get("controls", []):
        if control.get("type") != "sql":
            continue

        query = query_overrides.get(control["id"], control["query"])
        threshold = float(control.get("threshold", 0))
        # fail_count is the number of rows violating the rule.
        fail_count = int(
            execute_scalar(conn, query, {"batch_date": batch_date}) or 0
        )
        # Control passes only when failure count is at or below threshold.
        status = "PASS" if fail_count <= threshold else "FAIL"
        result = ControlResult(
            control_id=control["id"],
            severity=control["severity"],
            fail_count=fail_count,
            status=status,
        )
        results.append(result)

        insert_sql = """
            INSERT INTO CTRL.CONTROL_RESULT (
              run_id,
              control_id,
              control_name,
              status,
              fail_count,
              severity,
              executed_ts
            )
            SELECT
              %(run_id)s,
              %(control_id)s,
              %(control_name)s,
              %(status)s,
              %(fail_count)s,
              %(severity)s,
              CURRENT_TIMESTAMP()
        """
        with conn.cursor() as cur:
            cur.execute(
                insert_sql,
                {
                    "run_id": run_id,
                    "control_id": control["id"],
                    "control_name": control.get("description", control["id"]),
                    "status": status,
                    "fail_count": fail_count,
                    "severity": control["severity"],
                },
            )
    return results


def promotion_gate(results: list[ControlResult]) -> bool:
    """Return True only when no BLOCK control has failed."""
    return not any(
        result.severity == "BLOCK" and result.status == "FAIL"
        for result in results
    )
