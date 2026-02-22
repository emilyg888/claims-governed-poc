"""Persist control outcomes to Snowflake and local artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline.common.snowflake_client import SnowflakeClient


@dataclass
class ControlEvaluation:
    """Single control execution result."""

    code: str
    name: str
    severity: str
    metric_value: float
    threshold_value: float
    passed: bool
    details: dict[str, Any]


def write_control_result(
    client: SnowflakeClient,
    run_id: str,
    evaluation: ControlEvaluation,
) -> None:
    """Insert control result row into CTRL.CONTROL_RESULT."""
    sql = """
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
        %(code)s,
        %(name)s,
        %(status)s,
        %(fail_count)s,
        %(severity)s,
        CURRENT_TIMESTAMP()
    """
    fail_count = 0 if evaluation.passed else int(evaluation.metric_value)
    client.execute(
        sql,
        {
            "run_id": run_id,
            "code": evaluation.code,
            "name": evaluation.name,
            "status": "PASS" if evaluation.passed else "FAIL",
            "fail_count": fail_count,
            "severity": evaluation.severity,
        },
    )


def write_exception_evidence(
    client: SnowflakeClient,
    run_id: str,
    control_code: str,
    exception_count: int,
    payload: dict[str, Any],
) -> None:
    """Insert exception rows into CTRL.EXCEPTIONS."""
    sql = """
      INSERT INTO CTRL.EXCEPTIONS (
        run_id,
        control_id,
        claim_id,
        error_message,
        severity,
        recorded_ts
      )
      SELECT
        %(run_id)s,
        %(control_code)s,
        %(claim_id)s,
        %(error_message)s,
        %(severity)s,
        CURRENT_TIMESTAMP()
    """
    details = json.dumps(payload, sort_keys=True)
    client.execute(
        sql,
        {
            "run_id": run_id,
            "control_code": control_code,
            "claim_id": f"batch_exception_{exception_count}",
            "error_message": details,
            "severity": "BLOCK",
        },
    )


def write_local_evidence(out_dir: str | Path, run_id: str, payload: dict[str, Any]) -> Path:
    """Write local JSON evidence artifact for offline audit."""
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    file_path = out_path / f"{run_id}.json"
    file_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return file_path
