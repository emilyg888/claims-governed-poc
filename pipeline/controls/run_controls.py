"""Compatibility facade for metadata-driven control execution."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pipeline.controls.engine import ControlEngine
from pipeline.controls.handlers import GateHandler, PrecheckHandler, SqlHandler
from pipeline.controls.models import (
    ControlContext,
    ControlDefinition,
    ControlResult,
    ControlsSummary,
)
from pipeline.controls.registry import ControlRegistry
from pipeline.controls.repository import ControlRepository


def load_controls(path: str = "rules/controls.yaml") -> dict[str, Any]:
    """Return raw register payload for compatibility with existing tests."""
    return ControlRegistry().load_raw(path)


def load_control_register(register_path: str = "rules/controls.yaml") -> list[ControlDefinition]:
    """Return typed control definitions from register YAML."""
    return ControlRegistry().load(register_path)


def run_controls(
    conn: Any,
    run_id: str,
    batch_date: str,
    *,
    register_path: str = "rules/controls.yaml",
    sql_dir: str = "pipeline/controls/sql",
    files: dict[str, Path] | None = None,
    loaded_counts: dict[str, int] | None = None,
    prev_batch_date: str | None = None,
) -> ControlsSummary:
    """Execute controls from register via ControlEngine and return summary."""
    batch_date_value = datetime.strptime(batch_date, "%Y-%m-%d").date()
    prev_batch_date_value = (
        datetime.strptime(prev_batch_date, "%Y-%m-%d").date()
        if prev_batch_date
        else batch_date_value - timedelta(days=1)
    )
    context = ControlContext(
        run_id=run_id,
        batch_date=batch_date_value,
        files=files or {},
        loaded_counts=loaded_counts or {},
        connection=conn,
        prev_batch_date=prev_batch_date_value,
    )
    engine = ControlEngine(
        repository=ControlRepository(conn),
        registry=ControlRegistry(register_path),
        precheck_handler=PrecheckHandler(),
        sql_handler=SqlHandler(sql_dir=sql_dir),
        gate_handler=GateHandler(),
    )
    return engine.run(context, register_path=register_path)


def promotion_gate(results: list[ControlResult]) -> bool:
    """Return True when no blocking control has failed or errored."""
    return not any(
        result.blocking and result.status in {"FAIL", "ERROR"}
        for result in results
    )
