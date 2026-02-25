"""SQL control handler."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

from pipeline.common.raw_columns import events_expressions, snapshot_expressions
from pipeline.controls.models import ControlContext, ControlDefinition, ControlResult


class SqlHandler:
    """Executes SQL controls with a standardized output contract."""

    def __init__(self, sql_dir: str = "pipeline/controls/sql") -> None:
        self._sql_dir = sql_dir

    def handle(self, control: ControlDefinition, context: ControlContext) -> ControlResult:
        sql_template = self._load_sql_text(control)
        sql_context = self._build_sql_context(context.connection)
        rendered_sql = self._render_sql(sql_template, sql_context)
        params: dict[str, Any] = {
            "run_id": context.run_id,
            "batch_date": context.batch_date.isoformat(),
            "prev_batch_date": context.prev_batch_date.isoformat()
            if context.prev_batch_date else None,
            "threshold": control.threshold,
        }
        for key, value in (control.params or {}).items():
            if isinstance(value, (str, int, float, bool)) or value is None:
                params[key] = value

        with context.connection.cursor() as cur:
            cur.execute(rendered_sql, params)
            row = cur.fetchone()
            description = cur.description or []

        if row is None:
            raise ValueError(f"Control {control.control_id} returned no rows")

        payload = {str(col[0]).lower(): row[idx] for idx, col in enumerate(description)}
        total_count_raw = payload.get("total_count")
        total_count = int(total_count_raw) if total_count_raw is not None else None
        fail_count = int(payload.get("fail_count") or payload.get("control_value") or 0)
        variance = float(payload["variance"]) if payload.get("variance") is not None else None
        status_raw = str(payload.get("status", "")).strip().upper()
        status = status_raw if status_raw in {"PASS", "FAIL", "ERROR", "SKIP"} else (
            "PASS" if fail_count <= control.threshold else "FAIL"
        )
        details = payload.get("details")
        return ControlResult(
            run_id=context.run_id,
            batch_date=context.batch_date,
            control_id=control.control_id,
            status=status,  # type: ignore[arg-type]
            blocking=control.blocking,
            severity=control.severity,
            type="sql",
            total_count=total_count,
            fail_count=fail_count,
            variance=variance,
            details=str(details) if details is not None else None,
            executed_sql_hash=hashlib.sha256(rendered_sql.encode("utf-8")).hexdigest(),
        )

    def execute(self, ctx: ControlContext, control: ControlDefinition) -> ControlResult:
        """Alias matching the strategy signature in the design spec."""
        return self.handle(control, ctx)

    def _load_sql_text(self, control: ControlDefinition) -> str:
        if control.sql_path:
            root = Path(self._sql_dir).resolve()
            file_path = (root / control.sql_path).resolve()
            if root not in file_path.parents and file_path != root:
                raise ValueError(f"Invalid sql_path outside sql_dir: {control.sql_path}")
            return file_path.read_text(encoding="utf-8")
        if control.query:
            return control.query
        raise ValueError(f"SQL control {control.control_id} missing sql_path/query")

    def _build_sql_context(self, conn: Any) -> dict[str, str]:
        return {
            **{f"snapshot_{k}": v for k, v in snapshot_expressions(conn).items()},
            **{f"events_{k}": v for k, v in events_expressions(conn).items()},
        }

    def _render_sql(self, template: str, sql_context: dict[str, str]) -> str:
        rendered = template
        for key, value in sql_context.items():
            rendered = rendered.replace(f"{{{{{key}}}}}", value)
        unresolved = re.findall(r"\{\{[^}]+\}\}", rendered)
        if unresolved:
            raise ValueError(
                f"Unresolved SQL template placeholders: {', '.join(sorted(set(unresolved)))}"
            )
        return rendered


class SqlControlHandler(SqlHandler):
    """Name-compatible class matching the proposed architecture."""
