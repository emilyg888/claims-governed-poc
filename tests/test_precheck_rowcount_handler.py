"""Tests for rowcount reconciliation precheck handler."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from pipeline.controls.handlers import PrecheckRowcountHandler
from pipeline.controls.models import ControlContext, ControlDefinition


def _write_csv(path: Path, row_count: int) -> None:
    path.write_text(
        "col1,col2\n" + "\n".join(f"{idx},x" for idx in range(row_count)) + "\n",
        encoding="utf-8",
    )


def test_rowcount_control_pass(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "snapshot.csv"
    events_path = tmp_path / "events.csv"
    _write_csv(snapshot_path, 10)
    _write_csv(events_path, 5)

    control = ControlDefinition(
        control_id="C3_RECON_ROWCOUNT",
        type="precheck",
        enabled=True,
        blocking=True,
        severity="BLOCK",
        description="Rowcount reconciliation",
        sql_path=None,
        params={},
    )
    ctx = ControlContext(
        run_id="TEST",
        batch_date=date(2026, 1, 1),
        files={"snapshot": snapshot_path, "events": events_path},
        loaded_counts={"snapshot": 10, "events": 5},
        connection=None,
    )

    handler = PrecheckRowcountHandler()
    result = handler.execute(ctx, control)

    assert result.status == "PASS"
    assert result.variance == 0.0
