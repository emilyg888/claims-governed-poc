"""Typed models for metadata-driven controls."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal


ControlType = Literal["precheck", "sql", "gate"]
ControlStatus = Literal["PASS", "FAIL", "ERROR", "SKIP"]


@dataclass(frozen=True)
class ControlDefinition:
    """One control row from the control register."""

    control_id: str
    type: ControlType
    enabled: bool
    blocking: bool
    severity: str
    description: str
    sql_path: str | None
    params: dict[str, Any]
    threshold: float = 0.0
    query: str | None = None


@dataclass(frozen=True)
class ControlContext:
    """Execution context shared by handlers."""

    run_id: str
    batch_date: date
    files: dict[str, Path]
    loaded_counts: dict[str, int]
    connection: Any
    prev_batch_date: date | None = None


@dataclass(frozen=True)
class ControlResult:
    """Normalized result contract for all control types."""

    run_id: str
    batch_date: date
    control_id: str
    status: ControlStatus
    blocking: bool
    severity: str
    type: ControlType
    total_count: int | None = None
    fail_count: int | None = None
    variance: float | None = None
    details: str | None = None
    executed_sql_hash: str | None = None
    executed_at: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class ControlsSummary:
    """Aggregated control-run summary used by orchestrator gating."""

    run_id: str
    batch_date: date
    total: int
    passed: int
    failed: int
    skipped: int
    errored: int
    blocking_failures: int
    results: list[ControlResult]

    @property
    def blocking_failed(self) -> int:
        """Backward-compatible alias for older callers."""
        return self.blocking_failures
