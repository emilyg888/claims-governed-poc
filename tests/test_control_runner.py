"""Tests for metadata-driven control engine behavior."""

from __future__ import annotations

from datetime import date

from pipeline.controls.engine import ControlEngine
from pipeline.controls.models import ControlContext, ControlDefinition, ControlResult


class _FakeRegistry:
    def __init__(self, definitions: list[ControlDefinition]) -> None:
        self._definitions = definitions

    def load(self, register_path: str = "rules/controls.yaml") -> list[ControlDefinition]:
        return self._definitions


class _FakeRepo:
    def __init__(self) -> None:
        self.persisted: list[ControlResult] = []

    def persist(self, result: ControlResult) -> None:
        self.persisted.append(result)


class _PassPrecheck:
    def handle(self, control: ControlDefinition, context: ControlContext) -> ControlResult:
        return ControlResult(
            run_id=context.run_id,
            batch_date=context.batch_date,
            control_id=control.control_id,
            status="PASS",
            blocking=control.blocking,
            severity=control.severity,
            type="precheck",
            fail_count=0,
        )


class _PassSql:
    def handle(self, control: ControlDefinition, context: ControlContext) -> ControlResult:
        return ControlResult(
            run_id=context.run_id,
            batch_date=context.batch_date,
            control_id=control.control_id,
            status="PASS",
            blocking=control.blocking,
            severity=control.severity,
            type="sql",
            fail_count=0,
        )


class _GateFromPrior:
    def handle(
        self,
        control: ControlDefinition,
        context: ControlContext,
        prior_results: list[ControlResult],
    ) -> ControlResult:
        blocking_failures = [
            item for item in prior_results if item.blocking and item.status in {"FAIL", "ERROR"}
        ]
        return ControlResult(
            run_id=context.run_id,
            batch_date=context.batch_date,
            control_id=control.control_id,
            status="FAIL" if blocking_failures else "PASS",
            blocking=control.blocking,
            severity=control.severity,
            type="gate",
            fail_count=len(blocking_failures),
        )


def _context() -> ControlContext:
    return ControlContext(
        run_id="run_1",
        batch_date=date(2026, 2, 21),
        files={},
        loaded_counts={},
        connection=object(),
    )


def test_engine_executes_precheck_sql_and_gate() -> None:
    definitions = [
        ControlDefinition(
            control_id="C1_SCHEMA",
            type="precheck",
            enabled=True,
            blocking=True,
            severity="BLOCK",
            description="schema",
            sql_path=None,
            params={},
        ),
        ControlDefinition(
            control_id="C2_DQ_NON_NEGATIVE",
            type="sql",
            enabled=True,
            blocking=True,
            severity="BLOCK",
            description="dq",
            sql_path="C2_DQ_NON_NEGATIVE.sql",
            params={},
        ),
        ControlDefinition(
            control_id="C7_PROMOTION_GATE",
            type="gate",
            enabled=True,
            blocking=True,
            severity="BLOCK",
            description="gate",
            sql_path=None,
            params={},
        ),
    ]
    repo = _FakeRepo()
    engine = ControlEngine(
        registry=_FakeRegistry(definitions),
        repository=repo,
        precheck_handler=_PassPrecheck(),
        sql_handler=_PassSql(),
        gate_handler=_GateFromPrior(),
    )

    summary = engine.run(_context())

    assert summary.total == 3
    assert summary.passed == 3
    assert summary.blocking_failures == 0
    assert [item.control_id for item in repo.persisted] == [
        "C1_SCHEMA",
        "C2_DQ_NON_NEGATIVE",
        "C7_PROMOTION_GATE",
    ]


def test_engine_respects_enabled_flag() -> None:
    definitions = [
        ControlDefinition(
            control_id="C2_DQ_NON_NEGATIVE",
            type="sql",
            enabled=False,
            blocking=True,
            severity="BLOCK",
            description="dq",
            sql_path="C2_DQ_NON_NEGATIVE.sql",
            params={},
        )
    ]
    repo = _FakeRepo()
    engine = ControlEngine(
        registry=_FakeRegistry(definitions),
        repository=repo,
        precheck_handler=_PassPrecheck(),
        sql_handler=_PassSql(),
        gate_handler=_GateFromPrior(),
    )

    summary = engine.run(_context())

    assert summary.total == 1
    assert summary.skipped == 1
    assert summary.results[0].status == "SKIP"
    assert len(repo.persisted) == 1


def test_engine_counts_blocking_failures() -> None:
    class _FailPrecheck(_PassPrecheck):
        def handle(self, control: ControlDefinition, context: ControlContext) -> ControlResult:
            return ControlResult(
                run_id=context.run_id,
                batch_date=context.batch_date,
                control_id=control.control_id,
                status="FAIL",
                blocking=control.blocking,
                severity=control.severity,
                type="precheck",
                fail_count=1,
            )

    definitions = [
        ControlDefinition(
            control_id="C1_SCHEMA",
            type="precheck",
            enabled=True,
            blocking=True,
            severity="BLOCK",
            description="schema",
            sql_path=None,
            params={},
        ),
        ControlDefinition(
            control_id="C7_PROMOTION_GATE",
            type="gate",
            enabled=True,
            blocking=True,
            severity="BLOCK",
            description="gate",
            sql_path=None,
            params={},
        ),
    ]
    repo = _FakeRepo()
    engine = ControlEngine(
        registry=_FakeRegistry(definitions),
        repository=repo,
        precheck_handler=_FailPrecheck(),
        sql_handler=_PassSql(),
        gate_handler=_GateFromPrior(),
    )

    summary = engine.run(_context())

    assert summary.blocking_failures == 2


def test_engine_runs_gate_after_non_gate_controls() -> None:
    """Gate must evaluate failures from controls even if listed earlier."""

    class _FailSql(_PassSql):
        def handle(self, control: ControlDefinition, context: ControlContext) -> ControlResult:
            return ControlResult(
                run_id=context.run_id,
                batch_date=context.batch_date,
                control_id=control.control_id,
                status="FAIL",
                blocking=control.blocking,
                severity=control.severity,
                type="sql",
                fail_count=1,
            )

    definitions = [
        ControlDefinition(
            control_id="C7_PROMOTION_GATE",
            type="gate",
            enabled=True,
            blocking=True,
            severity="BLOCK",
            description="gate",
            sql_path=None,
            params={},
        ),
        ControlDefinition(
            control_id="C8_DUPLICATE_CLAIM_ID",
            type="sql",
            enabled=True,
            blocking=True,
            severity="BLOCK",
            description="dupes",
            sql_path="C8_DUPLICATE_CLAIM_ID.sql",
            params={},
        ),
    ]
    repo = _FakeRepo()
    engine = ControlEngine(
        registry=_FakeRegistry(definitions),
        repository=repo,
        precheck_handler=_PassPrecheck(),
        sql_handler=_FailSql(),
        gate_handler=_GateFromPrior(),
    )

    summary = engine.run(_context())

    assert [item.control_id for item in repo.persisted] == [
        "C8_DUPLICATE_CLAIM_ID",
        "C7_PROMOTION_GATE",
    ]
    assert summary.results[-1].control_id == "C7_PROMOTION_GATE"
    assert summary.results[-1].status == "FAIL"
    assert summary.blocking_failures == 2
