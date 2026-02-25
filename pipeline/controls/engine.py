"""Control engine orchestrating register-driven handler dispatch."""

from __future__ import annotations

from pipeline.controls.handlers import GateHandler, PrecheckHandler, SqlHandler
from pipeline.controls.models import ControlContext, ControlResult, ControlsSummary
from pipeline.controls.registry import ControlRegistry
from pipeline.controls.repository import ControlRepository


class ControlEngine:
    """Executes enabled controls in register order and persists all results."""

    def __init__(
        self,
        repository: ControlRepository,
        registry: ControlRegistry | None = None,
        precheck_handler: PrecheckHandler | None = None,
        sql_handler: SqlHandler | None = None,
        gate_handler: GateHandler | None = None,
    ) -> None:
        self._registry = registry or ControlRegistry()
        self._repository = repository
        self._precheck_handler = precheck_handler or PrecheckHandler()
        self._sql_handler = sql_handler or SqlHandler()
        self._gate_handler = gate_handler or GateHandler()

    def run(
        self,
        context: ControlContext,
        *,
        controls: list | None = None,
        register_path: str | None = None,
    ) -> ControlsSummary:
        definitions = controls if controls is not None else self._registry.load(register_path)
        results: list[ControlResult] = []

        non_gate_controls = [control for control in definitions if control.type != "gate"]
        gate_controls = [control for control in definitions if control.type == "gate"]

        for control in non_gate_controls:
            if not control.enabled:
                result = ControlResult(
                    run_id=context.run_id,
                    batch_date=context.batch_date,
                    control_id=control.control_id,
                    status="SKIP",
                    blocking=control.blocking,
                    severity=control.severity,
                    type=control.type,
                    fail_count=0,
                    details="Control disabled in register",
                )
                self._repository.persist(result)
                results.append(result)
                continue

            try:
                if control.type == "precheck":
                    result = self._precheck_handler.handle(control, context)
                else:
                    result = self._sql_handler.handle(control, context)
            except Exception as exc:  # pragma: no cover - defensive runtime guard
                result = ControlResult(
                    run_id=context.run_id,
                    batch_date=context.batch_date,
                    control_id=control.control_id,
                    status="ERROR",
                    blocking=control.blocking,
                    severity=control.severity,
                    type=control.type,
                    fail_count=1,
                    details=str(exc),
                )

            self._repository.persist(result)
            results.append(result)

        for control in gate_controls:
            if not control.enabled:
                result = ControlResult(
                    run_id=context.run_id,
                    batch_date=context.batch_date,
                    control_id=control.control_id,
                    status="SKIP",
                    blocking=control.blocking,
                    severity=control.severity,
                    type=control.type,
                    fail_count=0,
                    details="Control disabled in register",
                )
                self._repository.persist(result)
                results.append(result)
                continue

            try:
                result = self._gate_handler.handle(control, context, results)
            except Exception as exc:  # pragma: no cover - defensive runtime guard
                result = ControlResult(
                    run_id=context.run_id,
                    batch_date=context.batch_date,
                    control_id=control.control_id,
                    status="ERROR",
                    blocking=control.blocking,
                    severity=control.severity,
                    type=control.type,
                    fail_count=1,
                    details=str(exc),
                )
            self._repository.persist(result)
            results.append(result)

        blocking_failures = sum(
            1 for result in results if result.blocking and result.status in {"FAIL", "ERROR"}
        )
        return ControlsSummary(
            run_id=context.run_id,
            batch_date=context.batch_date,
            total=len(results),
            passed=sum(1 for item in results if item.status == "PASS"),
            failed=sum(1 for item in results if item.status == "FAIL"),
            skipped=sum(1 for item in results if item.status == "SKIP"),
            errored=sum(1 for item in results if item.status == "ERROR"),
            blocking_failures=blocking_failures,
            results=results,
        )

    def run_results(
        self,
        context: ControlContext,
        controls: list,
    ) -> list[ControlResult]:
        """Compatibility helper that returns only result rows."""
        return self.run(context, controls=controls).results
