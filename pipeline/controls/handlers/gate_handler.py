"""Gate control handler."""

from __future__ import annotations

from pipeline.controls.models import ControlContext, ControlDefinition, ControlResult


class GateHandler:
    """Computes promotion gate result from prior control outcomes."""

    def handle(
        self,
        control: ControlDefinition,
        context: ControlContext,
        prior_results: list[ControlResult],
    ) -> ControlResult:
        blocking_failures = [
            result.control_id
            for result in prior_results
            if result.blocking and result.status in {"FAIL", "ERROR"}
        ]
        blocking_evaluated = sum(1 for result in prior_results if result.blocking)
        status = "PASS" if not blocking_failures else "FAIL"
        details = (
            "No blocking failures"
            if not blocking_failures
            else "Blocking failures: " + ", ".join(blocking_failures)
        )
        return ControlResult(
            run_id=context.run_id,
            batch_date=context.batch_date,
            control_id=control.control_id,
            status=status,  # type: ignore[arg-type]
            blocking=control.blocking,
            severity=control.severity,
            type="gate",
            total_count=blocking_evaluated,
            fail_count=len(blocking_failures),
            details=details,
        )

    def execute(
        self,
        ctx: ControlContext,
        control: ControlDefinition,
        prior_results: list[ControlResult],
    ) -> ControlResult:
        """Alias matching the strategy signature in the design spec."""
        return self.handle(control, ctx, prior_results)
