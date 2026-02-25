"""Precheck control handlers."""

from __future__ import annotations

from typing import Any

from pipeline.common.snowflake_client import execute_scalar
from pipeline.common.utils import csv_row_count
from pipeline.controls.models import ControlContext, ControlDefinition, ControlResult
from pipeline.ingest.load_to_snowflake import validate_headers
from pipeline.ingest.schema_validate import validate_csv_against_schema

SNAPSHOT_REQUIRED_HEADERS = [
    "batch_date",
    "claim_id",
    "policy_id",
    "customer_id",
    "claim_amount_incurred",
    "paid_amount_to_date",
    "reserve_amount",
    "loss_date",
    "report_date",
    "claim_status",
    "pii_class",
]

EVENTS_REQUIRED_HEADERS = [
    "batch_date",
    "claim_id",
    "event_ts",
    "event_type",
    "old_status",
    "new_status",
    "amount_delta",
    "currency",
    "source_system",
    "note",
]


class PrecheckHandler:
    """Executes precheck controls."""

    def handle(self, control: ControlDefinition, context: ControlContext) -> ControlResult:
        if control.control_id == "C1_SCHEMA":
            return self._c1_schema(control, context)
        if control.control_id == "C3_RECON_ROWCOUNT":
            return self._c3_recon_rowcount(control, context)
        if control.control_id == "C6_RUN_AUDIT":
            return self._c6_run_audit(control, context)
        raise ValueError(f"No precheck handler for control {control.control_id}")

    def execute(self, ctx: ControlContext, control: ControlDefinition) -> ControlResult:
        """Alias matching the strategy signature in the design spec."""
        return self.handle(control, ctx)

    def _c1_schema(self, control: ControlDefinition, context: ControlContext) -> ControlResult:
        params = control.params or {}
        errors: list[str] = []
        failed_checks = 0
        snapshot_file = context.files.get("snapshot")
        events_file = context.files.get("events")
        if snapshot_file is None:
            errors.append("Missing snapshot file reference")
            failed_checks += 1
        if events_file is None:
            errors.append("Missing events file reference")
            failed_checks += 1

        snapshot_headers = params.get("snapshot_required_headers", SNAPSHOT_REQUIRED_HEADERS)
        events_headers = params.get("events_required_headers", EVENTS_REQUIRED_HEADERS)
        snapshot_schema = str(params.get("snapshot_schema", "schemas/claims_snapshot_schema.json"))
        events_schema = str(params.get("events_schema", "schemas/claims_events_schema.json"))

        if snapshot_file is not None:
            before = len(errors)
            try:
                validate_headers(snapshot_file, list(snapshot_headers))
            except ValueError as exc:
                errors.append(str(exc))
            snapshot_validation = validate_csv_against_schema(snapshot_file, snapshot_schema)
            if not snapshot_validation.valid:
                errors.extend(snapshot_validation.errors[:5])
            if len(errors) > before:
                failed_checks += 1

        if events_file is not None:
            before = len(errors)
            try:
                validate_headers(events_file, list(events_headers))
            except ValueError as exc:
                errors.append(str(exc))
            events_validation = validate_csv_against_schema(events_file, events_schema)
            if not events_validation.valid:
                errors.extend(events_validation.errors[:5])
            if len(errors) > before:
                failed_checks += 1

        return ControlResult(
            run_id=context.run_id,
            batch_date=context.batch_date,
            control_id=control.control_id,
            status="PASS" if not errors else "FAIL",
            blocking=control.blocking,
            severity=control.severity,
            type="precheck",
            total_count=2,
            fail_count=failed_checks,
            details="; ".join(errors[:5]) if errors else "Headers and schema validation passed",
        )

    def _c3_recon_rowcount(self, control: ControlDefinition, context: ControlContext) -> ControlResult:
        mismatches: list[str] = []
        total_variance = 0.0
        for dataset in ("snapshot", "events"):
            file_path = context.files.get(dataset)
            if file_path is None:
                mismatches.append(f"{dataset}: missing file reference")
                continue
            expected = csv_row_count(file_path)
            actual = int(context.loaded_counts.get(dataset, -1))
            diff = actual - expected
            total_variance += abs(diff)
            if diff != 0:
                mismatches.append(f"{dataset}: expected {expected}, loaded {actual}")

        return ControlResult(
            run_id=context.run_id,
            batch_date=context.batch_date,
            control_id=control.control_id,
            status="PASS" if not mismatches else "FAIL",
            blocking=control.blocking,
            severity=control.severity,
            type="precheck",
            total_count=2,
            fail_count=len(mismatches),
            variance=total_variance,
            details="; ".join(mismatches) if mismatches else "File/RAW rowcounts reconciled",
        )

    def _c6_run_audit(self, control: ControlDefinition, context: ControlContext) -> ControlResult:
        params = control.params or {}
        table = str(params.get("table", "CTRL.RUN_AUDIT"))
        min_rows = int(params.get("min_rows", 1))
        sql = f"SELECT COUNT(*) FROM {table} WHERE run_id = %(run_id)s"
        count = int(execute_scalar(context.connection, sql, {"run_id": context.run_id}) or 0)
        status = "PASS" if count >= min_rows else "FAIL"
        return ControlResult(
            run_id=context.run_id,
            batch_date=context.batch_date,
            control_id=control.control_id,
            status=status,
            blocking=control.blocking,
            severity=control.severity,
            type="precheck",
            total_count=count,
            fail_count=0 if status == "PASS" else 1,
            details=f"RUN_AUDIT rows for run_id={context.run_id}: {count}",
        )


class PrecheckRowcountHandler:
    """Dedicated handler for C3 rowcount reconciliation."""

    def execute(self, ctx: ControlContext, control: ControlDefinition) -> ControlResult:
        return PrecheckHandler()._c3_recon_rowcount(control, ctx)
