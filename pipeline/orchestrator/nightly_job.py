"""Nightly orchestration entrypoint for governed claims ingestion."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pipeline.common.snowflake_client import get_connection
from pipeline.common.utils import parse_batch_date
from pipeline.ingest.load_to_snowflake import (
    copy_file_to_raw,
    discover_files,
    validate_headers,
)
from pipeline.ingest.schema_validate import validate_csv_against_schema
from pipeline.controls.run_controls import promotion_gate, run_sql_controls
from pipeline.promote.promote_int_gold import promote_snapshot_to_int


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--stage", default="@RAW.CLAIMS_NIGHTLY_STAGE")
    parser.add_argument("--file-format", default="RAW.CSV_FF")
    return parser.parse_args()


def main() -> int:
    """Run the pipeline stages: ingest, control, and promotion."""
    args = parse_args()
    batch_date = parse_batch_date(args.batch_date)
    # Unique identifier ties all audit/control records for one execution.
    run_id = (
        f"run_{batch_date}_"
        f"{datetime.now(timezone.utc).strftime('%H%M%S')}"
    )

    # Locate both required nightly files for this batch date.
    files = discover_files(batch_date)

    # Fast fail before loading if required headers are missing.
    validate_headers(
        files["snapshot"],
        [
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
        ],
    )
    validate_headers(
        files["events"],
        [
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
        ],
    )
    snapshot_validation = validate_csv_against_schema(
        files["snapshot"],
        "schemas/claims_snapshot_schema.json",
    )
    if not snapshot_validation.valid:
        raise ValueError(
            "Snapshot schema validation failed: "
            + "; ".join(snapshot_validation.errors[:5])
        )

    events_validation = validate_csv_against_schema(
        files["events"],
        "schemas/claims_events_schema.json",
    )
    if not events_validation.valid:
        raise ValueError(
            "Events schema validation failed: "
            + "; ".join(events_validation.errors[:5])
        )

    with get_connection() as conn:
        # Mark run as started in audit table.
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO CTRL.RUN_AUDIT (
                  run_id,
                  dataset_name,
                  batch_date,
                  file_name,
                  start_ts,
                  status,
                  record_count
                )
                SELECT
                  %(run_id)s,
                  %(dataset_name)s,
                  %(batch_date)s::DATE,
                  %(file_name)s,
                  CURRENT_TIMESTAMP(),
                  'STARTED',
                  0
                """,
                {
                    "run_id": run_id,
                    "dataset_name": "claims_snapshot_events",
                    "batch_date": batch_date,
                    "file_name": f"{files['snapshot'].name},{files['events'].name}",
                },
            )

        copy_file_to_raw(
            conn,
            files["snapshot"],
            args.stage,
            "RAW.CLAIMS_SNAPSHOT_NIGHTLY",
            args.file_format,
            "snapshot",
        )
        copy_file_to_raw(
            conn,
            files["events"],
            args.stage,
            "RAW.CLAIMS_EVENTS_NIGHTLY",
            args.file_format,
            "events",
        )

        # Execute SQL controls and stop promotion on blocking failures.
        results = run_sql_controls(conn, run_id, batch_date)
        if not promotion_gate(results):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE CTRL.RUN_AUDIT
                    SET end_ts = CURRENT_TIMESTAMP(),
                        status = 'FAILED'
                    WHERE run_id = %(run_id)s
                    """,
                    {"run_id": run_id},
                )
            return 1

        # Promote clean snapshot data into INT layer.
        promoted = promote_snapshot_to_int(conn, batch_date)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO CTRL.PROMOTION_HISTORY (
                  run_id,
                  dataset_name,
                  status
                )
                SELECT
                  %(run_id)s,
                  %(dataset_name)s,
                  %(status)s
                """,
                {
                    "run_id": run_id,
                    "dataset_name": "CLAIMS_SNAPSHOT",
                    "status": "PROMOTED",
                },
            )
            cur.execute(
                """
                UPDATE CTRL.RUN_AUDIT
                SET end_ts = CURRENT_TIMESTAMP(),
                    status = 'PASSED',
                    record_count = %(record_count)s
                WHERE run_id = %(run_id)s
                """,
                {"record_count": promoted, "run_id": run_id},
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
