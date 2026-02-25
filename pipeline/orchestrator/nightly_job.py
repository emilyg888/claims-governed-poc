"""Nightly orchestration entrypoint for governed claims ingestion."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pipeline.common.snowflake_client import get_connection
from pipeline.common.utils import parse_batch_date
from pipeline.ingest.load_to_snowflake import (
    copy_file_to_raw,
    discover_files,
)
from pipeline.controls.run_controls import run_controls
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

        snapshot_loaded = copy_file_to_raw(
            conn,
            files["snapshot"],
            args.stage,
            "RAW.CLAIMS_SNAPSHOT_NIGHTLY",
            args.file_format,
            "snapshot",
        )
        events_loaded = copy_file_to_raw(
            conn,
            files["events"],
            args.stage,
            "RAW.CLAIMS_EVENTS_NIGHTLY",
            args.file_format,
            "events",
        )

        # Execute metadata-driven controls C1..C7 and block promotion on failures.
        summary = run_controls(
            conn,
            run_id,
            batch_date,
            files=files,
            loaded_counts={"snapshot": snapshot_loaded, "events": events_loaded},
        )
        if summary.blocking_failures > 0:
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
