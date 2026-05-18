"""File-vs-RAW reconciliation checks for ingestion integrity."""

from __future__ import annotations

from pathlib import Path

from pipeline.common.snowflake_client import SnowflakeClient
from pipeline.common.utils import csv_row_count, sha256_for_file


def build_manifest(files: list[Path]) -> list[dict[str, str | int]]:
    """Build local manifest with row counts and checksums."""
    manifest: list[dict[str, str | int]] = []
    for path in files:
        manifest.append(
            {
                "filename": path.name,
                "row_count": csv_row_count(path),
                "sha256": sha256_for_file(path),
            }
        )
    return manifest


def reconcile_row_count(
    client: SnowflakeClient,
    target_table: str,
    batch_date: str,
    expected_rows: int,
) -> tuple[bool, int, int]:
    """Compare expected local rows to rows loaded in Snowflake RAW table."""
    sql = f"""
      SELECT COUNT(*)
      FROM {target_table}
      WHERE batch_date = %(batch_date)s
    """
    row = client.query_one(sql, {"batch_date": batch_date})
    actual_rows = int(row[0]) if row else 0
    return actual_rows == expected_rows, expected_rows, actual_rows
