"""Load snapshot and events nightly files into Snowflake RAW tables."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Literal

from pipeline.common.snowflake_client import execute_scalar


def discover_files(
    batch_date: str,
    input_dir: str = "samples/nightly_drop",
) -> dict[str, Path]:
    """Return expected snapshot and events files for the given batch date."""
    # Convert YYYY-MM-DD -> YYYYMMDD for filename matching.
    stamp = batch_date.replace("-", "")
    root = Path(input_dir)
    snapshot = root / f"claims_snapshot_{stamp}.csv"
    events = root / f"claims_events_{stamp}.csv"
    missing = [str(path) for path in (snapshot, events) if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing input files: {', '.join(missing)}")
    return {"snapshot": snapshot, "events": events}


def validate_headers(csv_path: Path, required_headers: list[str]) -> None:
    """Validate CSV header includes required columns."""
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        headers = next(reader, None)
    if not headers:
        raise ValueError(f"Missing header in {csv_path.name}")
    missing = [name for name in required_headers if name not in headers]
    if missing:
        raise ValueError(
            f"{csv_path.name} missing required columns: {', '.join(missing)}"
        )


def copy_file_to_raw(
    conn,
    file_path: Path,
    stage_name: str,
    table_name: str,
    file_format: str,
    dataset_kind: Literal["snapshot", "events"],
) -> int:
    """PUT a file and COPY it into target RAW table.

    Note: Snowflake SQL bind variables cannot be used for object identifiers,
    so identifier arguments are controlled constants from pipeline config.
    """
    put_sql = (
        f"PUT file://{file_path.absolute()} {stage_name} "
        "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    )
    # 1) Upload local file into Snowflake internal stage.
    with conn.cursor() as cur:
        cur.execute(put_sql)

    copy_sql = _build_copy_sql(
        table_name=table_name,
        file_format=file_format,
        stage_name=stage_name,
        dataset_kind=dataset_kind,
    )
    # 2) Load just this file from stage into RAW table.
    with conn.cursor() as cur:
        cur.execute(copy_sql, {"pattern": f".*{file_path.name}.*"})

    count_sql = f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE src_filename ILIKE %(filename)s
    """
    loaded = execute_scalar(conn, count_sql, {"filename": f"%{file_path.name}%"})
    return int(loaded or 0)


def _build_copy_sql(
    table_name: str,
    file_format: str,
    stage_name: str,
    dataset_kind: Literal["snapshot", "events"],
) -> str:
    """Return COPY SQL with a column mapping aligned to target table layout."""
    if dataset_kind == "snapshot":
        # Snapshot files contain 32 business columns.
        projection = """
            t.$1,
            t.$2,
            t.$3,
            t.$4,
            t.$5,
            t.$6,
            t.$7,
            t.$8,
            t.$9,
            t.$10,
            t.$11,
            t.$12,
            t.$13,
            t.$14,
            t.$15,
            t.$16,
            t.$17,
            t.$18,
            t.$19,
            t.$20,
            t.$21,
            t.$22,
            t.$23,
            t.$24,
            t.$25,
            t.$26,
            t.$27,
            t.$28,
            t.$29,
            t.$30,
            t.$31,
            t.$32,
            METADATA$FILENAME,
            METADATA$FILE_ROW_NUMBER,
            CURRENT_TIMESTAMP()
        """
    else:
        # Events files contain 10 business columns.
        projection = """
            t.$1,
            t.$2,
            t.$3,
            t.$4,
            t.$5,
            t.$6,
            t.$7,
            t.$8,
            t.$9,
            t.$10,
            METADATA$FILENAME,
            METADATA$FILE_ROW_NUMBER,
            CURRENT_TIMESTAMP()
        """

    return f"""
        COPY INTO {table_name}
        FROM (
          SELECT
            {projection}
          FROM {stage_name} t
        )
        FILE_FORMAT = (FORMAT_NAME = '{file_format}')
        PATTERN = %(pattern)s
        ON_ERROR = 'ABORT_STATEMENT'
    """
