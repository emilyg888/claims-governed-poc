"""
Generic SQL runner for executing Snowflake DDL files.

Usage:
    python -m scripts.run_sql path/to/file.sql
    python -m scripts.run_sql path/to/folder/
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

from pipeline.common.snowflake_client import SnowflakeClient

def split_sql_statements(sql: str) -> list[str]:
    """
    Split SQL script into executable statements.
    Keeps $$ procedure blocks intact.
    """
    statements = []
    buffer = []
    in_dollar_block = False

    for line in sql.splitlines():
        stripped = line.strip()

        # Detect start or end of $$ block
        if "$$" in stripped:
            in_dollar_block = not in_dollar_block

        buffer.append(line)

        # Only split on semicolon if not inside $$ block
        if not in_dollar_block and stripped.endswith(";"):
            statements.append("\n".join(buffer).strip())
            buffer = []

    # Catch any trailing content
    if buffer:
        statements.append("\n".join(buffer).strip())

    return statements

def run_sql_file(path: Path) -> None:
    """Execute one SQL file statement-by-statement in a single client context."""
    print(f"Running: {path}")

    sql = path.read_text()
    statements = split_sql_statements(sql)

    with SnowflakeClient() as client:
        for stmt in statements:
            if stmt:
                print(f"Executing:\n{stmt}\n")
                client.execute(stmt)

    print(f"Completed: {path}\n")


def main() -> None:
    """CLI entrypoint for running one SQL file or all SQL files in a folder."""
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.run_sql <file_or_folder>")
        sys.exit(1)

    target = Path(sys.argv[1])

    if target.is_file():
        run_sql_file(target)

    elif target.is_dir():
        # Folder mode is useful for running a schema layer in order.
        sql_files = sorted(target.glob("*.sql"))
        for file in sql_files:
            run_sql_file(file)

    else:
        print(f"Invalid path: {target}")
        sys.exit(1)


if __name__ == "__main__":
    main()
