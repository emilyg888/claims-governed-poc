"""CSV schema contract validation before Snowflake load."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline.common.utils import load_json

try:
    from jsonschema import Draft202012Validator
except ModuleNotFoundError:  # pragma: no cover - fallback for offline envs
    Draft202012Validator = None


@dataclass
class ValidationResult:
    """Result payload for schema validation checks."""

    valid: bool
    errors: list[str]
    row_count: int


def validate_csv_against_schema(
    csv_path: str | Path,
    schema_path: str | Path,
) -> ValidationResult:
    """Validate headers and row-level JSON schema compatibility."""
    schema = load_json(schema_path)
    validator = Draft202012Validator(schema) if Draft202012Validator else None

    required = set(schema.get("required", []))
    errors: list[str] = []
    rows = 0

    with Path(csv_path).open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            return ValidationResult(
                valid=False,
                errors=["Missing CSV header"],
                row_count=0,
            )

        present = set(reader.fieldnames)
        missing = sorted(required - present)
        if missing:
            errors.append(f"Missing required columns: {', '.join(missing)}")

        for index, row in enumerate(reader, start=2):
            rows += 1
            # CSV values arrive as strings, so normalize to expected types first.
            normalized = _normalize_row(row)
            if validator is not None:
                row_errors = sorted(
                    validator.iter_errors(normalized),
                    key=lambda err: err.path,
                )
                for err in row_errors:
                    errors.append(f"line {index}: {err.message}")
            else:
                # Offline fallback for environments missing jsonschema package.
                _validate_required_values(index, normalized, required, errors)

    return ValidationResult(valid=len(errors) == 0, errors=errors, row_count=rows)


def _normalize_row(row: dict[str, str]) -> dict[str, Any]:
    """Convert CSV strings into typed values expected by schema checks."""
    normalized: dict[str, Any] = dict(row)
    numeric_columns = (
        "amount",
        "claim_amount_incurred",
        "paid_amount_to_date",
        "reserve_amount",
    )
    for numeric_col in numeric_columns:
        value = normalized.get(numeric_col)
        if value in ("", None):
            normalized[numeric_col] = None
            continue
        try:
            normalized[numeric_col] = float(value)
        except (TypeError, ValueError):
            normalized[numeric_col] = value
    return normalized


def _validate_required_values(
    row_index: int,
    normalized: dict[str, Any],
    required: set[str],
    errors: list[str],
) -> None:
    """Fallback validation when jsonschema package is unavailable."""
    for column in sorted(required):
        value = normalized.get(column)
        if value in (None, ""):
            errors.append(f"line {row_index}: '{column}' is required")
