"""Tests for schema/header validation helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.ingest.load_to_snowflake import validate_headers


REQUIRED = ["a", "b", "c"]


def test_validate_headers_success(tmp_path: Path) -> None:
    """Header validation should pass when all required columns exist."""
    path = tmp_path / "ok.csv"
    path.write_text("a,b,c,d\n1,2,3,4\n", encoding="utf-8")

    validate_headers(path, REQUIRED)


def test_validate_headers_missing_column(tmp_path: Path) -> None:
    """Header validation should fail when required columns are missing."""
    path = tmp_path / "bad.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")

    with pytest.raises(ValueError):
        validate_headers(path, REQUIRED)
