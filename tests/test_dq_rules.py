"""Tests for controls configuration parsing."""

from __future__ import annotations

import csv
from pathlib import Path

from pipeline.controls.run_controls import load_controls


def test_controls_yaml_contains_controls_list() -> None:
    """Controls config should expose a non-empty controls list."""
    payload = load_controls("rules/controls.yaml")
    controls = payload.get("controls")
    assert isinstance(controls, list)
    assert len(controls) > 0


def test_sql_controls_have_versioned_sql_pointer() -> None:
    """Every SQL control should include a repo sql_path pointer."""
    payload = load_controls("rules/controls.yaml")
    for control in payload.get("controls", []):
        if control.get("type") == "sql":
            assert isinstance(control.get("sql_path"), str)
            assert control["sql_path"].strip()
            assert Path("pipeline/controls/sql", control["sql_path"]).exists()


def test_controls_include_blocking_non_negative_rule() -> None:
    """C2 must remain a BLOCK control with threshold 0 for safety."""
    payload = load_controls("rules/controls.yaml")
    controls = payload.get("controls", [])
    c2 = next(item for item in controls if item["id"] == "C2_DQ_NON_NEGATIVE")
    assert c2["severity"] == "BLOCK"
    assert float(c2["threshold"]) == 0.0


def test_controls_include_blocking_duplicate_claim_rule() -> None:
    """C8 must remain a blocking duplicate guard with threshold 0."""
    payload = load_controls("rules/controls.yaml")
    controls = payload.get("controls", [])
    c8 = next(item for item in controls if item["id"] == "C8_DUPLICATE_CLAIM_ID")
    assert c8["type"] == "sql"
    assert c8["blocking"] is True
    assert c8["severity"] == "BLOCK"
    assert c8["sql_path"] == "C8_DUPLICATE_CLAIM_ID.sql"
    assert float(c8["threshold"]) == 0.0


def test_mini_claims_fixture_has_expected_dq_failures() -> None:
    """Fixture should include known edge cases to trigger DQ controls."""
    fixture_path = Path("tests/fixtures/mini_claims.csv")
    with fixture_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 5

    negative_amount_rows = [
        row
        for row in rows
        if float(row["claim_amount_incurred"]) < 0
        or float(row["paid_amount_to_date"]) < 0
        or float(row["reserve_amount"]) < 0
    ]
    assert len(negative_amount_rows) == 1

    missing_claim_id_rows = [row for row in rows if not row["claim_id"]]
    assert len(missing_claim_id_rows) == 1
