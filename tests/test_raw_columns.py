"""Regression tests for RAW schema variant column expression mapping."""

from __future__ import annotations

from collections.abc import Callable

from pipeline.common import raw_columns


def _patch_table_columns(
    monkeypatch,
    resolver: Callable[[str, str], set[str]],
) -> None:
    def fake_table_columns(conn, schema_name: str, table_name: str) -> set[str]:
        return resolver(schema_name, table_name)

    monkeypatch.setattr(raw_columns, "_table_columns", fake_table_columns)


def test_snapshot_expressions_named_columns_with_loaded_at(monkeypatch) -> None:
    """Named RAW columns should map directly and prefer LOADED_AT."""
    _patch_table_columns(
        monkeypatch,
        lambda schema, table: {"BATCH_DATE", "CLAIM_ID", "LOADED_AT"}
        if table == "CLAIMS_SNAPSHOT_NIGHTLY"
        else set(),
    )

    cols = raw_columns.snapshot_expressions(conn=None)

    assert cols["batch_date"] == "BATCH_DATE"
    assert cols["claim_id"] == "CLAIM_ID"
    assert cols["loaded_at"] == "LOADED_AT"


def test_snapshot_expressions_named_columns_with_load_ts_only(monkeypatch) -> None:
    """Named RAW columns should fall back to LOAD_TS when LOADED_AT is absent."""
    _patch_table_columns(
        monkeypatch,
        lambda schema, table: {"BATCH_DATE", "CLAIM_ID", "LOAD_TS"}
        if table == "CLAIMS_SNAPSHOT_NIGHTLY"
        else set(),
    )

    cols = raw_columns.snapshot_expressions(conn=None)

    assert cols["batch_date"] == "BATCH_DATE"
    assert cols["loaded_at"] == "LOAD_TS"


def test_snapshot_expressions_legacy_columns(monkeypatch) -> None:
    """Legacy COL_* RAW schema should map to positional expressions."""
    _patch_table_columns(
        monkeypatch,
        lambda schema, table: {"COL_1", "COL_3", "COL_24", "COL_25", "COL_26", "LOAD_TS"}
        if table == "CLAIMS_SNAPSHOT_NIGHTLY"
        else set(),
    )

    cols = raw_columns.snapshot_expressions(conn=None)

    assert cols["batch_date"] == "TRY_TO_DATE(COL_1)"
    assert cols["claim_id"] == "COL_3"
    assert cols["claim_amount_incurred"] == "TRY_TO_NUMBER(COL_24, 18, 2)"
    assert cols["loaded_at"] == "LOAD_TS"


def test_snapshot_expressions_missing_loaded_columns(monkeypatch) -> None:
    """Missing load timestamp columns should use CURRENT_TIMESTAMP fallback."""
    _patch_table_columns(
        monkeypatch,
        lambda schema, table: {"COL_1"} if table == "CLAIMS_SNAPSHOT_NIGHTLY" else set(),
    )

    cols = raw_columns.snapshot_expressions(conn=None)

    assert cols["loaded_at"] == "CURRENT_TIMESTAMP()"


def test_events_expressions_named_columns(monkeypatch) -> None:
    """Events RAW named schema should map directly to BATCH_DATE/EVENT_TYPE."""
    _patch_table_columns(
        monkeypatch,
        lambda schema, table: {"BATCH_DATE", "EVENT_TYPE"}
        if table == "CLAIMS_EVENTS_NIGHTLY"
        else set(),
    )

    cols = raw_columns.events_expressions(conn=None)

    assert cols["batch_date"] == "BATCH_DATE"
    assert cols["event_type"] == "EVENT_TYPE"


def test_events_expressions_legacy_columns(monkeypatch) -> None:
    """Events RAW legacy schema should map to COL_1/COL_4 expressions."""
    _patch_table_columns(
        monkeypatch,
        lambda schema, table: {"COL_1", "COL_4"}
        if table == "CLAIMS_EVENTS_NIGHTLY"
        else set(),
    )

    cols = raw_columns.events_expressions(conn=None)

    assert cols["batch_date"] == "TRY_TO_DATE(COL_1)"
    assert cols["event_type"] == "COL_4"
