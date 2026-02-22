"""Tests for promotion SQL dedup behavior."""

from __future__ import annotations

from pipeline.promote import promote_int_gold


class _FakeCursor:
    def __init__(self, conn) -> None:
        self._conn = conn
        self._last_sql = ""

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: dict | None = None) -> None:
        self._last_sql = sql
        self._conn.calls.append((sql, params))

    def fetchone(self):
        if "SELECT COUNT(*)" in self._last_sql:
            return (0,)
        return None


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict | None]] = []

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)


def test_promote_sql_deduplicates_by_claim_id(monkeypatch) -> None:
    """Merge source should deduplicate RAW rows per claim_id."""
    monkeypatch.setattr(
        promote_int_gold,
        "snapshot_expressions",
        lambda conn: {
            "batch_date": "TRY_TO_DATE(COL_1)",
            "claim_id": "COL_3",
            "policy_id": "COL_5",
            "customer_id": "COL_6",
            "claim_amount_incurred": "TRY_TO_NUMBER(COL_24, 18, 2)",
            "paid_amount_to_date": "TRY_TO_NUMBER(COL_25, 18, 2)",
            "reserve_amount": "TRY_TO_NUMBER(COL_26, 18, 2)",
            "loss_date": "TRY_TO_DATE(COL_12)",
            "report_date": "TRY_TO_DATE(COL_13)",
            "claim_status": "COL_18",
            "pii_class": "COL_29",
            "loaded_at": "LOAD_TS",
        },
    )

    conn = _FakeConn()
    promote_int_gold.promote_snapshot_to_int(conn, "2026-02-21")

    merge_sql = next(sql for sql, _ in conn.calls if "MERGE INTO INT.CLAIMS_SNAPSHOT" in sql)
    assert "QUALIFY ROW_NUMBER() OVER" in merge_sql
    assert "PARTITION BY src.claim_id" in merge_sql
    assert "ORDER BY src.loaded_at DESC NULLS LAST" in merge_sql
