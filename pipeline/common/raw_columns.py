"""Column-expression helpers for RAW tables with evolving schemas."""

from __future__ import annotations


def _table_columns(conn, schema_name: str, table_name: str) -> set[str]:
    """Return uppercase column names for an existing table."""
    sql = """
      SELECT UPPER(column_name)
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE table_schema = %(schema)s
        AND table_name = %(table)s
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"schema": schema_name.upper(), "table": table_name.upper()})
        rows = cur.fetchall() or []
    return {row[0] for row in rows}


def snapshot_expressions(conn) -> dict[str, str]:
    """Return SQL expressions for logical snapshot fields."""
    cols = _table_columns(conn, "RAW", "CLAIMS_SNAPSHOT_NIGHTLY")
    # Timestamp column name changed across schema versions.
    if "LOADED_AT" in cols:
        loaded_at_expr = "LOADED_AT"
    elif "LOAD_TS" in cols:
        loaded_at_expr = "LOAD_TS"
    else:
        # Keep pipeline runnable even if metadata column is absent.
        loaded_at_expr = "CURRENT_TIMESTAMP()"
    if "BATCH_DATE" in cols:
        # New schema: use direct column names.
        return {
            "batch_date": "BATCH_DATE",
            "claim_id": "CLAIM_ID",
            "policy_id": "POLICY_ID",
            "customer_id": "CUSTOMER_ID",
            "claim_amount_incurred": "CLAIM_AMOUNT_INCURRED",
            "paid_amount_to_date": "PAID_AMOUNT_TO_DATE",
            "reserve_amount": "RESERVE_AMOUNT",
            "loss_date": "LOSS_DATE",
            "report_date": "REPORT_DATE",
            "claim_status": "CLAIM_STATUS",
            "pii_class": "PII_CLASS",
            "loaded_at": loaded_at_expr,
        }
    # Legacy schema: map logical fields to positional COL_* columns.
    return {
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
        "loaded_at": loaded_at_expr,
    }


def events_expressions(conn) -> dict[str, str]:
    """Return SQL expressions for logical events fields used by controls."""
    cols = _table_columns(conn, "RAW", "CLAIMS_EVENTS_NIGHTLY")
    if "BATCH_DATE" in cols:
        # New schema with explicit field names.
        return {"batch_date": "BATCH_DATE", "event_type": "EVENT_TYPE"}
    # Legacy schema.
    return {"batch_date": "TRY_TO_DATE(COL_1)", "event_type": "COL_4"}
