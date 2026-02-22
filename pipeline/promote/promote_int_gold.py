"""Promote governed data from RAW to INT and GOLD layers."""

from __future__ import annotations

from pipeline.common.raw_columns import snapshot_expressions


def ensure_int_snapshot_table(conn) -> None:
    """Create INT snapshot table if missing."""
    ddl = """
      CREATE TABLE IF NOT EXISTS INT.CLAIMS_SNAPSHOT (
        batch_date DATE,
        claim_id STRING,
        policy_id STRING,
        customer_id STRING,
        claim_amount_incurred NUMBER(18,2),
        paid_amount_to_date NUMBER(18,2),
        reserve_amount NUMBER(18,2),
        loss_date DATE,
        report_date DATE,
        claim_status STRING,
        pii_class STRING,
        loaded_at TIMESTAMP_NTZ,
        PRIMARY KEY (claim_id)
      )
    """
    with conn.cursor() as cur:
        cur.execute(ddl)


def promote_snapshot_to_int(conn, batch_date: str) -> int:
    """Merge snapshot records from RAW into INT snapshot table."""
    ensure_int_snapshot_table(conn)
    cols = snapshot_expressions(conn)
    merge_sql = f"""
      MERGE INTO INT.CLAIMS_SNAPSHOT AS tgt
      USING (
        SELECT
          src.batch_date,
          src.claim_id,
          src.policy_id,
          src.customer_id,
          src.claim_amount_incurred,
          src.paid_amount_to_date,
          src.reserve_amount,
          src.loss_date,
          src.report_date,
          src.claim_status,
          src.pii_class,
          src.loaded_at
        FROM (
          SELECT
            {cols["batch_date"]} AS batch_date,
            {cols["claim_id"]} AS claim_id,
            {cols["policy_id"]} AS policy_id,
            {cols["customer_id"]} AS customer_id,
            {cols["claim_amount_incurred"]} AS claim_amount_incurred,
            {cols["paid_amount_to_date"]} AS paid_amount_to_date,
            {cols["reserve_amount"]} AS reserve_amount,
            {cols["loss_date"]} AS loss_date,
            {cols["report_date"]} AS report_date,
            {cols["claim_status"]} AS claim_status,
            {cols["pii_class"]} AS pii_class,
            {cols["loaded_at"]} AS loaded_at
          FROM RAW.CLAIMS_SNAPSHOT_NIGHTLY
          WHERE {cols["batch_date"]} = %(batch_date)s::DATE
        ) AS src
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY src.claim_id
          ORDER BY src.loaded_at DESC NULLS LAST
        ) = 1
      ) AS src
      ON tgt.claim_id = src.claim_id
      WHEN MATCHED THEN UPDATE SET
        batch_date = src.batch_date,
        policy_id = src.policy_id,
        customer_id = src.customer_id,
        claim_amount_incurred = src.claim_amount_incurred,
        paid_amount_to_date = src.paid_amount_to_date,
        reserve_amount = src.reserve_amount,
        loss_date = src.loss_date,
        report_date = src.report_date,
        claim_status = src.claim_status,
        pii_class = src.pii_class,
        loaded_at = src.loaded_at
      WHEN NOT MATCHED THEN INSERT (
        batch_date,
        claim_id,
        policy_id,
        customer_id,
        claim_amount_incurred,
        paid_amount_to_date,
        reserve_amount,
        loss_date,
        report_date,
        claim_status,
        pii_class,
        loaded_at
      ) VALUES (
        src.batch_date,
        src.claim_id,
        src.policy_id,
        src.customer_id,
        src.claim_amount_incurred,
        src.paid_amount_to_date,
        src.reserve_amount,
        src.loss_date,
        src.report_date,
        src.claim_status,
        src.pii_class,
        src.loaded_at
      )
    """
    with conn.cursor() as cur:
        cur.execute(merge_sql, {"batch_date": batch_date})

    count_sql = """
      SELECT COUNT(*)
      FROM INT.CLAIMS_SNAPSHOT
      WHERE batch_date = %(batch_date)s::DATE
    """
    with conn.cursor() as cur:
        cur.execute(count_sql, {"batch_date": batch_date})
        row = cur.fetchone()
    return int(row[0] if row else 0)
