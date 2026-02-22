USE DATABASE CLAIMS_POC;
USE SCHEMA GOLD;

-- Daily aggregate table consumed by semantic KPI views.
CREATE TABLE IF NOT EXISTS CLAIMS_MART (
  batch_date DATE,
  claim_count NUMBER,
  total_amount NUMBER(18,2),
  paid_claim_count NUMBER,
  denied_claim_count NUMBER,
  refresh_ts TIMESTAMP_NTZ
);

-- Upsert one summary row per batch_date from INT snapshot facts.
MERGE INTO GOLD.CLAIMS_MART AS tgt
USING (
  SELECT
    batch_date,
    COUNT(*) AS claim_count,
    COALESCE(SUM(claim_amount_incurred), 0)::NUMBER(18,2) AS total_amount,
    COUNT_IF(claim_status = 'PAID') AS paid_claim_count,
    COUNT_IF(claim_status = 'DENIED') AS denied_claim_count,
    CURRENT_TIMESTAMP() AS refresh_ts
  FROM INT.CLAIMS_SNAPSHOT
  GROUP BY batch_date
) AS src
ON tgt.batch_date = src.batch_date
WHEN MATCHED THEN UPDATE SET
  claim_count = src.claim_count,
  total_amount = src.total_amount,
  paid_claim_count = src.paid_claim_count,
  denied_claim_count = src.denied_claim_count,
  refresh_ts = src.refresh_ts
WHEN NOT MATCHED THEN INSERT (
  batch_date,
  claim_count,
  total_amount,
  paid_claim_count,
  denied_claim_count,
  refresh_ts
) VALUES (
  src.batch_date,
  src.claim_count,
  src.total_amount,
  src.paid_claim_count,
  src.denied_claim_count,
  src.refresh_ts
);
