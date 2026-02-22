USE DATABASE CLAIMS_POC;
USE SCHEMA INT;

-- Curated snapshot fact table populated from RAW after controls pass.
CREATE TABLE IF NOT EXISTS CLAIMS_SNAPSHOT (
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
);

-- Curated event fact table (available for future event-level analytics).
CREATE TABLE IF NOT EXISTS CLAIMS_EVENTS (
  batch_date DATE,
  claim_id STRING,
  event_ts TIMESTAMP_NTZ,
  event_type STRING,
  old_status STRING,
  new_status STRING,
  amount_delta NUMBER(18,2),
  currency STRING,
  source_system STRING,
  note STRING,
  loaded_at TIMESTAMP_NTZ,
  PRIMARY KEY (claim_id, event_ts, event_type)
);
