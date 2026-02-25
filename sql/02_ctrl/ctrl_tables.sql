USE DATABASE CLAIMS_POC;
USE SCHEMA CTRL;

CREATE OR REPLACE TABLE RUN_AUDIT (
  run_id STRING NOT NULL,
  dataset_name STRING NOT NULL,
  batch_date DATE NOT NULL,
  file_name STRING,
  start_ts TIMESTAMP_NTZ NOT NULL,
  end_ts TIMESTAMP_NTZ,
  status STRING,
  record_count NUMBER,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE CONTROL_RESULT (
  run_id STRING NOT NULL,
  batch_date DATE,
  control_id STRING NOT NULL,
  control_name STRING,
  status STRING,
  total_count NUMBER,
  fail_count NUMBER,
  variance FLOAT,
  severity STRING,
  blocking_flag BOOLEAN,
  details STRING,
  executed_sql_hash STRING,
  executed_at TIMESTAMP_NTZ,
  executed_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE EXCEPTIONS (
  run_id STRING NOT NULL,
  control_id STRING NOT NULL,
  claim_id STRING,
  error_message STRING,
  severity STRING,
  recorded_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE PROMOTION_HISTORY (
  run_id STRING NOT NULL,
  dataset_name STRING NOT NULL,
  promoted_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  status STRING
);
