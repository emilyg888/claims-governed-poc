USE DATABASE CLAIMS_POC;
USE SCHEMA CTRL;

CREATE OR REPLACE PROCEDURE RUN_CONTROLS(
  p_run_id VARCHAR,
  p_batch_date DATE
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  v_fail_count NUMBER DEFAULT 0;
  v_block_fails NUMBER DEFAULT 0;
BEGIN
  SELECT COUNT(*)
  INTO :v_fail_count
  FROM RAW.CLAIMS_SNAPSHOT_NIGHTLY
  WHERE batch_date = :p_batch_date
    AND (
      claim_amount_incurred < 0
      OR paid_amount_to_date < 0
      OR reserve_amount < 0
    );

  INSERT INTO CTRL.CONTROL_RESULT (
    run_id,
    control_id,
    control_name,
    status,
    fail_count,
    severity,
    executed_ts
  )
  VALUES (
    :p_run_id,
    'C2_DQ_NON_NEGATIVE',
    'Non-negative snapshot financial values',
    IFF(:v_fail_count = 0, 'PASS', 'FAIL'),
    :v_fail_count,
    'BLOCK',
    CURRENT_TIMESTAMP()
  );

  INSERT INTO CTRL.EXCEPTIONS (
    run_id,
    control_id,
    claim_id,
    error_message,
    severity,
    recorded_ts
  )
  SELECT
    :p_run_id,
    'C2_DQ_NON_NEGATIVE',
    claim_id,
    'Negative financial value detected',
    'BLOCK',
    CURRENT_TIMESTAMP()
  FROM RAW.CLAIMS_SNAPSHOT_NIGHTLY
  WHERE batch_date = :p_batch_date
    AND (
      claim_amount_incurred < 0
      OR paid_amount_to_date < 0
      OR reserve_amount < 0
    );

  SELECT COUNT(*)
  INTO :v_block_fails
  FROM CTRL.CONTROL_RESULT
  WHERE run_id = :p_run_id
    AND severity = 'BLOCK'
    AND status = 'FAIL';

  RETURN IFF(:v_block_fails = 0, 'PASS', 'FAIL');
END;
$$;
