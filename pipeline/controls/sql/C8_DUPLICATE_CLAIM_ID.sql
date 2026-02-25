-- Contract output columns:
-- control_value (NUMBER), total_count (NUMBER), fail_count (NUMBER),
-- variance (NUMBER), status (STRING), details (STRING)
WITH scoped AS (
  SELECT
    NULLIF(TRIM({{snapshot_claim_id}}), '') AS claim_id_norm
  FROM RAW.CLAIMS_SNAPSHOT_NIGHTLY
  WHERE {{snapshot_batch_date}} = %(batch_date)s::DATE
),
duplicates AS (
  SELECT
    UPPER(claim_id_norm) AS claim_key,
    COUNT(*) AS row_count
  FROM scoped
  WHERE claim_id_norm IS NOT NULL
  GROUP BY UPPER(claim_id_norm)
  HAVING COUNT(*) > 1
),
counts AS (
  SELECT
    COUNT(*) AS total_count
  FROM scoped
)
SELECT
  COALESCE(SUM(row_count - 1), 0) AS control_value,
  (SELECT total_count FROM counts) AS total_count,
  COALESCE(SUM(row_count - 1), 0) AS fail_count,
  CAST(COUNT(*) AS FLOAT) AS variance,
  IFF(COALESCE(SUM(row_count - 1), 0) <= %(threshold)s, 'PASS', 'FAIL') AS status,
  IFF(
    COUNT(*) = 0,
    'No duplicate claim_id rows found',
    'Duplicate claim_id groups detected: '
      || LISTAGG(claim_key, ', ') WITHIN GROUP (ORDER BY claim_key)
  ) AS details
FROM duplicates;
