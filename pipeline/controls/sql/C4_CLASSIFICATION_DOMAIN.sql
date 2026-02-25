-- Contract output columns:
-- control_value (NUMBER), total_count (NUMBER), fail_count (NUMBER),
-- variance (NUMBER), status (STRING), details (STRING)
SELECT
  SUM(IFF({{snapshot_pii_class}} NOT IN ('NONE', 'LOW', 'MEDIUM', 'HIGH'), 1, 0)) AS control_value,
  COUNT(*) AS total_count,
  SUM(IFF({{snapshot_pii_class}} NOT IN ('NONE', 'LOW', 'MEDIUM', 'HIGH'), 1, 0)) AS fail_count,
  CAST(NULL AS FLOAT) AS variance,
  IFF(
    SUM(IFF({{snapshot_pii_class}} NOT IN ('NONE', 'LOW', 'MEDIUM', 'HIGH'), 1, 0)) <= %(threshold)s,
    'PASS',
    'FAIL'
  ) AS status,
  'Rows with pii_class outside approved taxonomy' AS details
FROM RAW.CLAIMS_SNAPSHOT_NIGHTLY
WHERE {{snapshot_batch_date}} = %(batch_date)s::DATE;
