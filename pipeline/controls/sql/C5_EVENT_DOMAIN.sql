-- Contract output columns:
-- control_value (NUMBER), total_count (NUMBER), fail_count (NUMBER),
-- variance (NUMBER), status (STRING), details (STRING)
SELECT
  SUM(
    IFF(
      {{events_event_type}} NOT IN ('CREATED', 'UPDATED', 'STATUS_CHANGE', 'PAYMENT', 'NOTE'),
      1,
      0
    )
  ) AS control_value,
  COUNT(*) AS total_count,
  SUM(
    IFF(
      {{events_event_type}} NOT IN ('CREATED', 'UPDATED', 'STATUS_CHANGE', 'PAYMENT', 'NOTE'),
      1,
      0
    )
  ) AS fail_count,
  CAST(NULL AS FLOAT) AS variance,
  IFF(
    SUM(
      IFF(
        {{events_event_type}} NOT IN ('CREATED', 'UPDATED', 'STATUS_CHANGE', 'PAYMENT', 'NOTE'),
        1,
        0
      )
    ) <= %(threshold)s,
    'PASS',
    'FAIL'
  ) AS status,
  'Rows with event_type outside approved taxonomy' AS details
FROM RAW.CLAIMS_EVENTS_NIGHTLY
WHERE {{events_batch_date}} = %(batch_date)s::DATE;
