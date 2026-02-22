# Runbook

- Bootstrap Snowflake objects using SQL scripts in order.
- Execute nightly job with `--batch-date`.
- Inspect `CTRL.RUN_AUDIT`, `CTRL.CONTROL_RESULT`, and `CTRL.EXCEPTIONS` for evidence.
- Review local artifacts under `artifacts/dq_reports/` and `artifacts/manifests/`.
