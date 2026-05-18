# Starter Guide for Beginners

## Goal
Learn this project by running one batch end-to-end, then tracing how data moves from RAW to KPI views.

## Recommended Learning Order
1. Read the lab walkthrough:
   - `LAB_INSTRUCTIONS.md`
2. Read pipeline entrypoint:
   - `pipeline/orchestrator/nightly_job.py`
3. Read ingestion:
   - `pipeline/ingest/load_to_snowflake.py`
4. Read controls:
   - `pipeline/controls/run_controls.py`
   - `rules/controls.yaml`
5. Read promotion:
   - `pipeline/promote/promote_int_gold.py`
6. Read output layers:
   - `sql/04_gold/gold_claims_mart.sql`
   - `sql/05_semantic/views_kpi.sql`

## Hands-On Path
1. Run one batch date with the orchestrator.
2. Validate output tables:
   - `CTRL.RUN_AUDIT`
   - `CTRL.CONTROL_RESULT`
   - `INT.CLAIMS_SNAPSHOT`
   - `GOLD.CLAIMS_MART`
3. Query semantic KPI views:
   - `SEM.VW_KPI_RTW_RATE`
   - `SEM.VW_KPI_DENIAL_RATE`

## Learn Through Tests
Use tests as examples of expected behavior:
- `tests/test_raw_columns.py`
- `tests/test_promote_dedup.py`
- `tests/test_promotion_gate.py`

## Safe Practice Loop
1. Change one small rule (for example in `rules/controls.yaml`).
2. Run one batch date.
3. Observe changes in control and KPI outputs.
4. Add/update a test to lock expected behavior.

## Practical Advice
- Follow the code from entrypoint to downstream modules.
- Focus on one batch date until the flow is clear.
- Keep changes small and validated with tests.
