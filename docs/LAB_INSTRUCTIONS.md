# Claims Governed PoC Lab Instructions

## 1) Prerequisites
- Python 3.11+
- Snowflake account and role with permissions to create/use objects in:
  - `CLAIMS_POC.RAW`
  - `CLAIMS_POC.CTRL`
  - `CLAIMS_POC.INT`
  - `CLAIMS_POC.GOLD`
  - `CLAIMS_POC.SEM` (or `SEMANTIC`)

## 2) Folder Structure (important folders)
- `pipeline/`: Python runtime pipeline code
- `sql/`: Snowflake DDL and transformation SQL
- `samples/nightly_drop/`: input CSV drops per batch date
- `rules/controls.yaml`: DQ and policy controls
- `config/`: environment and helper scripts
- `tests/`: regression and unit tests

## 3) Setup Local Environment
From project root:

```bash
python3 -m venv .venv
source .venv/bin/activate
.venv/bin/python -c "import sys; print(sys.executable)"
.venv/bin/pip install -r requirements.txt
.venv/bin/pip install pytest
```

## 4) Configure Snowflake Environment Variables
Use the helper script:

```bash
source config/snowflake_env.sh
```

The script exports:
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`
- prompts for `SNOWFLAKE_PASSWORD` if not set

Validate variables are set (zsh):

```bash
for v in SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_PASSWORD SNOWFLAKE_ROLE SNOWFLAKE_WAREHOUSE SNOWFLAKE_DATABASE SNOWFLAKE_SCHEMA; do
  if [[ -n ${(P)v} ]]; then echo "SET: $v"; else echo "MISSING: $v"; fi
done
```

## 5) Bootstrap Snowflake Objects
Run foundation SQL first:

```bash
.venv/bin/python -m scripts.run_sql sql/00_bootstrap/snowflake_setup.sql
.venv/bin/python -m scripts.run_sql sql/00_bootstrap/stages_file_formats.sql
```

Then create RAW/CTRL/INT/GOLD/SEM objects:

```bash
.venv/bin/python -m scripts.run_sql sql/01_raw/create_raw_tables_all_varchar.sql
.venv/bin/python -m scripts.run_sql sql/02_ctrl/ctrl_tables.sql
.venv/bin/python -m scripts.run_sql sql/03_int/int_facts.sql
.venv/bin/python -m scripts.run_sql sql/04_gold/gold_claims_mart.sql
.venv/bin/python -m scripts.run_sql sql/05_semantic/views_kpi.sql
```

## 6) Prepare Input Files
Place files under `samples/nightly_drop/` with exact names:
- `claims_snapshot_YYYYMMDD.csv`
- `claims_events_YYYYMMDD.csv`

Example:
- `claims_snapshot_20260222.csv`
- `claims_events_20260222.csv`

## 7) Run Nightly Pipeline
Run one date:

```bash
.venv/bin/python -m pipeline.orchestrator.nightly_job --batch-date 2026-02-22
```

Run multiple days:

```bash
.venv/bin/python -m pipeline.orchestrator.nightly_job --batch-date 2026-02-19
.venv/bin/python -m pipeline.orchestrator.nightly_job --batch-date 2026-02-20
.venv/bin/python -m pipeline.orchestrator.nightly_job --batch-date 2026-02-21
.venv/bin/python -m pipeline.orchestrator.nightly_job --batch-date 2026-02-22
```

## 8) Refresh GOLD + SEM Layer
After loads, refresh KPI objects:

```bash
.venv/bin/python -m scripts.run_sql sql/04_gold/gold_claims_mart.sql
.venv/bin/python -m scripts.run_sql sql/05_semantic/views_kpi.sql
```

## 9) Validation SQL
### Run status by day
```sql
SELECT batch_date, status, record_count, start_ts, end_ts
FROM CTRL.RUN_AUDIT
ORDER BY batch_date, start_ts DESC;
```

### Control outcomes
```sql
SELECT run_id, control_id, status, fail_count, severity, executed_ts
FROM CTRL.CONTROL_RESULT
ORDER BY executed_ts DESC, control_id;
```

### INT row counts
```sql
SELECT batch_date, COUNT(*) AS row_count
FROM INT.CLAIMS_SNAPSHOT
GROUP BY batch_date
ORDER BY batch_date;
```

### GOLD daily aggregates
```sql
SELECT * FROM GOLD.CLAIMS_MART ORDER BY batch_date;
```

### Semantic KPI views
```sql
SELECT * FROM SEM.VW_KPI_RTW_RATE ORDER BY batch_date;
SELECT * FROM SEM.VW_KPI_DENIAL_RATE ORDER BY batch_date;
```

## 10) Run Tests
```bash
.venv/bin/python -m pytest -q
```

Key regression tests:
- `tests/test_raw_columns.py`: named vs `COL_*` RAW schema support
- `tests/test_promote_dedup.py`: dedup merge behavior for duplicate claim rows

## 11) Common Errors and Fixes
- `Missing input files`: ensure both snapshot and events CSV files exist for date.
- `Password is empty`: source `config/snowflake_env.sh` and provide password.
- `invalid identifier 'BATCH_DATE'`: run current code (supports legacy `COL_*` fallback) and ensure RAW DDL is applied.
- `Duplicate row detected during DML action`: handled by dedup merge logic in promotion module.
- `Schema ... does not exist`: run bootstrap SQL first.
