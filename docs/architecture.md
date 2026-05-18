# Architecture

## 1. Purpose

This project is a governed Snowflake claims ingestion proof of concept. It loads nightly claims snapshot and event files, validates schema and data quality, records control evidence, and promotes data into governed INT and GOLD layers only when blocking controls pass.

## 2. Current System Shape

The repository is a Python 3.11 application with a Snowflake data plane, file-based configuration, SQL assets, unit tests, and a Streamlit controls dashboard.

Entry points:

- `pipeline.orchestrator.nightly_job` runs the nightly ingestion, control, and promotion workflow.
- `scripts.run_sql` executes ordered Snowflake SQL setup and maintenance files.
- `streamlit/app.py` renders the controls dashboard.

The runtime uses `.venv/bin/python`. Credentials and secret values are expected from environment variables and must not be committed.

## 3. Component Map

| Component | Path | Responsibility | Key dependencies |
|---|---|---|---|
| Configuration | `config/`, `rules/`, `schemas/` | Environment defaults, dataset metadata, control registers, DQ rules, and JSON schemas | `PyYAML`, `jsonschema` |
| Snowflake access | `pipeline/common/snowflake_client.py` | Connection parameter assembly, transaction-scoped connections, helper execution APIs | `snowflake-connector-python`, environment variables |
| Ingestion | `pipeline/ingest/` | File discovery, schema validation, Snowflake stage loading, and reconciliation | `samples/nightly_drop/`, `schemas/`, Snowflake RAW tables |
| Controls | `pipeline/controls/` | Metadata-driven control execution, evidence writing, control handlers, and SQL control assets | `rules/controls.yaml`, `pipeline/controls/sql/`, Snowflake CTRL tables |
| Promotion | `pipeline/promote/` | Gate-approved promotion from RAW into INT and GOLD layers | Snowflake RAW, INT, GOLD schemas |
| Orchestration | `pipeline/orchestrator/nightly_job.py` | End-to-end batch run, audit updates, load, controls, gate, and promotion | Ingestion, controls, promotion modules |
| SQL assets | `sql/` | Snowflake bootstrap, table/view DDL, stored procedures, and maintenance scripts | Snowflake |
| Dashboard | `streamlit/` | Operational view of run audit and control evidence | Streamlit, pandas, Snowflake CTRL views/tables |
| Tests | `tests/` | Unit and focused integration tests for schema, controls, promotion gates, and raw column behavior | `pytest` |
| Documentation | `docs/`, `README.md`, `design/` | Architecture, runbook, lab guides, handoff issues, and user-facing setup | Markdown |
| Archives | `src_archives/` | Date-stamped housekeeping archive of moved generated or duplicate files | Manual review |

## 4. Runtime Flow

```text
Nightly CSV files
  -> file discovery and schema validation
  -> Snowflake stage/COPY into RAW
  -> reconciliation and metadata-driven controls
  -> CTRL evidence, exceptions, and run audit records
  -> promotion gate
  -> INT and GOLD promotion when blocking controls pass
  -> Streamlit dashboard reads audit/control evidence
```

## 5. Data Flow

Input files are expected under `samples/nightly_drop/` or the configured nightly drop location. Snapshot and event files are discovered by batch date, validated against JSON schemas, loaded into RAW Snowflake tables, checked by controls C1-C8, and promoted to INT/GOLD when the promotion gate allows it.

Control metadata lives in YAML under `rules/`. Control SQL lives under `pipeline/controls/sql/`. Run status, control results, exceptions, and promotion history are persisted in Snowflake CTRL tables.

## 6. Configuration

Important configuration files:

- `config/env.dev.yaml` and `config/env.prodlike.yaml` define non-secret Snowflake defaults.
- `config/datasets.yaml` defines dataset-level metadata.
- `rules/controls.yaml`, `rules/dq_rules.yaml`, and `rules/classification.yaml` define governed validation behavior.
- `.env.example` documents local environment shape without secret values.

Important environment variables include `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, and `SNOWFLAKE_SCHEMA`.

Local `.env` files and `config/snowflake_env.sh` are ignored and must not be committed. The working tree currently contains local secret-bearing environment files; they were not modified during housekeeping.

## 7. Testing and SIT

The primary SIT command is:

```bash
. .venv/bin/activate && .venv/bin/python -m pytest -q
```

The compile smoke check is:

```bash
. .venv/bin/activate && .venv/bin/python -m compileall pipeline tests streamlit config scripts
```

Last housekeeping run on 2026-05-18:

- `pytest`: passed, 23 tests.
- `compileall`: passed.

## 8. Deployment / Execution

Install dependencies in the project virtual environment:

```bash
.venv/bin/pip install -e ".[dev,dashboard]"
```

Run one nightly batch:

```bash
.venv/bin/python -m pipeline.orchestrator.nightly_job --batch-date 2026-02-21
```

Run the Streamlit dashboard:

```bash
source config/snowflake_env.sh
.venv/bin/python -m streamlit run streamlit/app.py
```

Bootstrap and maintenance SQL is stored under `sql/` and executed through `scripts.run_sql`.

## 9. Governance / Operational Notes

- Promotion is blocked when a blocking control fails or errors.
- Control execution is metadata-driven from `rules/controls.yaml`.
- Evidence and exceptions are written to Snowflake CTRL tables for auditability.
- SQL files are checked in as governed assets; runtime access should use governed pipeline modules and Snowflake views/tables, not ad hoc raw table access.
- `.env` and shell environment files must remain untracked because they may contain credentials.
- The repository AGENTS instructions mention a `07_runtime_local/query_engine/execute_query.py` governed query path that does not exist in this repo shape. That mismatch is tracked for review.

## 10. Known Gaps

Known handoff items are tracked in `design/issues-pending-review.md`.
