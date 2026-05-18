# Claims Governed Pipeline PoC

## Overview

This repository contains a governed Snowflake claims ingestion proof of concept. It loads nightly claims snapshot and event files, executes metadata-driven controls, records evidence in CTRL tables, and promotes data to INT and GOLD only when blocking controls pass.

## Architecture Summary

Runtime flow:

```text
Nightly CSV files -> RAW load -> controls and evidence -> promotion gate -> INT/GOLD -> dashboard
```

Core runtime modules live under `pipeline/`. Snowflake SQL assets live under `sql/`. Control registers and DQ rules live under `rules/`. Operational documentation lives under `docs/` and `design/`.

## Repository Structure

| Path | Purpose |
|---|---|
| `pipeline/` | Ingestion, controls, orchestration, promotion, and shared Snowflake helpers |
| `streamlit/` | Controls dashboard |
| `config/` | Dataset and Snowflake environment defaults |
| `rules/` | Control, data quality, and classification rules |
| `schemas/` | JSON schemas and generated schema notes |
| `sql/` | Snowflake bootstrap, layer DDL, semantic views, and maintenance SQL |
| `tests/` | Pytest tests and acceptance notes |
| `docs/` | Architecture, runbook, starter guide, BRD, and decisions |
| `design/` | Handoff issues pending review |
| `src_archives/` | Date-stamped archived generated or duplicate files |

## Setup

Use the project-local virtual environment only:

```bash
.venv/bin/pip install -e ".[dev,dashboard]"
```

Validate the interpreter before running Python:

```bash
.venv/bin/python -c "import sys; print(sys.executable)"
```

The output should contain `/.venv/bin/python`.

## Run

Run one governed nightly batch:

```bash
.venv/bin/python -m pipeline.orchestrator.nightly_job --batch-date 2026-02-21
```

Run the controls dashboard:

```bash
source config/snowflake_env.sh
.venv/bin/python -m streamlit run streamlit/app.py
```

## Test / SIT

Run the main SIT suite:

```bash
. .venv/bin/activate && .venv/bin/python -m pytest -q
```

Run the compile smoke check:

```bash
. .venv/bin/activate && .venv/bin/python -m compileall pipeline tests streamlit config scripts
```

## Configuration

Credentials are read from environment variables. Do not commit `.env`, `.env.*`, or `config/snowflake_env.sh`.

Key Snowflake variables include `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, and `SNOWFLAKE_SCHEMA`.

## Documentation

- Architecture: `docs/architecture.md`
- Runbook: `docs/runbook.md`
- Starter guide: `docs/starter_guide.md`
- Pending review issues: `design/issues-pending-review.md`

## Current Status

Housekeeping on 2026-05-18 archived generated and duplicate local artifacts into `src_archives/2026-05-18_housekeeping/`. SIT passed with 23 tests. Some pre-existing Streamlit edits remain in the working tree and should be reviewed before handoff.
