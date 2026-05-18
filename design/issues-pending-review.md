# Issues Pending Review

## Summary

| ID | Severity | Area | Issue | Recommended action | Status |
|---|---|---|---|---|---|
| ISSUE-001 | Medium | Config | Local `.env` and `config/snowflake_env.sh` files exist in the working tree and may contain credentials. | Keep ignored, never commit values, and rotate any credential that may have been exposed outside the local machine. | Pending review |
| ISSUE-002 | Medium | Governance | AGENTS instructions reference `07_runtime_local/query_engine/execute_query.py`, but this repository does not contain `07_runtime_local/`. | Reconcile AGENTS.md with the current repo shape or add the governed query-engine path if it is required. | Pending review |
| ISSUE-003 | Low | Code | `config/bootstrap.py` executes SQL by splitting on semicolons and bypasses the more robust `scripts.run_sql` splitter. | Replace or retire `config/bootstrap.py` in favor of `scripts.run_sql` for bootstrap execution. | Pending review |
| ISSUE-004 | Low | Dashboard | `streamlit/app.py` and `streamlit/components.py` had pre-existing uncommitted edits before housekeeping started. | Review the dashboard ordering/trend changes and decide whether to keep them. | Pending review |
| ISSUE-005 | Low | Packaging | Generated package metadata was present locally under `claims_governed_poc.egg-info/`; it is ignored but should remain out of commits. | Leave ignored and remove locally after confirming no tooling expects the folder. | Pending review |

## SIT Results

| Command | Result | Notes |
|---|---|---|
| `.venv/bin/python -c "import sys; print(sys.executable)"` | Passed | Output used the project-local `.venv/bin/python`. |
| `. .venv/bin/activate && .venv/bin/python -m pytest -q` | Passed | 23 tests passed; final validation completed after archiving and doc updates. |
| `. .venv/bin/activate && .venv/bin/python -m compileall pipeline tests streamlit config scripts` | Passed | Compile smoke check completed. |

## Archived Code Review

| Original path | Archived path | Reason | Review needed? |
|---|---|---|---|
| `build/` | `src_archives/2026-05-18_housekeeping/build/` | Generated setuptools build output; no repo references found and files mirror active source modules. | No |
| `notes_ai/` | `src_archives/2026-05-18_housekeeping/notes_ai/` | Untracked AI notes folder; raw files were checksum-identical to tracked `docs/` files and no repo references were found. | No |
| `docs/ModernDataPipeline.drawio v1.png` | `src_archives/2026-05-18_housekeeping/docs/ModernDataPipeline.drawio v1.png` | Untracked image artifact; checksum-identical copy existed in `notes_ai/raw/` and no repo references were found. | No |

## Detailed Issues

### ISSUE-001 - Local Secret Files Present

- Severity: Medium
- Area: Security
- Evidence: `.env` and `config/snowflake_env.sh` exist locally and are ignored by `.gitignore`.
- Impact: Accidental credential disclosure is possible if ignore rules are bypassed or files are copied into docs/logs.
- Recommended action: Keep files ignored, avoid printing values, and rotate any credential that may have left the local machine.
- Status: Pending review

### ISSUE-002 - AGENTS Runtime Path Mismatch

- Severity: Medium
- Area: Governance
- Evidence: AGENTS.md requires SQL through `07_runtime_local/query_engine/execute_query.py`, but this repo currently uses `pipeline/`, `scripts/`, and `sql/` paths and does not contain `07_runtime_local/`.
- Impact: Agents following AGENTS.md literally cannot satisfy the deterministic SQL execution rule in this repository.
- Recommended action: Update AGENTS.md to match this repo or add the required governed query-engine module.
- Status: Pending review

### ISSUE-003 - Bootstrap SQL Runner Is Less Robust

- Severity: Low
- Area: Code
- Evidence: `config/bootstrap.py` splits SQL with `sql.split(";")`; `scripts/run_sql.py` has a block-aware splitter for `$$` procedure bodies.
- Impact: Bootstrap scripts containing procedure bodies or complex SQL can execute incorrectly.
- Recommended action: Route bootstrap execution through `scripts.run_sql` or delete the redundant bootstrap helper after review.
- Status: Pending review

### ISSUE-004 - Pre-Existing Dashboard Edits

- Severity: Low
- Area: Code
- Evidence: Before housekeeping, `streamlit/app.py` had moved the failure details panel above the trend section, and `streamlit/components.py` had removed the variance trend chart.
- Impact: Dashboard behavior may have intentionally changed, but it was not part of housekeeping and should be reviewed as a separate functional change.
- Recommended action: Review and either commit with a dashboard-specific message or adjust before handoff.
- Status: Pending review

### ISSUE-005 - Local Generated Package Metadata

- Severity: Low
- Area: Packaging
- Evidence: `claims_governed_poc.egg-info/` exists locally and `.gitignore` ignores `*.egg-info/`.
- Impact: Local generated metadata can confuse housekeeping scans, but should not affect source execution.
- Recommended action: Remove locally after confirming no active editable install workflow relies on the current folder.
- Status: Pending review
