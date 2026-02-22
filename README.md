# Claims Governed Pipeline PoC

This repository contains a governed Snowflake claims ingestion PoC.

## Scope
- Load nightly `snapshot` and `events` claims files into RAW tables.
- Execute controls and persist evidence in `CTRL`.
- Promote governed data from RAW to INT and GOLD only when gate checks pass.

## Runtime
- Python: 3.11
- Virtual env: `.venv/bin/python`
- Data plane: Snowflake (`snowflake-connector-python`)

## Run
```bash
.venv/bin/python -m pipeline.orchestrator.nightly_job --batch-date 2026-02-21
```

## Notes
- Credentials are read from environment variables only.
- Local logs/reports are written under `artifacts/`.

## Learning Guides
- Starter guide: `docs/starter_guide.md`
- Full lab instructions: `LAB_INSTRUCTIONS.md`
