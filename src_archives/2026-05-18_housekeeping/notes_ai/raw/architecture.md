# Architecture

Pipeline stages:
1. File discovery and schema validation
2. Stage + COPY into `RAW.CLAIMS_SNAPSHOT_NIGHTLY` and `RAW.CLAIMS_EVENTS_NIGHTLY`
3. Reconciliation and control execution (`CTRL`)
4. Promotion to `INT` and `GOLD` only when gate passes
5. Semantic KPI views in `SEM`
