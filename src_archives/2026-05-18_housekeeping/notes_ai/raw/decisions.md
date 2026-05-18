# Decisions

- Control definitions are declarative in YAML and persisted into Snowflake evidence tables.
- Promotion gate is fail-closed: any critical control failure blocks RAW -> INT -> GOLD.
- Local artifacts are retained for operational debugging and reproducibility.
