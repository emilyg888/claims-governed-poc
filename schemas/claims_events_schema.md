# Claims Events Schema

Columns expected in events nightly drops:
- `batch_date` (YYYY-MM-DD)
- `claim_id`
- `event_ts` (ISO 8601 timestamp)
- `event_type` (`CREATED|UPDATED|STATUS_CHANGE|PAYMENT|NOTE|ADJUSTMENT`)
- `old_status`
- `new_status`
- `amount_delta`
- `currency`
- `source_system`
- `note` (optional)
