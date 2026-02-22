# Claims Snapshot Schema

Columns expected in snapshot nightly drops:
- `batch_date` (YYYY-MM-DD)
- `claim_id`
- `policy_id`
- `customer_id`
- `claim_amount_incurred`
- `paid_amount_to_date`
- `reserve_amount`
- `loss_date` (YYYY-MM-DD)
- `report_date` (YYYY-MM-DD)
- `claim_status` (`OPEN|PENDING|PAID|DENIED|CLOSED`)
- `pii_class` (`NONE|LOW|MEDIUM|HIGH`)
