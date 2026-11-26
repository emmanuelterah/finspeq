# ADR-009: Drop the category feature (FEATURE_SET_V2)

- **Status:** Accepted
- **Date:** 2026-11-26

## Context

In the dataset, legitimate traffic moves through merchant categories in time blocks: each month's
legitimate rows come from one to four categories (for example gas_transport in January 2025,
travel in June 2026). Fraud uses the same four categories (grocery_pos, shopping_net, misc_net,
shopping_pos) throughout. See `notes/data_quality.md`.

With `category_code` as a feature (v1), the model gave it 88% of its gain. On a January 2025 split
(train before Jan 12, valid Jan 12-21, test Jan 22-31), legitimate grocery_net traffic that began
during the test period scored about 0.99:

| | v1 (with category) | v2 (without) |
|---|---|---|
| Test PR-AUC | 0.822 | 0.941 |
| Test FPR at the 1%-FPR review threshold | 20.2% | 1.03% |
| Test FPR at the 0.1%-FPR block threshold | 19.6% | 0.09% |

## Decision

Use FEATURE_SET_V2 as the default: v1 without `category_code`, plus `amount_to_max_24h` (the
amount relative to the card's largest amount in the previous 24 hours). Keep v1 registered for
comparison.

## Consequences

- Validation thresholds transfer to the test period.
- The model cannot use category, a real fraud signal in production data; on this dataset that
  signal is inseparable from the generation artifact.
- Raw amount still shifts with the category mix; per-month test metrics must be reported.
