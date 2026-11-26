# Data-quality report

Source: `events_sorted.parquet`

## Checks

| Check | Violations | Result |
|---|---|---|
| trans_num unique | 0 | PASS |
| trans_num is 32 lowercase hex | 0 | PASS |
| cc_num is 8-19 digits | 0 | PASS |
| amt > 0 | 0 | PASS |
| amt is whole cents | 0 | PASS |
| category known | 0 | PASS |
| coordinates in range | 0 | PASS |
| merchant not blank | 0 | PASS |
| is_fraud is 0 or 1 | 0 | PASS |
| unix_time non-negative | 0 | PASS |

## Fraud episodes

- Cards with fraud: 9798
- Median fraud transactions per card: 10.0
- Median episode span: 46.5 h
- Max fraud transactions on one card: 17

## Volume and fraud rate by month

| Month | Rows | Fraud % |
|---|---|---|
| 2024-12 | 2102 | 0.0 |
| 2025-01 | 517622 | 1.093 |
| 2025-02 | 470865 | 1.097 |
| 2025-03 | 699512 | 0.81 |
| 2025-04 | 656464 | 0.787 |
| 2025-05 | 753134 | 0.787 |
| 2025-06 | 828411 | 0.619 |
| 2025-07 | 845646 | 0.652 |
| 2025-08 | 889588 | 0.604 |
| 2025-09 | 657490 | 0.806 |
| 2025-10 | 686459 | 0.84 |
| 2025-11 | 715603 | 0.806 |
| 2025-12 | 1337394 | 0.386 |
| 2026-01 | 527055 | 1.07 |
| 2026-02 | 469517 | 1.073 |
| 2026-03 | 685978 | 0.828 |
| 2026-04 | 661477 | 0.821 |
| 2026-05 | 760063 | 0.761 |
| 2026-06 | 389214 | 0.289 |

## Legitimate-traffic category concentration by month

Categories with at least 1% of a month's legitimate rows, and the largest one.

| Month | Top category | Top share % | Categories >= 1% |
|---|---|---|---|
| 2024-12 | gas_transport | 100.0 | 1 |
| 2025-01 | gas_transport | 93.7 | 2 |
| 2025-02 | gas_transport | 70.9 | 2 |
| 2025-03 | gas_transport | 36.9 | 3 |
| 2025-04 | grocery_pos | 79.5 | 2 |
| 2025-05 | grocery_pos | 54.4 | 3 |
| 2025-06 | misc_pos | 62.9 | 4 |
| 2025-07 | shopping_net | 60.7 | 3 |
| 2025-08 | shopping_pos | 58.1 | 2 |
| 2025-09 | shopping_pos | 78.5 | 2 |
| 2025-10 | entertainment | 69.5 | 3 |
| 2025-11 | food_dining | 58.7 | 2 |
| 2025-12 | health_fitness | 49.6 | 3 |
| 2026-01 | home | 77.7 | 2 |
| 2026-02 | home | 81.3 | 2 |
| 2026-03 | kids_pets | 71.6 | 2 |
| 2026-04 | kids_pets | 73.8 | 2 |
| 2026-05 | personal_care | 89.7 | 3 |
| 2026-06 | travel | 91.3 | 2 |
