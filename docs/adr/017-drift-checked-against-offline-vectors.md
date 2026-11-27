# ADR-017: Drift is measured by PSI, and live drift is checked against offline vectors

- **Status:** Accepted
- **Date:** 2026-11-27

## Context

M6 measured PSI per feature against a TRAIN reference, monthly over the dataset and daily over
the ledger's recorded vectors (`notes/m6.md`, sections 2 and 3). The ledger alerted on
`secs_since_last` and the 1 h counts every day; the dataset, for the same months, did not.
Joining the ledger with the offline vectors of the same events showed 15.4% of live vectors
differ, matching the measured stale-read share.

## Decision

Drift is PSI with TRAIN decile bins and an alert above 0.2, about 60 lines in the application
layer, no monitoring library. A live drift alert is read next to the offline PSI of the same
events (`scripts/ledger_skew.py`): equal PSI means the data changed; PSI only in the live
vectors means the serving path computed different features.

## Consequences

- `hour_of_day_utc` alerts in most months because the data's hour mix really changes, while
  recall stays near 93%. A PSI alert starts an investigation; it does not trigger retraining.
- The skew check needs the offline features of the same events, which exist here because the
  pipeline is deterministic and replayable; a system without replay could not separate drift
  from skew this way.
- Recall needs labels, and labels arrive 7–60 days late: on any day only 15–19% of the last 30
  days' frauds are labelled (section 4). Drift is the early signal; recall confirms it weeks
  later.
