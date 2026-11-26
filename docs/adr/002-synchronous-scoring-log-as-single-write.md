# ADR-002: Synchronous scoring; the event log is the hot path's only write

- **Status:** Accepted
- **Date:** 2026-11-26

## Context

A payment decision must be returned before authorization completes. The decision also has to
reach the event log, the feature store, and the decision ledger. Writing to several systems
without a shared transaction can leave them disagreeing (the dual-write problem).

## Decision

The scoring API reads features, scores, publishes one event to Redpanda, waits for the broker
acknowledgement, and then responds. Redis and Postgres are updated by consumers of that event.

## Consequences

- One write per decision; no partial outcome is visible to the caller.
- Hot-path latency includes the broker acknowledgement.
- Feature freshness lags by the consumer delay; this staleness is measured in M5.
