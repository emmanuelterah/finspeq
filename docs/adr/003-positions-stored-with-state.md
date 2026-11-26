# ADR-003: Consumer positions are stored with derived state

- **Status:** Accepted
- **Date:** 2026-11-26

## Context

At-least-once delivery redelivers events after a crash. If the consumer's position lives in one
system (Kafka group offsets) and its state in another (Redis), a crash between the two writes
double-applies or loses events.

## Decision

Each consumer stores its last applied offset per partition in the same store and the same atomic
operation as its state: a Lua script in Redis, a SQL transaction in Postgres. On assignment the
consumer seeks to the stored position. Kafka group offsets are committed only for lag metrics.

## Consequences

- Redelivered events at or below the stored position are skipped; data loss in the store rolls
  state and position back together.
- Offset gaps are detected and rejected.
- Consumers must implement seek-on-assign instead of relying on group offsets.
