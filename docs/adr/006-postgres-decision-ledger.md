# ADR-006: Postgres holds the decision ledger

- **Status:** Accepted
- **Date:** 2026-11-26

## Context

Decisions need a durable, queryable audit record with exactly one row per transaction, even when
events are redelivered.

## Decision

A ledger consumer writes decisions to Postgres with `UNIQUE (tx_id)` and
`INSERT ... ON CONFLICT DO NOTHING`, storing its partition position in the same transaction.

## Consequences

- Full ACID guarantees for the audit record; duplicates are impossible.
- One more container and consumer to operate.
