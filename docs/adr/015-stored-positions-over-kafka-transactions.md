# ADR-015: Positions stay with the state; Kafka transactions only for Kafka-to-Kafka steps

- **Status:** Accepted
- **Date:** 2026-11-27

## Context

ADR-003 stores each partition's position in the same atomic write as the state (Redis Lua
script, Postgres transaction). Kafka transactions offer another route to exactly-once: the
produced records and the consumed offsets commit together (`send_offsets_to_transaction`). M6
built a consume-transform-produce relay in both styles and crashed each 10 times
(`notes/m6.md`, section 5).

## Decision

The feature worker and the ledger worker keep their positions in the store they write.
Kafka transactions are the method for any future step whose output is a Kafka topic.

## Consequences

- With transactions, `read_committed` consumers saw 0 extra copies; the plain relay wrote
  2,000. `read_uncommitted` consumers still saw the 10 aborted batches, so every downstream
  consumer of a transactional topic must set `isolation.level=read_committed`.
- A Kafka transaction cannot include a Redis or Postgres write, so it cannot replace ADR-003
  for the two existing workers.
- Transactions cost 10% to 48% of relay throughput at 200 events per transaction in two runs.
- Recovery for the existing workers still never depends on Kafka's committed offsets.
