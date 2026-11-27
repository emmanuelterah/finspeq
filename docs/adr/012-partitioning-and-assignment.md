# ADR-012: Keyed partitioning is required; recovery never depends on partition ownership

- **Status:** Accepted
- **Date:** 2026-11-26

## Context

M4 scaled the feature worker to several processes in one consumer group. Three questions
needed answers from data: what happens when events are not keyed by card, which assignment
strategy to use, and whether rebalances or stalled members (zombies) can corrupt state.
`notes/m4.md` has the measurements.

## Decision

- Events are always keyed by card number and partitioned with Kafka's murmur2. The codec
  already rejects envelopes whose key is not the card; round-robin publishing exists only as an
  experiment flag on the simulator.
- The default assignment strategy stays eager `range`. `cooperative-sticky` is supported
  (`--assignment-strategy`) for deployments where stopping every partition matters more than
  how long one handoff takes.
- Recovery never depends on ownership: a new owner starts after the position stored in Redis,
  and a zombie's late write is skipped by the same position check (ADR-003). No fencing or
  generation check is added.

## Consequences

- Without a key, 67% of events reached Redis behind a later event of their card, 67% of
  feature vectors scored on arrival were wrong, and 89 fewer frauds were blocked, while the
  final state still matched a replay. Final-state checks alone cannot catch ordering bugs; the
  store's `behind` count can.
- Eager rebalances paused all partitions for a median 0.09 s (max 0.76 s); cooperative ones
  paused only moving partitions, but for about 2 s. Both gave 0 mismatches.
- A worker stalled past `max.poll.interval.ms` applied a 200-event batch that its peer had
  already applied: all 200 were skipped and verify-state found 0 mismatches.
- Throughput rises with partitions and workers (24,063 events/s with 8 and 8 on the machine),
  but the end-to-end limit there is the durable producer at about 6,000 events/s.
