# ADR-013: One writer per card; staleness on the hot path is measured, not removed

- **Status:** Accepted
- **Date:** 2026-11-27

## Context

The scoring API reads a card's window from Redis, while the feature worker writes it from the
log. A transaction that arrives before the worker has applied the card's previous one is
scored without it. Two options were built and measured (`notes/m5.md`, section 3): a single
writer (the worker), and read-your-writes, where the API also writes each event to Redis after
the log acknowledges it.

## Decision

- The feature worker stays the only writer of window state. The API reads.
- Read-your-writes stays available behind `--read-your-writes`; it is off by default.
- Staleness is measured: the load generator reports, per request, the card's earlier events
  missing from the window, and the model is not retrained for it.

## Consequences

- In the data, under 0.05% of transactions follow their card's previous one within 1 s; the
  worker applies events in about half a second, so staleness touches a tiny share of traffic.
- In 500 ms bursts, 5.1% of reads missed one event with a single writer and none with
  read-your-writes; at 10–250 ms gaps read-your-writes still left 15–30% stale, because the
  previous request had not finished.
- Read-your-writes would add 3.2 ms (p50) per request and a second writer per card. Its
  correctness is covered by the WindowAppender contract tests if it is switched on.
- A card-testing attack that fires transactions within milliseconds is the case this choice
  handles worst; it would need its own velocity check rather than a faster window.
