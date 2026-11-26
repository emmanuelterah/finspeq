# ADR-011: Allowed lateness of one hour; late events are applied, not dropped

- **Status:** Accepted
- **Date:** 2026-11-26

## Context

Events can reach the worker after events with a later event time. The store prunes each card's
entries older than the card's newest event time minus 24 h and the allowed lateness. A late
event's window therefore misses entries in two cases:

1. The entries were pruned. More allowed lateness keeps them, at the cost of memory.
2. Earlier events of the same card have not arrived yet. No allowed lateness helps; only
   waiting before scoring would.

`notes/m3.md` measures both on the M1 slice (115,720 events, 956 frauds) under two seeded
disorder profiles, scoring every event at arrival with the M0 model.

## Decision

- Allowed lateness is **3,600 s** by default for the worker, `verify-replay`, and
  `verify-state` (`--lateness-s`). Stores keep 24 h + 1 h of entries per card (4.2% more).
- Late events are **applied like any other event**. They are never dropped and there is no side
  topic. A late event's own entry is kept unless it is already older than the pruning boundary.
- The worker counts, per partition, events behind the newest event time
  (`fq_event_lateness_s`) and events beyond the allowed lateness (`fq_late_events_total`).

## Consequences

- With mild disorder (10% delayed, mean 5 min) 600 s already removes all pruning error; 1 h
  covers it with margin. With heavy disorder (20% delayed, mean 1 h, cap 24 h) 1 h removes 67%
  of the pruning error; 6 h removes all of it for 25% more memory and the same decisions as 1 h.
- Most error comes from events that have not arrived yet: 6,047 of the 7,120 mismatched vectors
  (85%) under heavy disorder with no allowed lateness. Reducing it needs a hold-before-scoring buffer or
  re-scoring, which is outside the scoring design (ADR-002).
- Dropping late events would remove their entries from later windows and add error; a side
  topic adds a second write path with no consumer. Neither was chosen.
- Lateness is measured against the partition's newest time, but pruning works per card. The
  partition count overstates harm: 22,435 events were behind the partition, 4,590 behind
  their own card.
- Replay equality is unaffected: a disordered stream with duplicates, applied by a crashing
  worker, matched an in-memory replay of the same arrival order with 0 mismatches.
