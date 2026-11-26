# ADR-010: Crash testing with seeded fault injection and a supervisor

- **Status:** Accepted
- **Date:** 2026-11-26

## Context

ADR-003 claims that storing consumer positions with the derived state makes redelivery
harmless. That claim needs evidence under real process crashes, not exceptions that run cleanup
code, and a baseline that shows what goes wrong without it.

## Decision

- The worker has seeded crash points after poll, after apply, and after ack. A crash calls
  `os._exit(137)`, which skips cleanup and offset commits.
- `scripts/crash_drill.py` restarts the command until it exits cleanly and records, after every
  exit, the stored position and the committed group offset. It can SIGKILL Redis once.
- `experiments/naive_counter.py` is the baseline: `INCR` then commit, with the same crash points.
- Consumers use a 6 s session timeout and optional static membership so a restart does not wait
  for the dead member to expire.

## Consequences

- With 108 crashes after apply the worker stayed exact; the naive counter over-counted by 4.93%
  (one batch per crash). See `notes/m2.md`.
- Group offsets lag stored positions after crashes (up to 150 events here) and must not drive
  recovery.
- Drills are reproducible from a seed and a single script, `scripts/m2_drills.sh`.
