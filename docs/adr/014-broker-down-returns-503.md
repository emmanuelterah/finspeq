# ADR-014: When the broker is down, the API answers 503

- **Status:** Accepted
- **Date:** 2026-11-27

## Context

ADR-002 makes the log the only write on the hot path: a decision exists only if its event is
in the log. When Redpanda is unavailable, the API must either refuse (503) or answer REVIEW and
keep the event somewhere else to replay later. M5 stopped Redpanda for 10 s at 200 requests/s
(`notes/m5.md`, section 4).

## Decision

The API answers 503 with `publish_failed` when the broker does not acknowledge within the
publish timeout (5 s). There is no local fallback log and no REVIEW stand-in.

## Consequences

- 80 of 18,000 requests (0.44%) got 503; the rest waited and succeeded once the broker
  returned. No decision left the system without a log entry.
- A fallback log would be a second write path that can disagree with the first, which ADR-002
  exists to avoid. The caller (the payment switch) applies its own stand-in rules on 503.
- While the broker is down, every thread waits up to 5 s and new requests queue; latency
  reached 15 s. A circuit breaker that fails fast after the first time-outs would bound that
  wait; it is not built yet.
