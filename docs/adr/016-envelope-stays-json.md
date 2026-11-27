# ADR-016: The event envelope stays JSON

- **Status:** Accepted
- **Date:** 2026-11-27

## Context

Every event on the topic is a JSON envelope (`adapters/codec.py`): the transaction and, once
scored, the feature vector and decision. DESIGN open question 4 asked whether a binary
envelope (msgspec) is worth it. M6 measured encode and decode on 20,000 scored envelopes
(`notes/m6.md`, section 6).

## Decision

Keep the JSON envelope and the stdlib codec.

## Consequences

- Decoding costs 38 µs per message against a 5–9 ms window read and publish on the hot path;
  msgspec would save about 34 µs. The M4 worker limit was the producer, not decoding.
- MessagePack would save 6% of the bytes (456 vs 487) and make the topic unreadable with
  standard tools.
- If decoding ever shows in a profile, msgspec JSON is the first step: same bytes on the topic,
  typed decode about 10 times faster, no format change for consumers.
