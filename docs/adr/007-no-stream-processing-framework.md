# ADR-007: Hand-written consumer loops instead of a stream-processing framework

- **Status:** Accepted
- **Date:** 2026-11-26

## Context

The project's goal is to learn streaming internals: offsets, rebalances, delivery guarantees, and
event time. Frameworks hide exactly these mechanisms.

## Decision

Use `confluent-kafka` directly with manual offset handling, and implement windowing, watermarks,
and recovery in project code.

## Consequences

- Every guarantee is visible and testable in project code.
- More code to write and maintain than with a framework.
