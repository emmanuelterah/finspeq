# ADR-005: Money as integer cents

- **Status:** Accepted
- **Date:** 2026-11-26

## Context

Floating-point sums depend on the order of addition, so replaying the same events in a different
grouping can change spend features in the last bits.

## Decision

The domain stores amounts as positive integer cents. Sums are integer arithmetic. Ratios and
dollar values are computed once, from integers, at the feature boundary.

## Consequences

- Spend aggregates are exact and independent of evaluation order.
- Adapters convert source amounts to cents and reject values that are not whole cents.
