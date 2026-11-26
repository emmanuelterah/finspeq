# ADR-004: Redis stores raw windowed events; feature math lives in the Python domain

- **Status:** Accepted
- **Date:** 2026-11-26

## Context

Features could be computed inside Redis (Lua) or stored precomputed. Either option duplicates
feature logic between the online store and offline training, which causes training-serving skew.

## Decision

Redis stores each card's raw transactions for the window (a sorted set plus a hash). Both paths
read a `CardWindow` and compute features with the same `FeatureSet` object in Python.

## Consequences

- Feature logic exists once; skew in feature math is impossible by construction.
- Each scoring call computes features in Python (budget: about 1 ms).
- Adding a feature needs no change to Redis or Lua.
