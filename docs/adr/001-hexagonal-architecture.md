# ADR-001: Hexagonal architecture with Protocol ports

- **Status:** Accepted
- **Date:** 2026-11-26

## Context

Finspeq runs the same feature logic offline (training) and online (scoring), against different
storage and I/O: Parquet and memory offline; Redis, Redpanda, and Postgres online. The core logic
must be testable without containers and must not change when infrastructure changes.

## Decision

Split the code into rings: domain, application, adapters, bootstrap. Imports point inward only.
Ports are `typing.Protocol` classes owned by the application ring. Import-linter enforces the
rings, and forbids third-party imports in the domain and application rings.

## Consequences

- The domain and use cases run in unit tests with memory adapters, in milliseconds.
- Every port has a contract suite that every adapter must pass.
- Composition happens in `bootstrap.py` only, which adds some wiring code.
