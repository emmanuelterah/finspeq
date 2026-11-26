# ADR-008: Manual composition roots instead of a dependency-injection framework

- **Status:** Accepted
- **Date:** 2026-11-26

## Context

Use cases receive their ports through constructors. A container library could wire them.

## Decision

Wire adapters into use cases by hand in `finspeq/bootstrap/`, one module per process.

## Consequences

- Wiring is plain, type-checked Python that is easy to read and debug.
- Wiring changes are edits to the composition root of the affected process.
