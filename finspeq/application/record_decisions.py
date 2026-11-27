"""RecordDecisions: store the scored events of one batch in the decision ledger."""

from __future__ import annotations

from finspeq.application.messages import ApplyResult, EventBatch
from finspeq.application.ports import Clock, DecisionLedger, Metrics

MILLISECONDS_PER_SECOND = 1_000.0


class RecordDecisions:
    """Records event batches in a decision ledger and records counts and latency."""

    def __init__(self, ledger: DecisionLedger, metrics: Metrics, clock: Clock) -> None:
        """Create the use case.

        Args:
            ledger: Receives the batches.
            metrics: Receives fq_ledger_rows_total, fq_ledger_skipped_total, and
                fq_ledger_latency_ms.
            clock: Times each record call.
        """
        self._ledger = ledger
        self._metrics = metrics
        self._clock = clock

    def execute(self, batch: EventBatch) -> ApplyResult:
        """Record batch and return the ledger's result.

        Raises:
            PositionGapError: The batch does not continue from the stored position.
            StoreUnavailableError: The database did not respond.
        """
        start = self._clock.now()
        result = self._ledger.record(batch)
        elapsed_ms = (self._clock.now() - start) * MILLISECONDS_PER_SECOND
        labels = {"partition": str(batch.partition)}
        self._metrics.observe("fq_ledger_latency_ms", elapsed_ms, labels)
        self._metrics.increment("fq_ledger_rows_total", result.applied, labels)
        self._metrics.increment("fq_ledger_skipped_total", result.skipped, labels)
        return result
