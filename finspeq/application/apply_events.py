"""ApplyEvents: store one batch of log events in the window store."""

from __future__ import annotations

from finspeq.application.messages import ApplyResult, EventBatch
from finspeq.application.ports import Clock, Metrics, WindowWriter

MILLISECONDS_PER_SECOND = 1_000.0


class ApplyEvents:
    """Applies event batches to a window writer and records counts and latency."""

    def __init__(self, writer: WindowWriter, metrics: Metrics, clock: Clock) -> None:
        """Create the use case.

        Args:
            writer: Window store that receives the batches.
            metrics: Receives fq_events_applied_total, fq_events_skipped_total, and
                fq_apply_latency_ms.
            clock: Times each apply call.
        """
        self._writer = writer
        self._metrics = metrics
        self._clock = clock

    def execute(self, batch: EventBatch) -> ApplyResult:
        """Apply batch and return the writer's result.

        Raises:
            PositionGapError: The batch does not continue from the stored position.
            StoreUnavailableError: The store did not respond.
        """
        start = self._clock.now()
        result = self._writer.apply(batch)
        elapsed_ms = (self._clock.now() - start) * MILLISECONDS_PER_SECOND
        labels = {"partition": str(batch.partition)}
        self._metrics.observe("fq_apply_latency_ms", elapsed_ms, labels)
        self._metrics.increment("fq_events_applied_total", result.applied, labels)
        self._metrics.increment("fq_events_skipped_total", result.skipped, labels)
        return result
