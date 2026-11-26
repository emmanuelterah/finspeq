"""ApplyEvents: store one batch of log events in the window store."""

from __future__ import annotations

from finspeq.application.messages import ApplyResult, EventBatch
from finspeq.application.ports import Clock, Metrics, WindowWriter
from finspeq.application.watermark import Watermark

MILLISECONDS_PER_SECOND = 1_000.0


class ApplyEvents:
    """Applies event batches to a window writer and records counts, latency, and lateness.

    Late events are applied like any other; the writer prunes entries that fall outside its
    allowed lateness. Watermarks live in process memory and start empty after a restart.
    """

    def __init__(
        self, writer: WindowWriter, metrics: Metrics, clock: Clock, allowed_lateness_s: int = 0
    ) -> None:
        """Create the use case.

        Args:
            writer: Window store that receives the batches.
            metrics: Receives fq_events_applied_total, fq_events_skipped_total,
                fq_apply_latency_ms, fq_event_lateness_s, and fq_late_events_total.
            clock: Times each apply call.
            allowed_lateness_s: Distance behind a partition's newest event time beyond which
                a delivered event is counted in fq_late_events_total.
        """
        self._writer = writer
        self._metrics = metrics
        self._clock = clock
        self._allowed_lateness_s = allowed_lateness_s
        self._watermarks: dict[int, Watermark] = {}
        self._observed_offsets: dict[int, int] = {}

    def execute(self, batch: EventBatch) -> ApplyResult:
        """Record the lateness of batch's events, apply batch, and return the writer's result.

        Each offset is observed once per process: every event behind its partition's newest
        event time adds its distance in seconds to fq_event_lateness_s, and redelivered offsets
        are not observed again.

        Raises:
            PositionGapError: The batch does not continue from the stored position.
            StoreUnavailableError: The store did not respond.
        """
        labels = {"partition": str(batch.partition)}
        self._record_lateness(batch, labels)
        start = self._clock.now()
        result = self._writer.apply(batch)
        elapsed_ms = (self._clock.now() - start) * MILLISECONDS_PER_SECOND
        self._metrics.observe("fq_apply_latency_ms", elapsed_ms, labels)
        self._metrics.increment("fq_events_applied_total", result.applied, labels)
        self._metrics.increment("fq_events_skipped_total", result.skipped, labels)
        return result

    def _record_lateness(self, batch: EventBatch, labels: dict[str, str]) -> None:
        """Advance the partition's watermark over batch's new offsets and record their lateness."""
        watermark = self._watermarks.setdefault(
            batch.partition, Watermark(self._allowed_lateness_s)
        )
        observed = self._observed_offsets.get(batch.partition, -1)
        late = 0
        for event in batch.events:
            if event.position.offset <= observed:
                continue
            behind = watermark.observe(event.transaction.time)
            if behind > 0:
                self._metrics.observe("fq_event_lateness_s", float(behind), labels)
            late += watermark.is_late(behind)
        self._observed_offsets[batch.partition] = max(observed, batch.last_position.offset)
        self._metrics.increment("fq_late_events_total", late, labels)
