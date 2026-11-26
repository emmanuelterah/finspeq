"""Feature worker: the consumer loop that applies log events to the window store."""

from __future__ import annotations

import threading
from dataclasses import dataclass

from finspeq.application.apply_events import ApplyEvents
from finspeq.application.ports import Clock, EventSource


@dataclass(frozen=True, slots=True)
class WorkerLimits:
    """Batch size and the two waiting limits of a worker run.

    Attributes:
        batch_size: Maximum events per poll.
        idle_exit_s: After the first event, stop once this many seconds pass without events;
            None runs until stopped.
        start_timeout_s: Before the first event, stop once this many seconds pass; None waits
            until stopped.
    """

    batch_size: int
    idle_exit_s: float | None = None
    start_timeout_s: float | None = None


@dataclass(frozen=True, slots=True)
class WorkerReport:
    """Totals of one worker run and why it stopped ("signal", "idle", or "start-timeout").

    busy_s is the time from the first poll that returned events to the end of the last apply.
    """

    batches: int
    events: int
    applied: int
    skipped: int
    busy_s: float
    stopped_by: str

    @property
    def events_per_second(self) -> float:
        """Return events / busy_s, or 0.0 when no events were applied."""
        return self.events / self.busy_s if self.busy_s > 0 else 0.0


class FeatureWorker:
    """Polls batches from an event source and applies them with ApplyEvents."""

    def __init__(
        self, source: EventSource, use_case: ApplyEvents, clock: Clock, limits: WorkerLimits
    ) -> None:
        """Create a worker.

        Args:
            source: Event source, already started.
            use_case: Applies each batch.
            clock: Measures idle time.
            limits: Batch size and waiting limits.
        """
        self._source = source
        self._use_case = use_case
        self._clock = clock
        self._limits = limits

    def run(self, stop: threading.Event) -> WorkerReport:
        """Apply batches until stop is set or a waiting limit passes; return the totals.

        Each batch is acknowledged to the source after it is applied.
        """
        batches = events = applied = skipped = 0
        last_event = self._clock.now()
        first_event: float | None = None
        while not stop.is_set():
            poll_start = self._clock.now()
            batch = self._source.poll(self._limits.batch_size)
            if batch is None:
                reason = self._waiting_limit_reached(first_event, last_event)
                if reason is not None:
                    busy = 0.0 if first_event is None else last_event - first_event
                    return WorkerReport(batches, events, applied, skipped, busy, reason)
                continue
            if first_event is None:
                first_event = poll_start
            result = self._use_case.execute(batch)
            self._source.acknowledge(batch)
            batches += 1
            events += len(batch.events)
            applied += result.applied
            skipped += result.skipped
            last_event = self._clock.now()
        busy = 0.0 if first_event is None else last_event - first_event
        return WorkerReport(batches, events, applied, skipped, busy, "signal")

    def _waiting_limit_reached(self, first_event: float | None, last_event: float) -> str | None:
        """Return "start-timeout" or "idle" if the applicable waiting limit has passed."""
        if first_event is None:
            limit, reason = self._limits.start_timeout_s, "start-timeout"
        else:
            limit, reason = self._limits.idle_exit_s, "idle"
        if limit is not None and self._clock.now() - last_event >= limit:
            return reason
        return None
