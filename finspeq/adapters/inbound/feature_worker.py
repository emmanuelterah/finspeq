"""Feature worker: the consumer loop that applies log events to the window store."""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass

from finspeq.adapters.inbound.faults import NO_FAULTS, CrashPoint, FaultInjector
from finspeq.application.apply_events import ApplyEvents
from finspeq.application.messages import ApplyResult
from finspeq.application.ports import Clock, EventSource

MILLISECONDS_PER_SECOND = 1_000.0


@dataclass(frozen=True, slots=True)
class WorkerLimits:
    """Batch size and the two waiting limits of a worker run.

    Attributes:
        batch_size: Maximum events per poll.
        idle_exit_s: After the first event, stop once this many seconds pass without events;
            None runs until stopped.
        start_timeout_s: Before the first event, stop once this many seconds pass; None waits
            until stopped.
        max_batches: Stop after applying this many batches; None has no limit.
    """

    batch_size: int
    idle_exit_s: float | None = None
    start_timeout_s: float | None = None
    max_batches: int | None = None


@dataclass(frozen=True, slots=True)
class WorkerReport:
    """Totals of one worker run and why it stopped.

    stopped_by is "signal", "idle", "start-timeout", or "max-batches".

    Attributes:
        batches: Batches applied.
        events: Events in those batches.
        applied: Events the store stored.
        skipped: Events the store skipped (already applied, or a repeated tx_id).
        behind: Stored events earlier than their card's newest event time.
        busy_s: Time from the first poll that returned events to the end of the last apply.
        poll_s: Time spent in polls that returned events (consume and decode).
        apply_s: Time spent applying batches.
        apply_p50_ms: Median apply time of one batch, in milliseconds.
        apply_p99_ms: 99th-percentile apply time of one batch, in milliseconds.
        stopped_by: Why the run ended.
    """

    batches: int
    events: int
    applied: int
    skipped: int
    behind: int
    busy_s: float
    poll_s: float
    apply_s: float
    apply_p50_ms: float
    apply_p99_ms: float
    stopped_by: str

    @property
    def events_per_second(self) -> float:
        """Return events / busy_s, or 0.0 when no events were applied."""
        return self.events / self.busy_s if self.busy_s > 0 else 0.0


class FeatureWorker:
    """Polls batches from an event source and applies them with ApplyEvents."""

    def __init__(
        self,
        source: EventSource,
        use_case: ApplyEvents,
        clock: Clock,
        limits: WorkerLimits,
        faults: FaultInjector | None = None,
    ) -> None:
        """Create a worker.

        Args:
            source: Event source, already started.
            use_case: Applies each batch.
            clock: Measures idle time and the time spent polling and applying.
            limits: Batch size and waiting limits.
            faults: Crash and stall injector called after each poll, apply, and
                acknowledgement; None injects nothing.
        """
        self._source = source
        self._use_case = use_case
        self._clock = clock
        self._limits = limits
        self._faults = faults or FaultInjector(NO_FAULTS, _never_called, _never_sleeps)

    def run(self, stop: threading.Event) -> WorkerReport:
        """Apply batches until stop is set or a waiting limit passes; return the totals.

        Each batch is acknowledged to the source after it is applied. The fault injector's
        checkpoints run after a poll that returned events, after the apply, and after the
        acknowledgement.
        """
        tally = _Tally(self._clock.now())
        while not stop.is_set():
            poll_start = self._clock.now()
            batch = self._source.poll(self._limits.batch_size)
            if batch is None:
                reason = self._waiting_limit_reached(tally.first_event, tally.last_event)
                if reason is not None:
                    return tally.report(reason)
                continue
            tally.start(poll_start, self._clock.now() - poll_start)
            self._faults.checkpoint(CrashPoint.AFTER_POLL)
            apply_start = self._clock.now()
            result = self._use_case.execute(batch)
            apply_end = self._clock.now()
            self._faults.checkpoint(CrashPoint.AFTER_APPLY)
            self._source.acknowledge(batch)
            self._faults.checkpoint(CrashPoint.AFTER_ACK)
            tally.add(len(batch.events), result, apply_end - apply_start)
            tally.last_event = self._clock.now()
            if tally.batches == self._limits.max_batches:
                return tally.report("max-batches")
        return tally.report("signal")

    def _waiting_limit_reached(self, first_event: float | None, last_event: float) -> str | None:
        """Return "start-timeout" or "idle" if the applicable waiting limit has passed."""
        if first_event is None:
            limit, reason = self._limits.start_timeout_s, "start-timeout"
        else:
            limit, reason = self._limits.idle_exit_s, "idle"
        if limit is not None and self._clock.now() - last_event >= limit:
            return reason
        return None


class _Tally:
    """Running totals of one worker run."""

    def __init__(self, started: float) -> None:
        """Start with zero totals; started is the run's start time on the worker clock."""
        self.first_event: float | None = None
        self.last_event = started
        self._counts = {"batches": 0, "events": 0, "applied": 0, "skipped": 0, "behind": 0}
        self._poll_s = 0.0
        self._apply_times: list[float] = []

    @property
    def batches(self) -> int:
        """Return the number of batches applied so far."""
        return self._counts["batches"]

    def start(self, poll_start: float, poll_s: float) -> None:
        """Record a poll that returned events: its start (if first) and its duration."""
        if self.first_event is None:
            self.first_event = poll_start
        self._poll_s += poll_s

    def add(self, events: int, result: ApplyResult, apply_s: float) -> None:
        """Add one applied batch of events with the store's result and the apply duration."""
        self._counts["batches"] += 1
        self._counts["events"] += events
        self._counts["applied"] += result.applied
        self._counts["skipped"] += result.skipped
        self._counts["behind"] += result.behind
        self._apply_times.append(apply_s)

    def report(self, stopped_by: str) -> WorkerReport:
        """Return the totals as a WorkerReport that ends for stopped_by."""
        busy = 0.0 if self.first_event is None else self.last_event - self.first_event
        counts = self._counts
        return WorkerReport(
            batches=counts["batches"],
            events=counts["events"],
            applied=counts["applied"],
            skipped=counts["skipped"],
            behind=counts["behind"],
            busy_s=busy,
            poll_s=self._poll_s,
            apply_s=sum(self._apply_times),
            apply_p50_ms=_percentile_ms(self._apply_times, 0.50),
            apply_p99_ms=_percentile_ms(self._apply_times, 0.99),
            stopped_by=stopped_by,
        )


def _percentile_ms(seconds: list[float], fraction: float) -> float:
    """Return the nearest-rank fraction percentile of seconds in milliseconds, or 0.0 if empty."""
    if not seconds:
        return 0.0
    ordered = sorted(seconds)
    rank = max(1, math.ceil(fraction * len(ordered)))
    return ordered[rank - 1] * MILLISECONDS_PER_SECOND


def _never_sleeps(_seconds: float) -> None:
    """Sleep callback of the default injector, whose plan never stalls."""


def _never_called() -> None:
    """Crash callback of the default injector, whose plan never crashes."""
