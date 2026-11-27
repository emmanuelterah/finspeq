"""Tests for the ApplyEvents use case."""

from __future__ import annotations

from collections.abc import Mapping

from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.application.apply_events import ApplyEvents
from tests.factories import BASE_TIME, batch, make_tx


class RecordingMetrics:
    """Metrics port that stores every call."""

    def __init__(self) -> None:
        """Create empty call lists."""
        self.observed: list[tuple[str, float]] = []
        self.counters: dict[str, float] = {}

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        """Record a histogram sample."""
        assert labels == {"partition": "0"}
        self.observed.append((name, value))

    def increment(
        self, name: str, amount: float = 1.0, labels: Mapping[str, str] | None = None
    ) -> None:
        """Add amount to the named counter."""
        assert labels == {"partition": "0"}
        self.counters[name] = self.counters.get(name, 0.0) + amount

    def set_gauge(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        """Ignore gauges."""


class TickClock:
    """Clock that advances 2 ms per call."""

    def __init__(self) -> None:
        """Start at time zero."""
        self._now = 0.0

    def now(self) -> float:
        """Return the current time and advance it by 2 ms."""
        current = self._now
        self._now += 0.002
        return current


def test_apply_records_counts_and_latency() -> None:
    """Apply a batch with one duplicate delivery and record counters and a 2 ms latency."""
    metrics = RecordingMetrics()
    use_case = ApplyEvents(MemoryWindowStore(), metrics, TickClock())
    events = batch((0, make_tx(1, BASE_TIME)), (1, make_tx(2, BASE_TIME + 1)))
    use_case.execute(events)
    result = use_case.execute(events)
    assert (result.applied, result.skipped) == (0, 2)
    assert metrics.counters == {
        "fq_events_applied_total": 2.0,
        "fq_events_skipped_total": 2.0,
        "fq_events_behind_card_total": 0.0,
        "fq_late_events_total": 0.0,
    }
    assert metrics.observed[0][0] == "fq_apply_latency_ms"
    assert abs(metrics.observed[0][1] - 2.0) < 1e-9


def test_apply_counts_events_behind_the_watermark() -> None:
    """Record each event's distance behind the newest time; count those beyond the lateness."""
    metrics = RecordingMetrics()
    use_case = ApplyEvents(MemoryWindowStore(), metrics, TickClock(), allowed_lateness_s=60)
    use_case.execute(batch((0, make_tx(1, BASE_TIME + 100)), (1, make_tx(2, BASE_TIME + 50))))
    use_case.execute(batch((2, make_tx(3, BASE_TIME + 30)), (3, make_tx(4, BASE_TIME + 200))))
    lateness = [value for name, value in metrics.observed if name == "fq_event_lateness_s"]
    assert lateness == [50.0, 70.0]
    assert metrics.counters["fq_late_events_total"] == 1.0
    assert metrics.counters["fq_events_applied_total"] == 4.0
