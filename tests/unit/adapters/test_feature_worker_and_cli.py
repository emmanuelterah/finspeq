"""Tests for the feature worker loop and the command-line arguments."""

from __future__ import annotations

import subprocess
import sys
import threading

import pytest

from finspeq.adapters.inbound import offline_cli, stream_cli
from finspeq.adapters.inbound.feature_worker import FeatureWorker, WorkerLimits
from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.adapters.outbound.null_metrics import NullMetrics
from finspeq.application.apply_events import ApplyEvents
from finspeq.application.disorder import NO_DISORDER, Disorder
from finspeq.application.messages import EventBatch, EventPosition
from finspeq.application.replay import CardSlice
from tests.factories import BASE_TIME, batch, make_tx


class ListSource:
    """EventSource that returns preset batches, then None, and records acknowledgements."""

    def __init__(self, batches: list[EventBatch]) -> None:
        """Create a source over batches."""
        self._batches = batches
        self.acknowledged: list[EventBatch] = []

    def poll(self, max_events: int) -> EventBatch | None:
        """Return the next batch, or None when none are left; check the size limit."""
        if not self._batches:
            return None
        assert len(self._batches[0].events) <= max_events
        return self._batches.pop(0)

    def seek(self, position: EventPosition) -> None:
        """Do nothing."""

    def acknowledge(self, batch: EventBatch) -> None:
        """Record the batch."""
        self.acknowledged.append(batch)


class StepClock:
    """Clock that advances one second per call."""

    def __init__(self) -> None:
        """Start at time zero."""
        self._time = 0.0

    def now(self) -> float:
        """Return the time and advance it by one second."""
        self._time += 1.0
        return self._time


def test_worker_applies_acknowledges_and_stops_when_idle() -> None:
    """Apply two batches, including one redelivery, then stop after the idle limit."""
    store = MemoryWindowStore()
    first = batch((0, make_tx(1, BASE_TIME)), (1, make_tx(2, BASE_TIME + 1)))
    source = ListSource([first, first])
    clock = StepClock()
    limits = WorkerLimits(batch_size=500, idle_exit_s=3.0)
    worker = FeatureWorker(source, ApplyEvents(store, NullMetrics(), clock), clock, limits)
    report = worker.run(threading.Event())
    assert (report.batches, report.events, report.applied, report.skipped) == (2, 4, 2, 2)
    assert report.stopped_by == "idle"
    assert report.busy_s > 0
    assert report.events_per_second == report.events / report.busy_s
    assert (report.behind, report.poll_s, report.apply_s) == (0, 2.0, 6.0)
    assert report.apply_p50_ms == report.apply_p99_ms == 3_000.0
    assert source.acknowledged == [first, first]
    assert store.position(0) == EventPosition(0, 1)


def test_worker_stops_on_signal() -> None:
    """Return immediately when the stop event is already set."""
    stop = threading.Event()
    stop.set()
    clock = StepClock()
    worker = FeatureWorker(
        ListSource([]),
        ApplyEvents(MemoryWindowStore(), NullMetrics(), clock),
        clock,
        WorkerLimits(batch_size=10),
    )
    assert worker.run(stop).stopped_by == "signal"


def test_idle_limit_applies_only_after_the_first_event() -> None:
    """Keep waiting past idle_exit_s before the first event; stop at the start timeout."""
    clock = StepClock()
    use_case = ApplyEvents(MemoryWindowStore(), NullMetrics(), clock)
    limits = WorkerLimits(batch_size=10, idle_exit_s=2.0, start_timeout_s=6.0)
    report = FeatureWorker(ListSource([]), use_case, clock, limits).run(threading.Event())
    assert report.stopped_by == "start-timeout"
    assert report.events == 0


def test_worker_leaves_after_max_batches() -> None:
    """Stop with "max-batches" once the batch limit is reached, leaving later batches."""
    clock = StepClock()
    source = ListSource([batch((n, make_tx(n + 1, BASE_TIME + n))) for n in range(3)])
    use_case = ApplyEvents(MemoryWindowStore(), NullMetrics(), clock)
    limits = WorkerLimits(batch_size=10, max_batches=2)
    report = FeatureWorker(source, use_case, clock, limits).run(threading.Event())
    assert (report.batches, report.stopped_by) == (2, "max-batches")


def test_simulate_arguments() -> None:
    """Parse the time range, slice, and rate of simulate."""
    command = stream_cli.parse(
        [
            "simulate",
            "--from-date",
            "2026-03-01",
            "--to-date",
            "2026-03-02",
            "--slice",
            "20:0",
            "--rate",
            "2000",
        ]
    )
    assert isinstance(command, stream_cli.SimulateCommand)
    assert command.selection.cards == CardSlice(20, 0)
    assert command.selection.time_range is not None
    assert command.selection.time_range.end.epoch_s - command.selection.time_range.start.epoch_s
    assert command.rate == 2000.0


def test_worker_and_verify_arguments() -> None:
    """Parse worker and verify-state commands with defaults."""
    worker = stream_cli.parse(["worker", "--idle-exit-s", "5"])
    assert isinstance(worker, stream_cli.WorkerCommand)
    assert (worker.threads, worker.max_batches, worker.assignment_strategy) == (1, None, "range")
    assert worker.idle_exit_s == 5.0
    assert worker.start_timeout_s == 60.0
    state = stream_cli.parse(["verify-state", "--partitions", "4"])
    assert isinstance(state, stream_cli.VerifyStateCommand)
    assert state.selection.time_range is None
    assert state.partitions == 4
    assert state.disorder == NO_DISORDER
    assert worker.lateness_s == state.lateness_s == stream_cli.DEFAULT_LATENESS_S == 3_600


def test_disorder_and_lateness_sweep_arguments() -> None:
    """Parse disorder flags on simulate and the lateness list of lateness-sweep."""
    flags = ["--delayed-fraction", "0.2", "--mean-delay-s", "60", "--max-delay-s", "600"]
    flags += ["--duplicate-fraction", "0.01", "--disorder-seed", "9"]
    simulate = stream_cli.parse(["simulate", *flags])
    assert isinstance(simulate, stream_cli.SimulateCommand)
    assert simulate.disorder == Disorder(0.2, 60.0, 600, 0.01, 9)
    sweep = offline_cli.parse(["lateness-sweep", "--lateness", "0,600", *flags])
    assert isinstance(sweep, offline_cli.LatenessSweepCommand)
    assert sweep.lateness_values == (0, 600)
    assert sweep.disorder == simulate.disorder
    assert sweep.model_dir is None


OFFLINE_ONLY = ("lightgbm", "onnxruntime", "onnxmltools", "sklearn")
STREAMING_ONLY = ("confluent_kafka", "redis", "prometheus_client")
TRAINING_ONLY = ("lightgbm", "onnxmltools", "sklearn")
SERVING_ONLY = ("fastapi", "uvicorn")


@pytest.mark.parametrize(
    ("root", "absent"),
    [
        ("finspeq.bootstrap.offline", STREAMING_ONLY + SERVING_ONLY),
        ("finspeq.bootstrap.streaming", OFFLINE_ONLY + SERVING_ONLY),
        ("finspeq.bootstrap.serving", TRAINING_ONLY),
    ],
)
def test_composition_roots_load_only_their_libraries(root: str, absent: tuple[str, ...]) -> None:
    """Import each composition root alone and find none of the other process's libraries."""
    code = f"import sys, {root}; print(sorted(m for m in {absent!r} if m in sys.modules))"
    output = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout
    assert output.strip() == "[]"
