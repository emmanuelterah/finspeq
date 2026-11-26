"""Tests for deterministic crash injection and its effect on the worker."""

from __future__ import annotations

import threading

import pytest

from finspeq.adapters.inbound.faults import CrashPoint, FaultInjector, FaultPlan
from finspeq.adapters.inbound.feature_worker import FeatureWorker, WorkerLimits
from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.adapters.outbound.null_metrics import NullMetrics
from finspeq.adapters.outbound.redpanda import consumer_config
from finspeq.application.apply_events import ApplyEvents
from finspeq.application.messages import EventBatch, EventPosition
from tests.factories import BASE_TIME, batch, make_tx
from tests.unit.adapters.test_feature_worker_and_cli import ListSource, StepClock


class SimulatedCrashError(Exception):
    """Raised in place of os._exit so a test can observe the state after a crash."""


def crash() -> None:
    """Raise SimulatedCrashError."""
    raise SimulatedCrashError


def no_sleep(seconds: float) -> None:
    """Fail the test: these plans must never stall."""
    raise AssertionError(f"unexpected stall of {seconds} s")


def crash_index(plan: FaultPlan, checkpoints: int) -> int | None:
    """Return the 1-based checkpoint at which plan crashes, or None within checkpoints."""
    injector = FaultInjector(plan, crash, no_sleep)
    for index in range(1, checkpoints + 1):
        try:
            injector.checkpoint(CrashPoint.AFTER_APPLY)
        except SimulatedCrashError:
            return index
    return None


def test_same_seed_crashes_at_the_same_checkpoint() -> None:
    """Crash at the same checkpoint for equal seeds."""
    plan = FaultPlan(CrashPoint.AFTER_APPLY, probability=0.1, seed=7)
    first = crash_index(plan, 500)
    assert first is not None
    assert crash_index(plan, 500) == first


def test_other_points_and_zero_probability_never_crash() -> None:
    """Never crash at a point other than the plan's, or with probability 0."""
    injector = FaultInjector(FaultPlan(CrashPoint.AFTER_POLL, 1.0, 1), crash, no_sleep)
    injector.checkpoint(CrashPoint.AFTER_APPLY)
    injector.checkpoint(CrashPoint.AFTER_ACK)
    assert injector.checkpoints == 0
    assert crash_index(FaultPlan(CrashPoint.AFTER_APPLY, 0.0, 1), 1_000) is None


def run_until_crash(point: CrashPoint) -> tuple[MemoryWindowStore, ListSource]:
    """Run a worker over one batch with a certain crash at point; return store and source."""
    store = MemoryWindowStore()
    source = ListSource([batch((0, make_tx(1, BASE_TIME)), (1, make_tx(2, BASE_TIME + 1)))])
    clock = StepClock()
    worker = FeatureWorker(
        source,
        ApplyEvents(store, NullMetrics(), clock),
        clock,
        WorkerLimits(batch_size=10, idle_exit_s=1.0),
        FaultInjector(FaultPlan(point, 1.0, 0), crash, no_sleep),
    )
    with pytest.raises(SimulatedCrashError):
        worker.run(threading.Event())
    return store, source


def test_crash_after_poll_applies_nothing() -> None:
    """Leave the store empty when the crash comes before the apply."""
    store, source = run_until_crash(CrashPoint.AFTER_POLL)
    assert store.position(0) is None
    assert source.acknowledged == []


def test_crash_after_apply_leaves_batch_applied_but_unacknowledged() -> None:
    """Keep the applied batch and its position; the group offset is never committed."""
    store, source = run_until_crash(CrashPoint.AFTER_APPLY)
    assert store.position(0) == EventPosition(0, 1)
    assert source.acknowledged == []


def test_crash_after_ack_leaves_batch_applied_and_acknowledged() -> None:
    """Apply and acknowledge the batch before the crash."""
    store, source = run_until_crash(CrashPoint.AFTER_ACK)
    assert store.position(0) == EventPosition(0, 1)
    assert [len(b.events) for b in source.acknowledged] == [2]


def test_redelivery_after_crash_changes_nothing() -> None:
    """Deliver the crashed batch again to the same store and apply none of its events."""
    store, _ = run_until_crash(CrashPoint.AFTER_APPLY)
    replay: EventBatch = batch((0, make_tx(1, BASE_TIME)), (1, make_tx(2, BASE_TIME + 1)))
    result = store.apply(replay)
    assert (result.applied, result.skipped) == (0, 2)


def test_stall_happens_once_after_the_chosen_poll() -> None:
    """Sleep stall_s once, at the AFTER_POLL checkpoint of batch stall_at_batch."""
    sleeps: list[float] = []
    injector = FaultInjector(FaultPlan(stall_s=12.0, stall_at_batch=2), crash, sleeps.append)
    for _ in range(4):
        injector.checkpoint(CrashPoint.AFTER_POLL)
        injector.checkpoint(CrashPoint.AFTER_APPLY)
    assert sleeps == [12.0]


def test_consumer_config_sets_strategy_and_poll_interval() -> None:
    """Pass the assignment strategy and the maximum poll interval through."""
    config = consumer_config("127.0.0.1:19092", "g", None, "cooperative-sticky", 10_000)
    assert config["partition.assignment.strategy"] == "cooperative-sticky"
    assert config["max.poll.interval.ms"] == 10_000
    assert consumer_config("127.0.0.1:19092", "g")["partition.assignment.strategy"] == "range"


def test_consumer_config_sets_session_timeout_and_static_membership() -> None:
    """Use a 6 s session timeout and set group.instance.id only when given."""
    dynamic = consumer_config("127.0.0.1:19092", "g")
    static = consumer_config("127.0.0.1:19092", "g", instance_id="worker-0")
    assert dynamic["session.timeout.ms"] == 6_000
    assert "group.instance.id" not in dynamic
    assert static["group.instance.id"] == "worker-0"
