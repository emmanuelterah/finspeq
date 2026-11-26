"""Composition root of the streaming processes: simulator, feature worker, verification."""

from __future__ import annotations

import signal
import sys
import threading
import time
from collections.abc import Callable, Iterator, Sequence

import redis
from confluent_kafka import Consumer, Producer

from finspeq.adapters.inbound import stream_cli
from finspeq.adapters.inbound.feature_worker import FeatureWorker, WorkerLimits, WorkerReport
from finspeq.adapters.inbound.output import render
from finspeq.adapters.outbound.clock import SystemClock
from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.adapters.outbound.null_metrics import NullMetrics
from finspeq.adapters.outbound.parquet import ParquetHistoricalSource
from finspeq.adapters.outbound.prometheus import PrometheusMetrics
from finspeq.adapters.outbound.redis_window import RedisWindowStore
from finspeq.adapters.outbound.redpanda import (
    RedpandaPublisher,
    RedpandaSource,
    consumer_config,
    producer_config,
)
from finspeq.application.apply_events import ApplyEvents
from finspeq.application.messages import LabeledTransaction
from finspeq.application.ports import Metrics
from finspeq.application.replay import partitioned_batches
from finspeq.application.simulate import ReplayHistory, SimulationPlan, SimulationReport
from finspeq.application.verify import (
    ReplayReport,
    ReplayScenario,
    StateReport,
    VerifyFinalState,
    VerifyReplay,
)

PUBLISH_TIMEOUT_S = 30.0
POLL_TIMEOUT_S = 0.5
VERIFY_TOPIC = "verify-replay"
STATE_BATCH_SIZE = 500


def run_simulate(command: stream_cli.SimulateCommand) -> SimulationReport:
    """Publish the selected history to the topic and return the achieved rate."""
    producer = Producer(producer_config(command.bootstrap, command.linger_ms))
    publisher = RedpandaPublisher(producer, command.topic, PUBLISH_TIMEOUT_S)
    selection = command.selection
    plan = SimulationPlan(
        selection.time_range, selection.cards, command.limit, command.rate, command.chunk_size
    )
    source = ParquetHistoricalSource(selection.source)
    return ReplayHistory(source, publisher, SystemClock(), time.sleep).execute(plan)


def run_worker(command: stream_cli.WorkerCommand) -> WorkerReport:
    """Run the feature worker until Ctrl+C, SIGTERM, or the idle limit; return its totals."""
    store = _redis_store(command.redis_url, command.topic, command.lateness_s)
    metrics: Metrics = NullMetrics()
    if command.metrics_port is not None:
        prometheus = PrometheusMetrics()
        prometheus.serve(command.metrics_port)
        metrics = prometheus
    consumer = Consumer(consumer_config(command.bootstrap, command.group))
    source = RedpandaSource(consumer, command.topic, store.position, POLL_TIMEOUT_S)
    source.start()
    clock = SystemClock()
    limits = WorkerLimits(command.batch_size, command.idle_exit_s, command.start_timeout_s)
    worker = FeatureWorker(source, ApplyEvents(store, metrics, clock), clock, limits)
    stop = threading.Event()
    _stop_on_signals(stop)
    try:
        return worker.run(stop)
    finally:
        source.close()


def run_verify_replay(command: stream_cli.VerifyReplayCommand) -> ReplayReport:
    """Replay the selection into Redis and into memory in process, and compare them.

    Raises:
        SystemExit: The Redis database holds keys and --flush was not given.
    """
    client = _redis_client(command.redis_url)
    if command.flush:
        client.flushdb()
    elif client.dbsize() > 0:
        raise SystemExit(f"{command.redis_url} is not empty; pass --flush to empty it")
    candidate = RedisWindowStore(client, VERIFY_TOPIC, command.lateness_s)
    batches = partitioned_batches(
        _selected_items(command.selection), command.partitions, command.batch_size
    )
    scenario = ReplayScenario(redeliver_every=command.redeliver_every)
    return VerifyReplay(MemoryWindowStore(command.lateness_s), candidate).execute(batches, scenario)


def run_verify_state(command: stream_cli.VerifyStateCommand) -> StateReport:
    """Compare the worker-filled Redis store with an in-process replay of the selection."""
    candidate = _redis_store(command.redis_url, command.topic, command.lateness_s)
    use_case = VerifyFinalState(
        MemoryWindowStore(command.lateness_s), candidate, command.partitions
    )
    return use_case.execute(_selected_items(command.selection), STATE_BATCH_SIZE)


def _selected_items(selection: stream_cli.EventSelection) -> Iterator[LabeledTransaction]:
    """Yield the selection's historical transactions in canonical order."""
    for item in ParquetHistoricalSource(selection.source).iter_labeled(selection.time_range):
        if selection.cards.contains(item.transaction.card):
            yield item


def _redis_client(url: str) -> redis.Redis:
    """Return a Redis client for url with binary responses."""
    return redis.Redis.from_url(url)


def _redis_store(url: str, topic: str, lateness_s: int) -> RedisWindowStore:
    """Return a RedisWindowStore over the server at url."""
    return RedisWindowStore(_redis_client(url), topic, lateness_s)


def _stop_on_signals(stop: threading.Event) -> None:
    """Set stop on SIGINT and, where the platform has it, SIGTERM."""

    def handle(_signum: int, _frame: object) -> None:
        """Set the stop event."""
        stop.set()

    signal.signal(signal.SIGINT, handle)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle)


def main(argv: Sequence[str]) -> int:
    """Parse argv, run the streaming command, print its result as JSON, and return 0."""
    command = stream_cli.parse(argv)
    handlers: dict[type, Callable[..., object]] = {
        stream_cli.SimulateCommand: run_simulate,
        stream_cli.WorkerCommand: run_worker,
        stream_cli.VerifyReplayCommand: run_verify_replay,
        stream_cli.VerifyStateCommand: run_verify_state,
    }
    print(render(handlers[type(command)](command)))
    return 0


def run() -> None:
    """Run main with the process arguments and exit with its status (console entry point)."""
    sys.exit(main(sys.argv[1:]))


if __name__ == "__main__":
    run()
