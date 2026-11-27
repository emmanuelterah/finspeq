"""Composition root of the streaming processes: simulator, workers, and verification."""

from __future__ import annotations

import os
import signal
import sys
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import date, timedelta

import psycopg
import redis
from confluent_kafka import Consumer, Producer

from finspeq.adapters.inbound import stream_cli
from finspeq.adapters.inbound.faults import FaultInjector
from finspeq.adapters.inbound.feature_worker import FeatureWorker, WorkerLimits, WorkerReport
from finspeq.adapters.inbound.output import render, write_json
from finspeq.adapters.inbound.selection import EventSelection
from finspeq.adapters.outbound.clock import SystemClock
from finspeq.adapters.outbound.drift_reference import read_reference
from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.adapters.outbound.null_metrics import NullMetrics
from finspeq.adapters.outbound.parquet import ParquetHistoricalSource
from finspeq.adapters.outbound.postgres_ledger import PostgresLedger
from finspeq.adapters.outbound.prometheus import PrometheusMetrics
from finspeq.adapters.outbound.redis_window import RedisWindowStore
from finspeq.adapters.outbound.redpanda import (
    COOPERATIVE_STRATEGY,
    RebalanceEvent,
    RedpandaPublisher,
    RedpandaSource,
    SourceOptions,
    consumer_config,
    producer_config,
    read_topic,
)
from finspeq.application.apply_events import ApplyEvents
from finspeq.application.disorder import arrival_order
from finspeq.application.drift import MeasureDrift, daily_columns
from finspeq.application.errors import StoreUnavailableError
from finspeq.application.messages import LabeledTransaction, TimeRange
from finspeq.application.ports import Metrics
from finspeq.application.record_decisions import RecordDecisions
from finspeq.application.replay import partitioned_batches
from finspeq.application.shadow import CompareShadow, ShadowReport
from finspeq.application.simulate import ReplayHistory, SimulationPlan, SimulationReport
from finspeq.application.verify import (
    ReplayReport,
    ReplayScenario,
    StateReport,
    VerifyFinalState,
    VerifyReplay,
)
from finspeq.application.verify_ledger import LedgerReport, VerifyLedger
from finspeq.domain.model import EventTime, TxId

PUBLISH_TIMEOUT_S = 30.0
POLL_TIMEOUT_S = 0.5
VERIFY_TOPIC = "verify-replay"
STATE_BATCH_SIZE = 500
CRASH_EXIT_CODE = 137
READER = "reader"  # consumer and topic names of a ledger used only for rows()
EPOCH_DAY = date(1970, 1, 1)
READ_TIMEOUT_S = 300.0
POSTGRES_WAIT_S = 60.0
POSTGRES_CONNECT_S = 5
POSTGRES_RETRY_S = 1.0


def run_simulate(command: stream_cli.SimulateCommand) -> SimulationReport:
    """Publish the selected history to the topic and return the achieved rate."""
    producer = Producer(producer_config(command.bootstrap, command.linger_ms))
    publisher = RedpandaPublisher(
        producer, command.topic, PUBLISH_TIMEOUT_S, command.round_robin_partitions
    )
    selection = command.selection
    plan = SimulationPlan(
        selection.time_range,
        selection.cards,
        command.limit,
        command.rate,
        command.chunk_size,
        command.disorder,
    )
    source = ParquetHistoricalSource(selection.source)
    return ReplayHistory(source, publisher, SystemClock(), time.sleep).execute(plan)


@dataclass(frozen=True, slots=True)
class ConsumerRun:
    """One consumer's totals and the rebalance events it saw."""

    report: WorkerReport
    rebalances: tuple[RebalanceEvent, ...]


@dataclass(frozen=True, slots=True)
class WorkerRunReport:
    """Totals of every consumer of one worker process.

    events_per_second is the sum of events divided by the longest busy time.
    """

    consumers: tuple[ConsumerRun, ...]
    events: int
    events_per_second: float


def run_worker(command: stream_cli.WorkerCommand) -> WorkerRunReport:
    """Run command.threads consumers until Ctrl+C, SIGTERM, or the idle limit; return totals.

    Each consumer has its own Kafka consumer, Redis client, and ApplyEvents, and runs in its
    own thread. With an instance id and more than one thread, consumer i uses "<id>-<i>".
    """
    metrics: Metrics = NullMetrics()
    if command.metrics_port is not None:
        prometheus = PrometheusMetrics()
        prometheus.serve(command.metrics_port)
        metrics = prometheus
    stop = threading.Event()
    _stop_on_signals(stop)
    runs: list[ConsumerRun | None] = [None] * command.threads
    threads = [
        threading.Thread(target=_run_consumer, args=(command, index, metrics, stop, runs))
        for index in range(command.threads)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    finished = tuple(run for run in runs if run is not None)
    events = sum(run.report.events for run in finished)
    longest = max((run.report.busy_s for run in finished), default=0.0)
    rate = events / longest if longest > 0 else 0.0
    return WorkerRunReport(finished, events, rate)


def _run_consumer(
    command: stream_cli.WorkerCommand,
    index: int,
    metrics: Metrics,
    stop: threading.Event,
    runs: list[ConsumerRun | None],
) -> None:
    """Run consumer number index of command until it stops; store its result in runs[index]."""
    store = _redis_store(command.redis_url, command.topic, command.lateness_s)
    instance_id = command.instance_id
    if instance_id is not None and command.threads > 1:
        instance_id = f"{instance_id}-{index}"
    config = consumer_config(
        command.bootstrap,
        command.group,
        instance_id,
        command.assignment_strategy,
        command.max_poll_interval_ms,
    )
    cooperative = command.assignment_strategy == COOPERATIVE_STRATEGY
    source = RedpandaSource(
        Consumer(config),
        command.topic,
        store.position,
        SourceOptions(POLL_TIMEOUT_S, cooperative, metrics=metrics),
    )
    source.start()
    clock = SystemClock()
    limits = WorkerLimits(
        command.batch_size, command.idle_exit_s, command.start_timeout_s, command.max_batches
    )
    faults = FaultInjector(command.faults, _hard_exit, time.sleep)
    use_case = ApplyEvents(store, metrics, clock, command.lateness_s)
    worker = FeatureWorker(source, use_case, clock, limits, faults)
    try:
        report = worker.run(stop)
    finally:
        source.close()
    runs[index] = ConsumerRun(report, tuple(source.rebalances))


def run_ledger(command: stream_cli.LedgerCommand) -> WorkerReport:
    """Record the topic's scored events in Postgres until a stop condition; return totals."""
    connection = _postgres(command.dsn)
    ledger = PostgresLedger(connection, command.consumer, command.topic)
    config = consumer_config(command.bootstrap, command.group)
    source = RedpandaSource(
        Consumer(config), command.topic, ledger.position, SourceOptions(POLL_TIMEOUT_S)
    )
    source.start()
    clock = SystemClock()
    limits = WorkerLimits(command.batch_size, command.idle_exit_s, command.start_timeout_s)
    faults = FaultInjector(command.faults, _hard_exit, time.sleep)
    worker = FeatureWorker(
        source, RecordDecisions(ledger, NullMetrics(), clock), clock, limits, faults
    )
    stop = threading.Event()
    _stop_on_signals(stop)
    try:
        return worker.run(stop)
    finally:
        source.close()
        connection.close()


def run_verify_ledger(command: stream_cli.VerifyLedgerCommand) -> LedgerReport:
    """Read the whole topic and compare its scored events with the Postgres ledger."""
    consumer = Consumer(
        {
            "bootstrap.servers": command.bootstrap,
            "group.id": f"verify-ledger-{os.getpid()}",
            "broker.address.family": "v4",
            "enable.auto.commit": False,
        }
    )
    try:
        events = read_topic(consumer, command.topic, command.partitions, READ_TIMEOUT_S)
    finally:
        consumer.close()
    connection = _postgres(command.dsn)
    try:
        ledger = PostgresLedger(connection, command.consumer, command.topic)
        return VerifyLedger(ledger, ledger).execute(events)
    finally:
        connection.close()


def run_drift_ledger(command: stream_cli.DriftLedgerCommand) -> dict[str, object]:
    """Measure each ledger day's PSI against the reference and write the report.

    Days with fewer than command.min_rows rows are skipped. With fail_on_alert, the process
    exits with status 1 after writing the report if any day has an alert.
    """
    reference = read_reference(command.reference)
    names = [feature.name for feature in reference.features]
    connection = _postgres(command.dsn)
    try:
        ledger = PostgresLedger(connection, READER, READER)
        days = daily_columns(ledger.rows(), names)
    finally:
        connection.close()
    use_case = MeasureDrift(reference, command.alert_psi)
    reports = [
        use_case.from_values(_day_label(day), columns)
        for day, columns in sorted(days.items())
        if len(columns[names[0]]) >= command.min_rows
    ]
    alerts = sum(1 for report in reports if report.alerts)
    result: dict[str, object] = {
        "alert_psi": command.alert_psi,
        "days": reports,
        "days_with_alerts": alerts,
    }
    write_json(command.output, result)
    if command.fail_on_alert and alerts:
        print(render(result))
        raise SystemExit(1)
    return result


def run_shadow_report(command: stream_cli.ShadowReportCommand) -> ShadowReport:
    """Compare the ledger's live and shadow decisions, labelled from the event file."""
    connection = _postgres(command.dsn)
    try:
        rows = list(PostgresLedger(connection, READER, READER).rows())
    finally:
        connection.close()
    shadowed = {row.tx_id for row in rows if row.record.shadow is not None}
    labels: dict[TxId, bool] = {}
    if shadowed:
        times = [row.time.epoch_s for row in rows if row.tx_id in shadowed]
        span = TimeRange(EventTime(min(times)), EventTime(max(times) + 1))
        for item in ParquetHistoricalSource(command.source).iter_labeled(span):
            if item.transaction.tx_id in shadowed:
                labels[item.transaction.tx_id] = item.is_fraud
    report = CompareShadow().execute(rows, labels)
    write_json(command.output, report)
    return report


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
    """Compare the worker-filled Redis store with an in-process replay of the same arrivals.

    The replay applies the selection in the arrival order that command.disorder produces, which
    is the order `simulate` published with the same disorder arguments.
    """
    candidate = _redis_store(command.redis_url, command.topic, command.lateness_s)
    use_case = VerifyFinalState(
        MemoryWindowStore(command.lateness_s), candidate, command.partitions
    )
    arrivals = arrival_order(_selected_items(command.selection), command.disorder)
    return use_case.execute(arrivals, STATE_BATCH_SIZE)


def _selected_items(selection: EventSelection) -> Iterator[LabeledTransaction]:
    """Yield the selection's historical transactions in canonical order."""
    for item in ParquetHistoricalSource(selection.source).iter_labeled(selection.time_range):
        if selection.cards.contains(item.transaction.card):
            yield item


def _postgres(dsn: str) -> psycopg.Connection:
    """Return an autocommit connection to dsn, retrying for up to POSTGRES_WAIT_S seconds.

    Raises:
        StoreUnavailableError: Postgres did not accept a connection in time.
    """
    deadline = time.monotonic() + POSTGRES_WAIT_S
    while True:
        try:
            return psycopg.connect(dsn, autocommit=True, connect_timeout=POSTGRES_CONNECT_S)
        except psycopg.OperationalError as exc:
            if time.monotonic() > deadline:
                raise StoreUnavailableError(f"Postgres unavailable: {exc}") from exc
            time.sleep(POSTGRES_RETRY_S)


def _day_label(day: int) -> str:
    """Return the UTC date of day number day (days since 1970-01-01) as YYYY-MM-DD."""
    return (EPOCH_DAY + timedelta(days=day)).isoformat()


def _redis_client(url: str) -> redis.Redis:
    """Return a Redis client for url with binary responses."""
    return redis.Redis.from_url(url)


def _redis_store(url: str, topic: str, lateness_s: int) -> RedisWindowStore:
    """Return a RedisWindowStore over the server at url."""
    return RedisWindowStore(_redis_client(url), topic, lateness_s)


def _hard_exit() -> None:
    """End the process immediately with CRASH_EXIT_CODE, without cleanup or offset commits."""
    os._exit(CRASH_EXIT_CODE)


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
        stream_cli.LedgerCommand: run_ledger,
        stream_cli.VerifyLedgerCommand: run_verify_ledger,
        stream_cli.DriftLedgerCommand: run_drift_ledger,
        stream_cli.ShadowReportCommand: run_shadow_report,
    }
    print(render(handlers[type(command)](command)))
    return 0


def run() -> None:
    """Run main with the process arguments and exit with its status (console entry point)."""
    sys.exit(main(sys.argv[1:]))


if __name__ == "__main__":
    run()
