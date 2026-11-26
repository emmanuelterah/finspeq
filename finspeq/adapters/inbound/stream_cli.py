"""Command-line arguments of the streaming commands: simulate, worker, verify-*."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass

from finspeq.adapters.inbound.faults import CrashPoint, FaultPlan
from finspeq.adapters.inbound.selection import (
    EventSelection,
    add_disorder,
    add_selection,
    disorder_from,
    selection_from,
)
from finspeq.application.disorder import Disorder

DEFAULT_BOOTSTRAP = "127.0.0.1:19092"
DEFAULT_TOPIC = "transactions"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"
DEFAULT_LATENESS_S = 3_600
ASSIGNMENT_STRATEGIES = ("range", "roundrobin", "cooperative-sticky")
DEFAULT_MAX_POLL_INTERVAL_MS = 300_000
LATENESS_HELP = "seconds kept beyond the 24 h window (ADR-011)"


@dataclass(frozen=True, slots=True)
class SimulateCommand:
    """Arguments of `simulate`."""

    selection: EventSelection
    disorder: Disorder
    limit: int | None
    rate: float
    chunk_size: int
    bootstrap: str
    topic: str
    linger_ms: int
    round_robin_partitions: int | None


@dataclass(frozen=True, slots=True)
class WorkerCommand:
    """Arguments of `worker`."""

    bootstrap: str
    topic: str
    group: str
    redis_url: str
    lateness_s: int
    batch_size: int
    idle_exit_s: float | None
    start_timeout_s: float | None
    metrics_port: int | None
    instance_id: str | None
    faults: FaultPlan
    assignment_strategy: str
    max_poll_interval_ms: int
    threads: int
    max_batches: int | None


@dataclass(frozen=True, slots=True)
class VerifyReplayCommand:
    """Arguments of `verify-replay` (store level, in process)."""

    selection: EventSelection
    partitions: int
    batch_size: int
    redeliver_every: int
    redis_url: str
    lateness_s: int
    flush: bool


@dataclass(frozen=True, slots=True)
class VerifyStateCommand:
    """Arguments of `verify-state` (system level, after a worker run)."""

    selection: EventSelection
    disorder: Disorder
    redis_url: str
    topic: str
    partitions: int
    lateness_s: int


StreamCommand = SimulateCommand | WorkerCommand | VerifyReplayCommand | VerifyStateCommand


def add_commands(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Add the simulate, worker, verify-replay, and verify-state sub-commands."""
    simulate = commands.add_parser("simulate", help="publish history to the topic")
    add_selection(simulate)
    add_disorder(simulate)
    simulate.add_argument("--limit", type=int)
    simulate.add_argument("--rate", type=float, default=0.0, help="events/s; 0 = unlimited")
    simulate.add_argument("--chunk-size", type=int, default=500)
    simulate.add_argument("--linger-ms", type=int, default=20)
    simulate.add_argument(
        "--round-robin-partitions",
        type=int,
        help="ignore the key and spread events over this many partitions (M4 experiment)",
    )
    _add_broker(simulate)
    worker = commands.add_parser("worker", help="apply topic events to the Redis store")
    _add_broker(worker)
    worker.add_argument("--group", default="finspeq-features")
    worker.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    worker.add_argument("--lateness-s", type=int, default=DEFAULT_LATENESS_S, help=LATENESS_HELP)
    worker.add_argument("--batch-size", type=int, default=500)
    worker.add_argument(
        "--idle-exit-s", type=float, help="after the first event, stop after this long idle"
    )
    worker.add_argument(
        "--start-timeout-s", type=float, default=60.0, help="stop if no event arrives in time"
    )
    worker.add_argument("--metrics-port", type=int, help="serve Prometheus metrics on this port")
    worker.add_argument("--instance-id", help="static group member id (fast rejoin after crash)")
    worker.add_argument("--crash-point", choices=[p.value for p in CrashPoint])
    worker.add_argument("--crash-probability", type=float, default=0.0)
    worker.add_argument("--crash-seed", type=int, default=0)
    worker.add_argument("--stall-s", type=float, default=0.0, help="sleep once after a poll")
    worker.add_argument("--stall-at-batch", type=int, default=1, help="batch to stall after")
    worker.add_argument(
        "--assignment-strategy", choices=ASSIGNMENT_STRATEGIES, default=ASSIGNMENT_STRATEGIES[0]
    )
    worker.add_argument("--max-poll-interval-ms", type=int, default=DEFAULT_MAX_POLL_INTERVAL_MS)
    worker.add_argument("--threads", type=int, default=1, help="consumers in this process")
    worker.add_argument("--max-batches", type=int, help="leave the group after this many batches")
    replay = commands.add_parser("verify-replay", help="compare Redis with memory, in process")
    add_selection(replay)
    replay.add_argument("--partitions", type=int, default=4)
    replay.add_argument("--batch-size", type=int, default=500)
    replay.add_argument("--redeliver-every", type=int, default=0)
    replay.add_argument("--redis-url", default="redis://localhost:6379/1")
    replay.add_argument("--lateness-s", type=int, default=DEFAULT_LATENESS_S, help=LATENESS_HELP)
    replay.add_argument("--flush", action="store_true", help="empty the Redis database first")
    state = commands.add_parser("verify-state", help="compare worker-filled Redis with memory")
    add_selection(state)
    add_disorder(state)
    state.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    state.add_argument("--topic", default=DEFAULT_TOPIC)
    state.add_argument("--partitions", type=int, default=1)
    state.add_argument("--lateness-s", type=int, default=DEFAULT_LATENESS_S, help=LATENESS_HELP)


def parse(argv: Sequence[str]) -> StreamCommand:
    """Return the streaming command described by argv; exit with status 2 on invalid arguments."""
    parser = argparse.ArgumentParser(prog="finspeq-stream")
    add_commands(parser.add_subparsers(dest="command", required=True))
    command = _command_from(parser.parse_args(list(argv)))
    if command is None:
        parser.error(f"unknown command {argv[0] if argv else ''}")
    return command


def _command_from(args: argparse.Namespace) -> StreamCommand | None:
    """Return the streaming command described by args, or None for an unknown command."""
    if args.command == "simulate":
        return SimulateCommand(
            selection_from(args),
            disorder_from(args),
            args.limit,
            args.rate,
            args.chunk_size,
            args.bootstrap,
            args.topic,
            args.linger_ms,
            args.round_robin_partitions,
        )
    if args.command == "worker":
        return WorkerCommand(
            args.bootstrap,
            args.topic,
            args.group,
            args.redis_url,
            args.lateness_s,
            args.batch_size,
            args.idle_exit_s,
            args.start_timeout_s,
            args.metrics_port,
            args.instance_id,
            FaultPlan(
                None if args.crash_point is None else CrashPoint(args.crash_point),
                args.crash_probability,
                args.crash_seed,
                args.stall_s,
                args.stall_at_batch,
            ),
            args.assignment_strategy,
            args.max_poll_interval_ms,
            args.threads,
            args.max_batches,
        )
    if args.command == "verify-replay":
        return VerifyReplayCommand(
            selection_from(args),
            args.partitions,
            args.batch_size,
            args.redeliver_every,
            args.redis_url,
            args.lateness_s,
            args.flush,
        )
    if args.command == "verify-state":
        return VerifyStateCommand(
            selection_from(args),
            disorder_from(args),
            args.redis_url,
            args.topic,
            args.partitions,
            args.lateness_s,
        )
    return None


def _add_broker(parser: argparse.ArgumentParser) -> None:
    """Add --bootstrap and --topic to parser."""
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
