"""Command-line arguments of the streaming commands: simulate, worker, verify-*."""

from __future__ import annotations

import argparse
import calendar
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from finspeq.application.messages import TimeRange
from finspeq.application.replay import ALL_CARDS, CardSlice
from finspeq.domain.model import EventTime

DEFAULT_SOURCE = Path("data/processed/events_sorted.parquet")
DEFAULT_BOOTSTRAP = "127.0.0.1:19092"
DEFAULT_TOPIC = "transactions"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"
ALL_SLICE_TEXT = "all"


@dataclass(frozen=True, slots=True)
class EventSelection:
    """Historical events chosen by source file, time range, and card slice."""

    source: Path
    time_range: TimeRange | None
    cards: CardSlice


@dataclass(frozen=True, slots=True)
class SimulateCommand:
    """Arguments of `simulate`."""

    selection: EventSelection
    limit: int | None
    rate: float
    chunk_size: int
    bootstrap: str
    topic: str
    linger_ms: int


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
    redis_url: str
    topic: str
    partitions: int
    lateness_s: int


StreamCommand = SimulateCommand | WorkerCommand | VerifyReplayCommand | VerifyStateCommand


def add_commands(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Add the simulate, worker, verify-replay, and verify-state sub-commands."""
    simulate = commands.add_parser("simulate", help="publish history to the topic")
    _add_selection(simulate)
    simulate.add_argument("--limit", type=int)
    simulate.add_argument("--rate", type=float, default=0.0, help="events/s; 0 = unlimited")
    simulate.add_argument("--chunk-size", type=int, default=500)
    simulate.add_argument("--linger-ms", type=int, default=20)
    _add_broker(simulate)
    worker = commands.add_parser("worker", help="apply topic events to the Redis store")
    _add_broker(worker)
    worker.add_argument("--group", default="finspeq-features")
    worker.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    worker.add_argument("--lateness-s", type=int, default=0)
    worker.add_argument("--batch-size", type=int, default=500)
    worker.add_argument(
        "--idle-exit-s", type=float, help="after the first event, stop after this long idle"
    )
    worker.add_argument(
        "--start-timeout-s", type=float, default=60.0, help="stop if no event arrives in time"
    )
    worker.add_argument("--metrics-port", type=int, help="serve Prometheus metrics on this port")
    replay = commands.add_parser("verify-replay", help="compare Redis with memory, in process")
    _add_selection(replay)
    replay.add_argument("--partitions", type=int, default=4)
    replay.add_argument("--batch-size", type=int, default=500)
    replay.add_argument("--redeliver-every", type=int, default=0)
    replay.add_argument("--redis-url", default="redis://localhost:6379/1")
    replay.add_argument("--lateness-s", type=int, default=0)
    replay.add_argument("--flush", action="store_true", help="empty the Redis database first")
    state = commands.add_parser("verify-state", help="compare worker-filled Redis with memory")
    _add_selection(state)
    state.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    state.add_argument("--topic", default=DEFAULT_TOPIC)
    state.add_argument("--partitions", type=int, default=1)
    state.add_argument("--lateness-s", type=int, default=0)


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
            _selection(args),
            args.limit,
            args.rate,
            args.chunk_size,
            args.bootstrap,
            args.topic,
            args.linger_ms,
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
        )
    if args.command == "verify-replay":
        return VerifyReplayCommand(
            _selection(args),
            args.partitions,
            args.batch_size,
            args.redeliver_every,
            args.redis_url,
            args.lateness_s,
            args.flush,
        )
    if args.command == "verify-state":
        return VerifyStateCommand(
            _selection(args), args.redis_url, args.topic, args.partitions, args.lateness_s
        )
    return None


def parse_slice(text: str) -> CardSlice:
    """Return the card slice written as "modulus:remainder", or every card for "all"."""
    if text == ALL_SLICE_TEXT:
        return ALL_CARDS
    modulus, remainder = text.split(":")
    return CardSlice(int(modulus), int(remainder))


def _add_selection(parser: argparse.ArgumentParser) -> None:
    """Add --source, --from-date, --to-date, and --slice to parser."""
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--from-date", help="first day, YYYY-MM-DD (UTC)")
    parser.add_argument("--to-date", help="day after the last, YYYY-MM-DD (UTC)")
    parser.add_argument("--slice", default=ALL_SLICE_TEXT, help='"modulus:remainder" or "all"')


def _add_broker(parser: argparse.ArgumentParser) -> None:
    """Add --bootstrap and --topic to parser."""
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)


def _selection(args: argparse.Namespace) -> EventSelection:
    """Return the EventSelection described by --source, --from-date, --to-date, --slice.

    Raises:
        SystemExit: Only one of --from-date and --to-date is given.
    """
    if (args.from_date is None) != (args.to_date is None):
        raise SystemExit("give both --from-date and --to-date, or neither")
    time_range = None
    if args.from_date is not None:
        time_range = TimeRange(_day(args.from_date), _day(args.to_date))
    return EventSelection(args.source, time_range, parse_slice(args.slice))


def _day(text: str) -> EventTime:
    """Return 00:00:00 UTC of the day written as YYYY-MM-DD."""
    return EventTime(calendar.timegm(date.fromisoformat(text).timetuple()))
