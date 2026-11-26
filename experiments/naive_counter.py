"""Naive at-least-once counter: INCR per card, then commit the group offset.

`run` consumes the topic with Kafka group offsets (no stored position, no deduplication) and
can crash at the same points as the feature worker. `compare` reports how far the counters
exceed the true counts from the historical source.
"""

from __future__ import annotations

import argparse
import calendar
import json
import os
import sys
import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import redis
from confluent_kafka import Consumer, TopicPartition

from finspeq.adapters.codec import decode
from finspeq.adapters.inbound.faults import CrashPoint, FaultInjector, FaultPlan
from finspeq.adapters.inbound.selection import DEFAULT_SOURCE, parse_slice
from finspeq.adapters.outbound.parquet import ParquetHistoricalSource
from finspeq.adapters.outbound.redpanda import consumer_config, position_of
from finspeq.application.messages import LabeledTransaction, TimeRange
from finspeq.domain.model import EventTime, Transaction

COUNT_KEY = "naive:count:"
CENTS_KEY = "naive:cents:"
CRASH_EXIT_CODE = 137
POLL_TIMEOUT_S = 0.5


@dataclass(frozen=True, slots=True)
class Totals:
    """Event count and amount in cents of one card."""

    events: int
    cents: int


@dataclass(frozen=True, slots=True)
class Comparison:
    """True totals against counted totals, summed over every card."""

    cards: int
    events_true: int
    events_counted: int
    events_over: int
    cents_over: int
    cards_over_counted: int

    @property
    def over_count_percent(self) -> float:
        """Return events_over as a percentage of events_true."""
        return 100.0 * self.events_over / self.events_true if self.events_true else 0.0


def count(transactions: Iterable[Transaction]) -> dict[str, Totals]:
    """Return the event count and total cents of each card in transactions."""
    totals: dict[str, Totals] = {}
    for tx in transactions:
        current = totals.get(tx.card.value, Totals(0, 0))
        totals[tx.card.value] = Totals(current.events + 1, current.cents + tx.amount.cents)
    return totals


def compare(truth: dict[str, Totals], counted: dict[str, Totals]) -> Comparison:
    """Return the comparison of counted totals with true totals over the true cards."""
    zero = Totals(0, 0)
    events_counted = sum(counted.get(card, zero).events for card in truth)
    events_true = sum(t.events for t in truth.values())
    cents_over = sum(counted.get(card, zero).cents - t.cents for card, t in truth.items())
    over_cards = sum(1 for card, t in truth.items() if counted.get(card, zero).events > t.events)
    return Comparison(
        len(truth),
        events_true,
        events_counted,
        events_counted - events_true,
        cents_over,
        over_cards,
    )


def read_counters(client: redis.Redis, cards: Iterable[str]) -> dict[str, Totals]:
    """Return the counted totals of cards from Redis; missing counters count as zero."""
    result: dict[str, Totals] = {}
    for card in cards:
        events, cents = client.mget(COUNT_KEY + card, CENTS_KEY + card)
        result[card] = Totals(int(events or 0), int(cents or 0))
    return result


def run(args: argparse.Namespace) -> int:
    """Consume and count until idle; crash per the fault plan. Return 0 on idle exit."""
    client = redis.Redis.from_url(args.redis_url)
    consumer = Consumer(consumer_config(args.bootstrap, args.group, args.instance_id))
    consumer.subscribe([args.topic])
    point = None if args.crash_point is None else CrashPoint(args.crash_point)
    plan = FaultPlan(point, args.crash_probability, args.crash_seed)
    faults = FaultInjector(plan, _hard_exit, time.sleep)
    started = last_event = time.monotonic()
    seen_event = False
    events = 0
    while True:
        messages = consumer.consume(args.batch_size, POLL_TIMEOUT_S)
        if not messages:
            limit = args.idle_exit_s if seen_event else args.start_timeout_s
            if time.monotonic() - last_event >= limit:
                break
            continue
        seen_event = True
        pipeline = client.pipeline(transaction=False)
        next_offsets: dict[int, int] = {}
        for message in messages:
            position = position_of(message)
            next_offsets[position.partition] = position.offset + 1
            tx = decode(message.key(), message.value(), position).transaction
            pipeline.incr(COUNT_KEY + tx.card.value)
            pipeline.incrby(CENTS_KEY + tx.card.value, tx.amount.cents)
        pipeline.execute()
        faults.checkpoint(CrashPoint.AFTER_APPLY)
        commits = [TopicPartition(args.topic, p, o) for p, o in sorted(next_offsets.items())]
        consumer.commit(offsets=commits, asynchronous=False)
        faults.checkpoint(CrashPoint.AFTER_ACK)
        events += len(messages)
        last_event = time.monotonic()
    consumer.close()
    print(json.dumps({"events": events, "seconds": round(time.monotonic() - started, 1)}))
    return 0


def compare_command(args: argparse.Namespace) -> int:
    """Print the comparison of the Redis counters with the source's true totals; return 0."""
    cards = parse_slice(args.slice)
    time_range = TimeRange(_day(args.from_date), _day(args.to_date))
    items: Iterable[LabeledTransaction] = ParquetHistoricalSource(args.source).iter_labeled(
        time_range
    )
    truth = count(i.transaction for i in items if cards.contains(i.transaction.card))
    result = compare(truth, read_counters(redis.Redis.from_url(args.redis_url), truth))
    print(json.dumps({**asdict(result), "over_count_percent": result.over_count_percent}, indent=2))
    return 0


def _day(text: str) -> EventTime:
    """Return 00:00 UTC of the day written as YYYY-MM-DD."""
    return EventTime(calendar.timegm(date.fromisoformat(text).timetuple()))


def _hard_exit() -> None:
    """End the process immediately with CRASH_EXIT_CODE, without committing offsets."""
    os._exit(CRASH_EXIT_CODE)


def main() -> int:
    """Parse the sub-command and run it."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    runner = commands.add_parser("run")
    runner.add_argument("--bootstrap", default="127.0.0.1:19092")
    runner.add_argument("--topic", default="transactions")
    runner.add_argument("--group", default="naive-counter")
    runner.add_argument("--instance-id")
    runner.add_argument("--redis-url", default="redis://127.0.0.1:6379/2")
    runner.add_argument("--batch-size", type=int, default=50)
    runner.add_argument("--idle-exit-s", type=float, default=10.0)
    runner.add_argument("--start-timeout-s", type=float, default=60.0)
    runner.add_argument("--crash-point", choices=[p.value for p in CrashPoint])
    runner.add_argument("--crash-probability", type=float, default=0.0)
    runner.add_argument("--crash-seed", type=int, default=0)
    checker = commands.add_parser("compare")
    checker.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    checker.add_argument("--from-date", required=True)
    checker.add_argument("--to-date", required=True)
    checker.add_argument("--slice", default="all")
    checker.add_argument("--redis-url", default="redis://127.0.0.1:6379/2")
    args = parser.parse_args()
    return run(args) if args.command == "run" else compare_command(args)


if __name__ == "__main__":
    sys.exit(main())
