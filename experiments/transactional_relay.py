"""Kafka transactions vs a plain relay: consume, transform, produce, under crashes.

`run` copies every input event to an output topic as a small JSON record that names its
source partition and offset. Both modes flush each batch's records to the broker before the
after-apply crash point. In `transactional` mode the output records and the consumed offsets
are committed in one Kafka transaction (send_offsets_to_transaction); a crash before the
commit leaves the flushed records in the log as an aborted transaction. In `plain` mode the
group offset is committed after the flush, so a crash between the two produces the batch
again. Crash points reuse the worker's names: after-apply (records flushed, nothing committed)
and after-ack (committed).
`count` reads the output topic with read_committed and read_uncommitted and counts copies per
input position.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass

from confluent_kafka import OFFSET_BEGINNING, Consumer, Message, Producer, TopicPartition

from finspeq.adapters.inbound.faults import CrashPoint, FaultInjector, FaultPlan

CRASH_EXIT_CODE = 137
POLL_TIMEOUT_S = 1.0
TRANSACTION_TIMEOUT_S = 30.0
EMPTY_POLLS_TO_STOP = 5
RELAY_CRASH_POINTS = (CrashPoint.AFTER_APPLY.value, CrashPoint.AFTER_ACK.value)
ISOLATION_LEVELS = ("read_committed", "read_uncommitted")


@dataclass(frozen=True, slots=True)
class RunReport:
    """Totals of one relay run: batches, input events, busy seconds, and events/s."""

    mode: str
    batches: int
    events: int
    seconds: float
    events_per_second: float


@dataclass(frozen=True, slots=True)
class CopyCount:
    """Output copies per input position, read with one isolation level.

    Attributes:
        isolation: "read_committed" or "read_uncommitted".
        records: Output records read.
        inputs: Input events (sum of the input end offsets).
        missing: Input positions with no output copy.
        duplicated: Input positions with more than one copy.
        extra_copies: Copies beyond the first, summed.
    """

    isolation: str
    records: int
    inputs: int
    missing: int
    duplicated: int
    extra_copies: int


def transform(message: Message) -> tuple[bytes | None, bytes]:
    """Return the output key (the input key) and a JSON value naming the input position."""
    value = {
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "bytes": len(message.value() or b""),
    }
    return message.key(), json.dumps(value, sort_keys=True).encode("ascii")


def count_copies(
    positions: list[tuple[int, int]], ends: dict[int, int], isolation: str
) -> CopyCount:
    """Return the copy statistics of output records (source positions) against input ends."""
    copies = Counter(positions)
    expected = {(p, o) for p, end in ends.items() for o in range(end)}
    duplicated = [n for key, n in copies.items() if n > 1 and key in expected]
    return CopyCount(
        isolation=isolation,
        records=len(positions),
        inputs=len(expected),
        missing=sum(1 for key in expected if key not in copies),
        duplicated=len(duplicated),
        extra_copies=sum(n - 1 for n in duplicated),
    )


def relay_consumer(args: argparse.Namespace) -> Consumer:
    """Return the relay's consumer: manual commits, read_committed, static membership."""
    return Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "broker.address.family": "v4",
            "group.id": args.group,
            "group.instance.id": f"{args.group}-0",
            "session.timeout.ms": 6_000,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
    )


def relay_producer(args: argparse.Namespace) -> Producer:
    """Return an idempotent producer, transactional (with a stable id) in transactional mode."""
    config: dict[str, str | int | bool] = {
        "bootstrap.servers": args.bootstrap,
        "broker.address.family": "v4",
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
    }
    if args.mode == "transactional":
        config["transactional.id"] = f"{args.group}-relay"
    return Producer(config)


def run(args: argparse.Namespace) -> int:
    """Relay input to output until idle, crashing as the fault plan says; return 0."""
    transactional = args.mode == "transactional"
    consumer = relay_consumer(args)
    producer = relay_producer(args)
    if transactional:
        producer.init_transactions(TRANSACTION_TIMEOUT_S)
    point = None if args.crash_point is None else CrashPoint(args.crash_point)
    faults = FaultInjector(
        FaultPlan(point, args.crash_probability, args.crash_seed), _hard_exit, time.sleep
    )
    consumer.subscribe([args.input_topic])
    started = last_event = time.monotonic()
    first_event: float | None = None
    batches = events = 0
    while time.monotonic() - last_event <= args.idle_exit_s:
        messages = [
            m for m in consumer.consume(args.batch_size, POLL_TIMEOUT_S) if m.error() is None
        ]
        if not messages:
            continue
        first_event = first_event or time.monotonic()
        if transactional:
            producer.begin_transaction()
        for message in messages:
            key, value = transform(message)
            producer.produce(args.output_topic, value=value, key=key)
        producer.flush(TRANSACTION_TIMEOUT_S)
        faults.checkpoint(CrashPoint.AFTER_APPLY)
        if transactional:
            positions = consumer.position(consumer.assignment())
            metadata = consumer.consumer_group_metadata()
            producer.send_offsets_to_transaction(positions, metadata, TRANSACTION_TIMEOUT_S)
            producer.commit_transaction(TRANSACTION_TIMEOUT_S)
        else:
            consumer.commit(asynchronous=False)
        faults.checkpoint(CrashPoint.AFTER_ACK)
        batches += 1
        events += len(messages)
        last_event = time.monotonic()
    busy = last_event - (first_event or started)
    report = RunReport(args.mode, batches, events, round(busy, 3), events / busy if busy else 0.0)
    print(json.dumps(asdict(report), indent=2))
    consumer.close()
    return 0


def read_positions(
    args: argparse.Namespace, isolation: str
) -> tuple[list[tuple[int, int]], dict[int, int]]:
    """Return the source position of every output record and the input end offsets.

    Reads the output topic from the beginning with the given isolation level until
    EMPTY_POLLS_TO_STOP polls in a row return nothing.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "broker.address.family": "v4",
            "group.id": f"relay-count-{os.getpid()}",
            "enable.auto.commit": False,
            "isolation.level": isolation,
        }
    )
    try:
        ends = {
            p: consumer.get_watermark_offsets(TopicPartition(args.input_topic, p), 10.0)[1]
            for p in range(args.partitions)
        }
        consumer.assign(
            [TopicPartition(args.output_topic, p, OFFSET_BEGINNING) for p in range(args.partitions)]
        )
        positions: list[tuple[int, int]] = []
        empty = 0
        while empty < EMPTY_POLLS_TO_STOP:
            batch = [m for m in consumer.consume(1_000, POLL_TIMEOUT_S) if m.error() is None]
            empty = 0 if batch else empty + 1
            for message in batch:
                value = json.loads(message.value() or b"{}")
                positions.append((int(value["source_partition"]), int(value["source_offset"])))
        return positions, ends
    finally:
        consumer.close()


def count_command(args: argparse.Namespace) -> int:
    """Print the copy statistics of the output topic under both isolation levels; return 0."""
    results = []
    for isolation in ISOLATION_LEVELS:
        positions, ends = read_positions(args, isolation)
        results.append(asdict(count_copies(positions, ends, isolation)))
    print(json.dumps(results, indent=2))
    return 0


def _hard_exit() -> None:
    """End the process immediately with CRASH_EXIT_CODE, without cleanup or commits."""
    os._exit(CRASH_EXIT_CODE)


def main() -> int:
    """Parse the sub-command and run it."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    runner = commands.add_parser("run", help="relay the input topic to the output topic")
    counter = commands.add_parser("count", help="count output copies per input position")
    for sub in (runner, counter):
        sub.add_argument("--bootstrap", default="127.0.0.1:19092")
        sub.add_argument("--input-topic", required=True)
        sub.add_argument("--output-topic", required=True)
    runner.add_argument("--mode", choices=["transactional", "plain"], required=True)
    runner.add_argument("--group", required=True)
    runner.add_argument("--batch-size", type=int, default=200)
    runner.add_argument("--idle-exit-s", type=float, default=10.0)
    runner.add_argument("--crash-point", choices=RELAY_CRASH_POINTS)
    runner.add_argument("--crash-probability", type=float, default=0.0)
    runner.add_argument("--crash-seed", type=int, default=0)
    counter.add_argument("--partitions", type=int, default=1)
    args = parser.parse_args()
    return run(args) if args.command == "run" else count_command(args)


if __name__ == "__main__":
    sys.exit(main())
