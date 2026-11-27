"""Kafka-protocol (Redpanda) implementations of the publisher and event source ports."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from typing import Literal, Protocol

from confluent_kafka import OFFSET_BEGINNING, KafkaError, Message, TopicPartition

from finspeq.adapters.codec import decode, encode
from finspeq.application.errors import PortError, PublishFailedError
from finspeq.application.messages import Event, EventBatch, EventPosition
from finspeq.application.ports import BulkPublisher, EventPublisher, EventSource, Metrics
from finspeq.domain.model import Transaction
from finspeq.domain.scored import ScoredTransaction

POLL_INTERVAL_S = 0.05
DEFAULT_MESSAGE_TIMEOUT_MS = 300_000
ACK_GRACE_S = 1.0
UNASSIGNED_PARTITION = -1
COOPERATIVE_STRATEGY = "cooperative-sticky"


def producer_config(
    bootstrap: str, linger_ms: int, message_timeout_ms: int = DEFAULT_MESSAGE_TIMEOUT_MS
) -> dict[str, str | int | bool]:
    """Return producer settings: idempotent, acks=all, murmur2 partitioning, IPv4 only.

    A message not acknowledged within message_timeout_ms fails with a delivery error.
    """
    return {
        "message.timeout.ms": message_timeout_ms,
        "bootstrap.servers": bootstrap,
        "broker.address.family": "v4",
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": linger_ms,
        "partitioner": "murmur2_random",
        "compression.type": "lz4",
    }


DEFAULT_SESSION_TIMEOUT_MS = 6_000
HEARTBEAT_INTERVAL_MS = 2_000
DEFAULT_MAX_POLL_INTERVAL_MS = 300_000
ASSIGNMENT_STRATEGIES = ("range", "roundrobin", "cooperative-sticky")


def consumer_config(
    bootstrap: str,
    group: str,
    instance_id: str | None = None,
    assignment_strategy: str = "range",
    max_poll_interval_ms: int = DEFAULT_MAX_POLL_INTERVAL_MS,
) -> dict[str, str | int | bool]:
    """Return consumer settings: manual offsets, start at the beginning, committed reads, IPv4.

    The 6 s session timeout bounds how long a crashed member holds its partitions. With
    instance_id set, the consumer is a static member: a restart with the same id gets its
    partitions back without waiting for the session timeout. assignment_strategy is one of
    ASSIGNMENT_STRATEGIES; "cooperative-sticky" moves only the partitions that change owner.
    A member that does not poll for max_poll_interval_ms leaves the group.
    """
    config: dict[str, str | int | bool] = {
        "bootstrap.servers": bootstrap,
        "broker.address.family": "v4",
        "group.id": group,
        "partition.assignment.strategy": assignment_strategy,
        "max.poll.interval.ms": max_poll_interval_ms,
        "session.timeout.ms": DEFAULT_SESSION_TIMEOUT_MS,
        "heartbeat.interval.ms": HEARTBEAT_INTERVAL_MS,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
        "enable.partition.eof": False,
    }
    if instance_id is not None:
        config["group.instance.id"] = instance_id
    return config


class ProducerLike(Protocol):
    """The subset of confluent_kafka.Producer used by RedpandaPublisher."""

    def produce(
        self,
        topic: str,
        *,
        value: bytes,
        key: bytes,
        partition: int,
        on_delivery: Callable[[KafkaError | None, Message], None],
    ) -> None:
        """Queue one message for delivery to partition, or by key when partition is -1."""
        ...

    def poll(self, timeout: float) -> int:
        """Serve delivery callbacks for up to timeout seconds."""
        ...

    def flush(self, timeout: float) -> int:
        """Wait up to timeout seconds for queued messages; return the number still queued."""
        ...


class ConsumerLike(Protocol):
    """The subset of confluent_kafka.Consumer used by RedpandaSource."""

    def subscribe(
        self,
        topics: list[str],
        on_assign: Callable[[object, list[TopicPartition]], None],
        on_revoke: Callable[[object, list[TopicPartition]], None],
        on_lost: Callable[[object, list[TopicPartition]], None],
    ) -> None:
        """Join the group for topics with assignment callbacks."""
        ...

    def assign(self, partitions: list[TopicPartition]) -> None:
        """Replace the assignment with partitions at their given offsets (eager protocol)."""
        ...

    def incremental_assign(self, partitions: list[TopicPartition]) -> None:
        """Add partitions at their given offsets to the assignment (cooperative protocol)."""
        ...

    def consume(self, num_messages: int, timeout: float) -> list[Message]:
        """Return up to num_messages messages, waiting up to timeout seconds."""
        ...

    def seek(self, partition: TopicPartition) -> None:
        """Move the read position of an assigned partition."""
        ...

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float = -1.0, cached: bool = False
    ) -> tuple[int, int]:
        """Return the low and high offsets of partition; cached=True uses the last fetch."""
        ...

    def commit(self, *, offsets: list[TopicPartition], asynchronous: Literal[True]) -> None:
        """Commit group offsets."""
        ...

    def close(self) -> None:
        """Leave the group and release resources."""
        ...


class RedpandaPublisher(EventPublisher, BulkPublisher):
    """Writes transactions to one topic, keyed by card number."""

    def __init__(
        self,
        producer: ProducerLike,
        topic: str,
        timeout_s: float,
        round_robin_partitions: int | None = None,
    ) -> None:
        """Create a publisher that waits up to timeout_s seconds for broker acknowledgements.

        Messages go to the partition the producer's key partitioner chooses. With
        round_robin_partitions set, message n of the publisher goes to partition
        n % round_robin_partitions instead, ignoring the key (the M4 unkeyed experiment).
        """
        self._producer = producer
        self._topic = topic
        self._timeout_s = timeout_s
        self._round_robin_partitions = round_robin_partitions
        self._sent = 0

    def publish_raw(self, tx: Transaction) -> EventPosition:
        """Append tx without a decision record and return its position after the broker ack."""
        (position,) = self._send([encode(tx, None)])
        return position

    def publish_scored(self, scored: ScoredTransaction) -> EventPosition:
        """Append scored and return its position after the broker ack."""
        (position,) = self._send([encode(scored.transaction, scored.record)])
        return position

    def publish_raw_many(self, txs: Sequence[Transaction]) -> tuple[EventPosition, ...]:
        """Append txs in order and return their positions after every broker ack."""
        return self._send([encode(tx, None) for tx in txs])

    def _send(self, messages: list[tuple[bytes, bytes]]) -> tuple[EventPosition, ...]:
        """Produce messages in order, wait for every ack, and return their positions.

        Raises:
            PublishFailedError: A message was not acknowledged within the timeout or failed.
        """
        results: list[EventPosition | KafkaError | None] = [None] * len(messages)
        for index, (key, value) in enumerate(messages):
            callback = _delivery_callback(results, index)
            partition = self._next_partition()
            while True:
                try:
                    self._producer.produce(
                        self._topic,
                        value=value,
                        key=key,
                        partition=partition,
                        on_delivery=callback,
                    )
                    break
                except BufferError:
                    self._producer.poll(POLL_INTERVAL_S)
        if self._producer.flush(self._timeout_s) > 0:
            raise PublishFailedError(f"messages still queued after {self._timeout_s} s")
        positions = []
        for result in results:
            if not isinstance(result, EventPosition):
                raise PublishFailedError(f"delivery failed: {result}")
            positions.append(result)
        return tuple(positions)

    def _next_partition(self) -> int:
        """Return the next round-robin partition, or -1 to let the key choose it."""
        if self._round_robin_partitions is None:
            return UNASSIGNED_PARTITION
        partition = self._sent % self._round_robin_partitions
        self._sent += 1
        return partition


class ConcurrentPublisher(EventPublisher):
    """Publishes from many threads at once; each call waits only for its own ack.

    A background thread serves delivery callbacks; each produce call gets a Future that its
    delivery callback resolves. Call start() before publishing and close() at shutdown.
    """

    def __init__(self, producer: ProducerLike, topic: str, timeout_s: float) -> None:
        """Create a publisher that waits up to timeout_s + ACK_GRACE_S for each delivery report.

        Configure the producer's message.timeout.ms to timeout_s, so that every message gets a
        report (delivered or expired) before the caller stops waiting: a caller that is told
        the publish failed can rely on the event never reaching the log.
        """
        self._producer = producer
        self._topic = topic
        self._timeout_s = timeout_s
        self._stop = threading.Event()
        self._poller = threading.Thread(target=self._serve, name="producer-poll", daemon=True)

    def start(self) -> None:
        """Start the thread that serves delivery callbacks."""
        self._poller.start()

    def close(self) -> None:
        """Deliver queued messages for up to timeout_s seconds, then stop the poll thread."""
        self._producer.flush(self._timeout_s)
        self._stop.set()
        self._poller.join(timeout=self._timeout_s)

    def publish_raw(self, tx: Transaction) -> EventPosition:
        """Append tx without a decision record and return its position after the broker ack."""
        return self._send(*encode(tx, None))

    def publish_scored(self, scored: ScoredTransaction) -> EventPosition:
        """Append scored and return its position after the broker ack."""
        return self._send(*encode(scored.transaction, scored.record))

    def _send(self, key: bytes, value: bytes) -> EventPosition:
        """Produce one message and wait for its delivery report.

        Raises:
            PublishFailedError: The local queue is full, the broker reported an error, or no
                report arrived within the timeout.
        """
        outcome: Future[EventPosition] = Future()

        def on_delivery(error: KafkaError | None, message: Message) -> None:
            """Resolve the outcome with the position, or fail it with the broker error."""
            if error is not None:
                outcome.set_exception(PublishFailedError(f"delivery failed: {error}"))
            else:
                outcome.set_result(position_of(message))

        try:
            self._producer.produce(
                self._topic,
                value=value,
                key=key,
                partition=UNASSIGNED_PARTITION,
                on_delivery=on_delivery,
            )
        except BufferError as exc:
            raise PublishFailedError("producer queue is full") from exc
        try:
            return outcome.result(timeout=self._timeout_s + ACK_GRACE_S)
        except FutureTimeoutError as exc:
            raise PublishFailedError(f"no delivery report within {self._timeout_s} s") from exc

    def _serve(self) -> None:
        """Serve delivery callbacks every POLL_INTERVAL_S until close() is called."""
        while not self._stop.is_set():
            self._producer.poll(POLL_INTERVAL_S)


@dataclass(frozen=True, slots=True)
class RebalanceEvent:
    """One change of a consumer's assignment.

    Attributes:
        at: Wall-clock time in seconds since the epoch.
        kind: "assign", "revoke", "lost", or "poll-timeout" (this member exceeded
            max.poll.interval.ms and left the group).
        partitions: The partitions added or removed, in ascending order.
    """

    at: float
    kind: str
    partitions: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class SourceOptions:
    """Settings of a RedpandaSource.

    Attributes:
        poll_timeout_s: Seconds poll() waits for new messages.
        cooperative: True when the consumer uses the cooperative-sticky strategy, whose
            callbacks receive only the partitions that change.
        wall_clock: Timestamps rebalance events, in seconds since the epoch.
        metrics: Receives the gauge fq_consumer_lag{partition} after each consume, or None.
    """

    poll_timeout_s: float
    cooperative: bool = False
    wall_clock: Callable[[], float] = time.time
    metrics: Metrics | None = None


class RedpandaSource(EventSource):
    """Reads one topic in a consumer group and starts each partition at the store's position."""

    def __init__(
        self,
        consumer: ConsumerLike,
        topic: str,
        stored_position: Callable[[int], EventPosition | None],
        options: SourceOptions,
    ) -> None:
        """Create a source; call start() before poll().

        Args:
            consumer: Kafka consumer configured with consumer_config.
            topic: Topic to read.
            stored_position: Returns the last applied position of a partition, or None.
            options: Poll timeout, rebalance protocol, and the clock for rebalance events.
        """
        self._consumer = consumer
        self._topic = topic
        self._stored_position = stored_position
        self._poll_timeout_s = options.poll_timeout_s
        self._cooperative = options.cooperative
        self._wall_clock = options.wall_clock
        self._metrics = options.metrics
        self._buffers: dict[int, list[Event]] = {}
        self.rebalances: list[RebalanceEvent] = []
        self.transient_errors = 0

    def start(self) -> None:
        """Join the consumer group with the assignment callbacks."""
        self._consumer.subscribe(
            [self._topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )

    def poll(self, max_events: int) -> EventBatch | None:
        """Return up to max_events buffered or newly consumed events of one partition.

        Raises:
            CodecError: A message is not a valid envelope.
            PortError: The consumer reported an error.
        """
        if not any(self._buffers.values()):
            consumed: dict[int, int] = {}
            for message in self._consumer.consume(max_events, self._poll_timeout_s):
                position = self._buffer(message)
                if position is not None:
                    consumed[position.partition] = position.offset
            self._report_lag(consumed)
        for partition in sorted(self._buffers):
            events = self._buffers[partition]
            if events:
                taken, self._buffers[partition] = events[:max_events], events[max_events:]
                return EventBatch(partition=partition, events=tuple(taken))
        return None

    def seek(self, position: EventPosition) -> None:
        """Discard buffered events of position.partition and read it from position.offset."""
        self._buffers.pop(position.partition, None)
        partition = TopicPartition(self._topic, position.partition, position.offset)
        self._consumer.seek(partition)

    def acknowledge(self, batch: EventBatch) -> None:
        """Commit the group offset after batch, asynchronously, for lag reporting."""
        offset = batch.last_position.offset + 1
        partition = TopicPartition(self._topic, batch.partition, offset)
        self._consumer.commit(offsets=[partition], asynchronous=True)

    def close(self) -> None:
        """Leave the consumer group."""
        self._consumer.close()

    def _report_lag(self, consumed: dict[int, int]) -> None:
        """Set fq_consumer_lag{partition} for each partition in consumed.

        consumed maps a partition to the offset of its last consumed message. The lag is the
        number of messages in the log after it, from the high offset of the last fetch; a
        partition whose high offset is not known yet is skipped.
        """
        if self._metrics is None:
            return
        for partition, offset in consumed.items():
            topic_partition = TopicPartition(self._topic, partition)
            _, high = self._consumer.get_watermark_offsets(topic_partition, cached=True)
            if high >= 0:
                lag = max(0, high - offset - 1)
                self._metrics.set_gauge("fq_consumer_lag", lag, {"partition": str(partition)})

    def _buffer(self, message: Message) -> EventPosition | None:
        """Decode message, append it to its partition's buffer, and return its position.

        A maximum-poll-interval error is recorded as a "poll-timeout" rebalance event and
        skipped: the consumer rejoins the group on its next poll. Other non-fatal client errors
        (for example a broker connection lost) are counted in transient_errors and skipped,
        because the client reconnects by itself. Skipped messages return None.

        Raises:
            PortError: message carries a fatal error or an error that is not a KafkaError.
        """
        error = message.error()
        if isinstance(error, KafkaError) and error.code() == KafkaError._MAX_POLL_EXCEEDED:
            self._record("poll-timeout", [])
            return None
        if isinstance(error, KafkaError) and not error.fatal():
            self.transient_errors += 1
            return None
        if error is not None:
            raise PortError(f"consumer error: {error}")
        position = position_of(message)
        event = decode(message.key(), message.value(), position)
        self._buffers.setdefault(position.partition, []).append(event)
        return position

    def _on_assign(self, _consumer: object, partitions: list[TopicPartition]) -> None:
        """Start each assigned partition after its stored position, or at the beginning.

        Under the cooperative protocol the partitions are added to the assignment; under the
        eager protocol they replace it.
        """
        for partition in partitions:
            stored = self._stored_position(partition.partition)
            partition.offset = OFFSET_BEGINNING if stored is None else stored.offset + 1
            self._buffers.pop(partition.partition, None)
        self._record("assign", partitions)
        if self._cooperative:
            self._consumer.incremental_assign(partitions)
        else:
            self._consumer.assign(partitions)

    def _on_revoke(self, _consumer: object, partitions: list[TopicPartition]) -> None:
        """Discard buffered events of revoked partitions; their new owner reads them again."""
        self._record("revoke", partitions)
        for partition in partitions:
            self._buffers.pop(partition.partition, None)

    def _on_lost(self, _consumer: object, partitions: list[TopicPartition]) -> None:
        """Discard buffered events of partitions lost without a revoke (session expired)."""
        self._record("lost", partitions)
        for partition in partitions:
            self._buffers.pop(partition.partition, None)

    def _record(self, kind: str, partitions: list[TopicPartition]) -> None:
        """Append a RebalanceEvent of kind for partitions to self.rebalances."""
        numbers = tuple(sorted(partition.partition for partition in partitions))
        self.rebalances.append(RebalanceEvent(self._wall_clock(), kind, numbers))


class SnapshotConsumer(Protocol):
    """The subset of confluent_kafka.Consumer used by read_topic."""

    def get_watermark_offsets(self, partition: TopicPartition, timeout: float) -> tuple[int, int]:
        """Return the low and high watermark offsets of partition."""
        ...

    def assign(self, partitions: list[TopicPartition]) -> None:
        """Start consuming partitions at their given offsets."""
        ...

    def consume(self, num_messages: int, timeout: float) -> list[Message]:
        """Return up to num_messages messages, waiting up to timeout seconds."""
        ...


SNAPSHOT_BATCH = 1_000


def read_topic(
    consumer: SnapshotConsumer, topic: str, partitions: int, timeout_s: float
) -> list[Event]:
    """Return every event of topic's partitions, up to their end offsets when called.

    Events are sorted by partition, then offset.

    Raises:
        PortError: A consumer error, or the end offsets were not reached within timeout_s.
        CodecError: A message is not a valid envelope.
    """
    ends = {
        partition: consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout_s)[1]
        for partition in range(partitions)
    }
    consumer.assign(
        [TopicPartition(topic, p, OFFSET_BEGINNING) for p, end in ends.items() if end > 0]
    )
    next_offset = dict.fromkeys(ends, 0)
    events: list[Event] = []
    deadline = time.monotonic() + timeout_s
    while any(next_offset[p] < end for p, end in ends.items()):
        if time.monotonic() > deadline:
            raise PortError(f"read stopped at {next_offset}, end offsets {ends}")
        for message in consumer.consume(SNAPSHOT_BATCH, POLL_INTERVAL_S):
            if message.error() is not None:
                raise PortError(f"consumer error: {message.error()}")
            position = position_of(message)
            if position.offset < ends[position.partition]:
                events.append(decode(message.key(), message.value(), position))
                next_offset[position.partition] = position.offset + 1
    return sorted(events, key=lambda event: event.position)


def _delivery_callback(
    results: list[EventPosition | KafkaError | None], index: int
) -> Callable[[KafkaError | None, Message], None]:
    """Return a delivery callback that stores the outcome of message index in results."""

    def on_delivery(error: KafkaError | None, message: Message) -> None:
        """Store the error, or the message's partition and offset."""
        results[index] = error if error is not None else position_of(message)

    return on_delivery


def position_of(message: Message) -> EventPosition:
    """Return the partition and offset of a delivered or consumed message.

    Raises:
        PortError: The message has no partition or offset.
    """
    partition, offset = message.partition(), message.offset()
    if partition is None or offset is None:
        raise PortError("message has no partition or offset")
    return EventPosition(partition, offset)
