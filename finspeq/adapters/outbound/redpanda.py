"""Kafka-protocol (Redpanda) implementations of EventPublisher, BulkPublisher, and EventSource."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Literal, Protocol

from confluent_kafka import OFFSET_BEGINNING, KafkaError, Message, TopicPartition

from finspeq.adapters.codec import decode, encode
from finspeq.application.errors import PortError, PublishFailedError
from finspeq.application.messages import Event, EventBatch, EventPosition
from finspeq.application.ports import BulkPublisher, EventPublisher, EventSource
from finspeq.domain.model import Transaction
from finspeq.domain.scored import ScoredTransaction

POLL_INTERVAL_S = 0.05


def producer_config(bootstrap: str, linger_ms: int) -> dict[str, str | int | bool]:
    """Return producer settings: idempotent, acks=all, murmur2 partitioning, IPv4 only."""
    return {
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


def consumer_config(
    bootstrap: str, group: str, instance_id: str | None = None
) -> dict[str, str | int | bool]:
    """Return consumer settings: manual offsets, start at the beginning, committed reads, IPv4.

    The 6 s session timeout bounds how long a crashed member holds its partitions. With
    instance_id set, the consumer is a static member: a restart with the same id gets its
    partitions back without waiting for the session timeout.
    """
    config: dict[str, str | int | bool] = {
        "bootstrap.servers": bootstrap,
        "broker.address.family": "v4",
        "group.id": group,
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
        on_delivery: Callable[[KafkaError | None, Message], None],
    ) -> None:
        """Queue one message for delivery."""
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
    ) -> None:
        """Join the group for topics with assignment callbacks."""
        ...

    def assign(self, partitions: list[TopicPartition]) -> None:
        """Start consuming partitions at their given offsets."""
        ...

    def consume(self, num_messages: int, timeout: float) -> list[Message]:
        """Return up to num_messages messages, waiting up to timeout seconds."""
        ...

    def seek(self, partition: TopicPartition) -> None:
        """Move the read position of an assigned partition."""
        ...

    def commit(self, *, offsets: list[TopicPartition], asynchronous: Literal[True]) -> None:
        """Commit group offsets."""
        ...

    def close(self) -> None:
        """Leave the group and release resources."""
        ...


class RedpandaPublisher(EventPublisher, BulkPublisher):
    """Writes transactions to one topic, keyed by card number."""

    def __init__(self, producer: ProducerLike, topic: str, timeout_s: float) -> None:
        """Create a publisher that waits up to timeout_s seconds for broker acknowledgements."""
        self._producer = producer
        self._topic = topic
        self._timeout_s = timeout_s

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
            while True:
                try:
                    self._producer.produce(self._topic, value=value, key=key, on_delivery=callback)
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


class RedpandaSource(EventSource):
    """Reads one topic in a consumer group and starts each partition at the store's position."""

    def __init__(
        self,
        consumer: ConsumerLike,
        topic: str,
        stored_position: Callable[[int], EventPosition | None],
        poll_timeout_s: float,
    ) -> None:
        """Create a source; call start() before poll().

        Args:
            consumer: Kafka consumer configured with consumer_config.
            topic: Topic to read.
            stored_position: Returns the last applied position of a partition, or None.
            poll_timeout_s: Seconds poll() waits for new messages.
        """
        self._consumer = consumer
        self._topic = topic
        self._stored_position = stored_position
        self._poll_timeout_s = poll_timeout_s
        self._buffers: dict[int, list[Event]] = {}

    def start(self) -> None:
        """Join the consumer group with the assignment callbacks."""
        self._consumer.subscribe(
            [self._topic], on_assign=self._on_assign, on_revoke=self._on_revoke
        )

    def poll(self, max_events: int) -> EventBatch | None:
        """Return up to max_events buffered or newly consumed events of one partition.

        Raises:
            CodecError: A message is not a valid envelope.
            PortError: The consumer reported an error.
        """
        if not any(self._buffers.values()):
            for message in self._consumer.consume(max_events, self._poll_timeout_s):
                self._buffer(message)
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

    def _buffer(self, message: Message) -> None:
        """Decode message and append it to its partition's buffer.

        Raises:
            PortError: message carries a consumer error.
        """
        error = message.error()
        if error is not None:
            raise PortError(f"consumer error: {error}")
        position = position_of(message)
        event = decode(message.key(), message.value(), position)
        self._buffers.setdefault(position.partition, []).append(event)

    def _on_assign(self, _consumer: object, partitions: list[TopicPartition]) -> None:
        """Start each assigned partition after its stored position, or at the beginning."""
        for partition in partitions:
            stored = self._stored_position(partition.partition)
            partition.offset = OFFSET_BEGINNING if stored is None else stored.offset + 1
            self._buffers.pop(partition.partition, None)
        self._consumer.assign(partitions)

    def _on_revoke(self, _consumer: object, partitions: list[TopicPartition]) -> None:
        """Discard buffered events of revoked partitions; their new owner reads them again."""
        for partition in partitions:
            self._buffers.pop(partition.partition, None)


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
