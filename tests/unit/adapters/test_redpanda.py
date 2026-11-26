"""Tests for RedpandaPublisher and RedpandaSource with in-process fakes."""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

import pytest

pytest.importorskip("confluent_kafka")

from confluent_kafka import OFFSET_BEGINNING, KafkaError, Message, TopicPartition

from finspeq.adapters.codec import encode
from finspeq.adapters.outbound.redpanda import RedpandaPublisher, RedpandaSource
from finspeq.application.errors import PortError, PublishFailedError
from finspeq.application.messages import EventPosition
from finspeq.domain.model import Transaction
from tests.factories import OTHER_CARD, batch, make_tx

TOPIC = "t"
Callback = Callable[[KafkaError | None, Message], None]


class FakeMessage:
    """Consumed or delivered message with fixed fields."""

    def __init__(
        self, partition: int, offset: int, key: bytes, value: bytes, error: str | None = None
    ) -> None:
        """Store the message fields; error is returned by error()."""
        self._fields = (partition, offset, key, value)
        self._error = error

    def partition(self) -> int:
        """Return the partition."""
        return self._fields[0]

    def offset(self) -> int:
        """Return the offset."""
        return self._fields[1]

    def key(self) -> bytes:
        """Return the key."""
        return self._fields[2]

    def value(self) -> bytes:
        """Return the value."""
        return self._fields[3]

    def error(self) -> str | None:
        """Return the stored error, or None."""
        return self._error


class FakeProducer:
    """Producer that assigns partition len(key) % 2 and acknowledges on flush."""

    def __init__(self, fail: bool = False) -> None:
        """Create a producer; with fail=True every delivery reports an error."""
        self._fail = fail
        self._queued: list[tuple[bytes, bytes, Callback]] = []
        self._offsets = [0, 0]

    def produce(self, topic: str, *, value: bytes, key: bytes, on_delivery: Callback) -> None:
        """Queue a message."""
        assert topic == TOPIC
        self._queued.append((key, value, on_delivery))

    def poll(self, timeout: float) -> int:
        """Serve nothing; check the timeout is non-negative."""
        assert timeout >= 0
        return 0

    def flush(self, timeout: float) -> int:
        """Deliver every queued message and return 0; check the timeout is positive."""
        assert timeout > 0
        for key, value, callback in self._queued:
            partition = len(key) % 2
            message = FakeMessage(partition, self._offsets[partition], key, value)
            self._offsets[partition] += 1
            error = cast(KafkaError, "broker down") if self._fail else None
            callback(error, cast(Message, message))
        self._queued.clear()
        return 0


class FakeConsumer:
    """Consumer that returns preset messages and records assignment, seeks, and commits."""

    def __init__(self, messages: list[FakeMessage]) -> None:
        """Create a consumer that returns messages on the first consume call."""
        self._messages = messages
        self.assigned: list[TopicPartition] = []
        self.commits: list[TopicPartition] = []
        self.seeks: list[TopicPartition] = []
        self.asynchronous: list[bool] = []
        self.on_assign: Callable[[object, list[TopicPartition]], None] | None = None
        self.on_revoke: Callable[[object, list[TopicPartition]], None] | None = None

    def subscribe(
        self,
        topics: list[str],
        on_assign: Callable[[object, list[TopicPartition]], None],
        on_revoke: Callable[[object, list[TopicPartition]], None],
    ) -> None:
        """Store the callbacks."""
        assert topics == [TOPIC]
        self.on_assign, self.on_revoke = on_assign, on_revoke

    def assign(self, partitions: list[TopicPartition]) -> None:
        """Record the assignment."""
        self.assigned = partitions

    def consume(self, num_messages: int, timeout: float) -> list[Message]:
        """Return up to num_messages preset messages; check the timeout is non-negative."""
        assert timeout >= 0
        messages = self._messages[:num_messages]
        self._messages = self._messages[num_messages:]
        return cast(list[Message], messages)

    def seek(self, partition: TopicPartition) -> None:
        """Record the seek."""
        self.seeks.append(partition)

    def commit(self, *, offsets: list[TopicPartition], asynchronous: bool) -> None:
        """Record the committed offsets and whether the commit was asynchronous."""
        self.commits.extend(offsets)
        self.asynchronous.append(asynchronous)

    def close(self) -> None:
        """Do nothing."""


def message(partition: int, offset: int, tx: Transaction) -> FakeMessage:
    """Return a fake message holding tx's raw envelope."""
    key, value = encode(tx, None)
    return FakeMessage(partition, offset, key, value)


def test_publisher_returns_positions_in_order() -> None:
    """Return one acknowledged position per transaction, in input order."""
    publisher = RedpandaPublisher(FakeProducer(), TOPIC, timeout_s=1.0)
    positions = publisher.publish_raw_many([make_tx(1), make_tx(2), make_tx(3)])
    assert [p.offset for p in positions] == [0, 1, 2]
    assert publisher.publish_raw(make_tx(4)).offset == 3


def test_publisher_raises_on_failed_delivery() -> None:
    """Raise PublishFailedError when the broker reports an error."""
    publisher = RedpandaPublisher(FakeProducer(fail=True), TOPIC, timeout_s=1.0)
    with pytest.raises(PublishFailedError):
        publisher.publish_raw(make_tx(1))


def test_assignment_starts_after_stored_position() -> None:
    """Start a partition after its stored offset, or at the beginning without one."""
    consumer = FakeConsumer([])
    stored = {0: EventPosition(0, 41)}
    source = RedpandaSource(consumer, TOPIC, stored.get, poll_timeout_s=0.0)
    source.start()
    assert consumer.on_assign is not None
    consumer.on_assign(consumer, [TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 1)])
    assert [p.offset for p in consumer.assigned] == [42, OFFSET_BEGINNING]


def test_poll_returns_one_partition_at_a_time() -> None:
    """Split consumed messages into single-partition batches, lowest partition first."""
    messages = [
        message(1, 5, make_tx(1, card=OTHER_CARD)),
        message(0, 9, make_tx(2)),
        message(1, 6, make_tx(3, card=OTHER_CARD)),
    ]
    source = RedpandaSource(FakeConsumer(messages), TOPIC, lambda _: None, poll_timeout_s=0.0)
    first, second = source.poll(10), source.poll(10)
    assert first is not None and second is not None
    assert (first.partition, [e.position.offset for e in first.events]) == (0, [9])
    assert (second.partition, [e.position.offset for e in second.events]) == (1, [5, 6])
    assert source.poll(10) is None


def test_revoke_discards_buffered_events() -> None:
    """Drop the buffered events of a revoked partition and keep the others' delivered."""
    consumer = FakeConsumer([message(0, 0, make_tx(1)), message(1, 0, make_tx(2, card=OTHER_CARD))])
    source = RedpandaSource(consumer, TOPIC, lambda _: None, poll_timeout_s=0.0)
    source.start()
    first = source.poll(5)
    assert first is not None and first.partition == 0
    assert consumer.on_revoke is not None
    consumer.on_revoke(consumer, [TopicPartition(TOPIC, 1)])
    assert source.poll(5) is None


def test_acknowledge_commits_next_offset() -> None:
    """Commit the offset after the batch's last event."""
    consumer = FakeConsumer([])
    source = RedpandaSource(consumer, TOPIC, lambda _: None, poll_timeout_s=0.0)
    source.acknowledge(batch((7, make_tx(1)), (8, make_tx(2))))
    assert [(c.partition, c.offset) for c in consumer.commits] == [(0, 9)]
    assert consumer.asynchronous == [True]


def test_consumer_error_is_raised() -> None:
    """Raise PortError for a message that carries an error."""
    broken = FakeMessage(0, 0, b"k", b"v", error="broker transport failure")
    source = RedpandaSource(FakeConsumer([broken]), TOPIC, lambda _: None, 0.0)
    with pytest.raises(PortError):
        source.poll(1)
