"""End to end on the compose stack: publish, consume with the worker, verify Redis state.

Runs only when FINSPEQ_KAFKA_BOOTSTRAP and FINSPEQ_REDIS_URL are set. Uses Redis database 15,
which it empties, and a new topic per run, which it deletes.
"""

from __future__ import annotations

import os
import threading
import uuid
from collections.abc import Iterator

import pytest

pytest.importorskip("confluent_kafka")
pytest.importorskip("redis")

import redis
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from finspeq.adapters.inbound.feature_worker import FeatureWorker, WorkerLimits
from finspeq.adapters.outbound.clock import SystemClock
from finspeq.adapters.outbound.kafka_partitioner import key_partition
from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.adapters.outbound.null_metrics import NullMetrics
from finspeq.adapters.outbound.redis_window import RedisWindowStore
from finspeq.adapters.outbound.redpanda import (
    RedpandaPublisher,
    RedpandaSource,
    SourceOptions,
    consumer_config,
    producer_config,
)
from finspeq.application.apply_events import ApplyEvents
from finspeq.application.messages import LabeledTransaction
from finspeq.application.verify import VerifyFinalState
from tests.factories import BASE_TIME, make_tx

pytestmark = pytest.mark.integration
PARTITIONS = 3
EVENTS = 300
CARDS = [f"42000000000{n:05d}" for n in range(30)]


@pytest.fixture
def topic() -> Iterator[str]:
    """Create a new topic with PARTITIONS partitions and delete it afterwards."""
    bootstrap = os.environ.get("FINSPEQ_KAFKA_BOOTSTRAP")
    if not bootstrap or not os.environ.get("FINSPEQ_REDIS_URL"):
        pytest.skip("FINSPEQ_KAFKA_BOOTSTRAP and FINSPEQ_REDIS_URL must be set")
    admin = AdminClient({"bootstrap.servers": bootstrap})
    name = f"finspeq-it-{uuid.uuid4().hex[:8]}"
    admin.create_topics([NewTopic(name, num_partitions=PARTITIONS, replication_factor=1)])[
        name
    ].result(timeout=30)
    yield name
    admin.delete_topics([name])[name].result(timeout=30)


def history() -> list[LabeledTransaction]:
    """Return EVENTS transactions over CARDS, one every 7 minutes, in canonical order."""
    return [
        LabeledTransaction(
            i,
            make_tx(i + 1, BASE_TIME + i * 420, cents=100 + i, card=CARDS[i % len(CARDS)]),
            is_fraud=False,
        )
        for i in range(EVENTS)
    ]


def test_worker_state_matches_reference_replay(topic: str) -> None:
    """Publish EVENTS transactions, run the worker until idle, and match the memory replay."""
    bootstrap = os.environ["FINSPEQ_KAFKA_BOOTSTRAP"]
    client = redis.Redis.from_url(os.environ["FINSPEQ_REDIS_URL"], db=15)
    client.flushdb()
    items = history()
    publisher = RedpandaPublisher(Producer(producer_config(bootstrap, 5)), topic, 30.0)
    positions = publisher.publish_raw_many([item.transaction for item in items])
    assert {p.partition for p in positions} == set(range(PARTITIONS))

    store = RedisWindowStore(client, topic)
    consumer = Consumer(consumer_config(bootstrap, f"{topic}-group"))
    source = RedpandaSource(consumer, topic, store.position, SourceOptions(0.5))
    source.start()
    clock = SystemClock()
    limits = WorkerLimits(batch_size=50, idle_exit_s=5.0, start_timeout_s=60.0)
    worker = FeatureWorker(source, ApplyEvents(store, NullMetrics(), clock), clock, limits)
    try:
        report = worker.run(threading.Event())
    finally:
        source.close()
    assert report.events == EVENTS
    assert report.applied == EVENTS

    state = VerifyFinalState(MemoryWindowStore(), store, PARTITIONS).execute(items, 64)
    assert state.passed, state.first_mismatch


def test_broker_partitions_match_python_murmur2(topic: str) -> None:
    """Every keyed event lands in the partition the Python murmur2 partitioner predicts."""
    bootstrap = os.environ["FINSPEQ_KAFKA_BOOTSTRAP"]
    publisher = RedpandaPublisher(Producer(producer_config(bootstrap, 5)), topic, 30.0)
    txs = [item.transaction for item in history()]
    positions = publisher.publish_raw_many(txs)
    predicted = [key_partition(tx.card.value.encode("ascii"), PARTITIONS) for tx in txs]
    assert [position.partition for position in positions] == predicted
    assert len(set(predicted)) == PARTITIONS
