"""Fixtures that provide every adapter of each port to the contract suites."""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Protocol

import fakeredis
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import redis

from finspeq.adapters.outbound.memory_history import MemoryHistoricalSource, MemoryTrainingSink
from finspeq.adapters.outbound.memory_ledger import MemoryLedger
from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.adapters.outbound.parquet import ParquetHistoricalSource, ParquetTrainingSink
from finspeq.adapters.outbound.postgres_ledger import PostgresLedger
from finspeq.adapters.outbound.redis_window import RedisWindowStore
from finspeq.application.messages import LabeledTransaction, Split, TrainingRow
from finspeq.application.ports import (
    DecisionLedger,
    DecisionReader,
    HistoricalSource,
    TrainingSink,
    WindowAppender,
    WindowStore,
)
from finspeq.domain.features import FeatureVector
from finspeq.domain.model import EventTime, TxId


class AppendingStore(WindowStore, WindowAppender, Protocol):
    """A window store that also implements WindowAppender, as every adapter here does."""


StoreFactory = Callable[[int], AppendingStore]
SourceFactory = Callable[[Sequence[LabeledTransaction]], HistoricalSource]
SINK_SCHEMA = "v1"
SINK_FEATURES = ("f0", "f1", "f2")


LIVE_REDIS_ENV = "FINSPEQ_REDIS_URL"
LIVE_REDIS_DB = 15
TEST_TOPIC = "contract"


def _memory_store(allowed_lateness_s: int) -> AppendingStore:
    """Return a new MemoryWindowStore with the given allowed lateness."""
    return MemoryWindowStore(allowed_lateness_s=allowed_lateness_s)


def _fake_redis_store(allowed_lateness_s: int) -> AppendingStore:
    """Return a RedisWindowStore over a new, empty in-process fakeredis server."""
    return RedisWindowStore(fakeredis.FakeRedis(), TEST_TOPIC, allowed_lateness_s)


def _live_redis_store(allowed_lateness_s: int) -> AppendingStore:
    """Return a RedisWindowStore over database 15 of the server in FINSPEQ_REDIS_URL, flushed."""
    url = os.environ.get(LIVE_REDIS_ENV)
    if not url:
        pytest.skip(f"{LIVE_REDIS_ENV} is not set")
    client = redis.Redis.from_url(url, db=LIVE_REDIS_DB)
    client.flushdb()
    return RedisWindowStore(client, TEST_TOPIC, allowed_lateness_s)


@pytest.fixture(
    params=[
        _memory_store,
        _fake_redis_store,
        pytest.param(_live_redis_store, marks=pytest.mark.integration),
    ],
    ids=["memory", "fakeredis", "redis"],
)
def make_store(request: pytest.FixtureRequest) -> StoreFactory:
    """Return a factory that creates an empty store for the parametrized adapter."""
    factory: StoreFactory = request.param
    return factory


class LedgerStore(DecisionLedger, DecisionReader, Protocol):
    """A ledger that also implements DecisionReader, as every adapter here does."""


LIVE_POSTGRES_ENV = "FINSPEQ_POSTGRES_DSN"
SCHEMA_DIR = Path(__file__).parents[2] / "infra" / "postgres" / "init"


@pytest.fixture(params=["memory", pytest.param("postgres", marks=pytest.mark.integration)])
def ledger(request: pytest.FixtureRequest) -> LedgerStore:
    """Return an empty ledger of the parametrized adapter.

    The Postgres ledger uses the database in FINSPEQ_POSTGRES_DSN, applies every schema script
    in infra/postgres/init (all idempotent), and empties both tables.
    """
    if request.param == "memory":
        return MemoryLedger()
    dsn = os.environ.get(LIVE_POSTGRES_ENV)
    if not dsn:
        pytest.skip(f"{LIVE_POSTGRES_ENV} is not set")
    connection = psycopg.connect(dsn, autocommit=True)
    request.addfinalizer(connection.close)
    for script in sorted(SCHEMA_DIR.glob("*.sql")):
        connection.execute(script.read_text(encoding="utf-8"))
    connection.execute("TRUNCATE decisions, consumer_positions")
    return PostgresLedger(connection, "contract", TEST_TOPIC)


@pytest.fixture(params=["memory", "parquet"])
def make_source(request: pytest.FixtureRequest, tmp_path: Path) -> SourceFactory:
    """Return a factory that builds the parametrized HistoricalSource over given items."""
    if request.param == "memory":
        return MemoryHistoricalSource

    def factory(items: Sequence[LabeledTransaction]) -> HistoricalSource:
        """Write items to an event Parquet file and return a source over it."""
        path = tmp_path / "events.parquet"
        _write_events(path, items)
        return ParquetHistoricalSource(path)

    return factory


def _write_events(path: Path, items: Sequence[LabeledTransaction]) -> None:
    """Write items in the column layout produced by scripts/sort_events.py."""
    txs = [item.transaction for item in items]
    columns = {
        "seq": [item.seq for item in items],
        "trans_num": [tx.tx_id.value for tx in txs],
        "cc_num": [tx.card.value for tx in txs],
        "unix_time": [tx.time.epoch_s for tx in txs],
        "category": [tx.category.value for tx in txs],
        "amt": [tx.amount.cents / 100 for tx in txs],
        "merchant": [tx.merchant for tx in txs],
        "merch_lat": [tx.merchant_point.lat for tx in txs],
        "merch_long": [tx.merchant_point.lon for tx in txs],
        "lat": [tx.home_point.lat for tx in txs],
        "long": [tx.home_point.lon for tx in txs],
        "is_fraud": [int(item.is_fraud) for item in items],
    }
    pq.write_table(pa.table(columns), path, row_group_size=2)


class SinkHarness(Protocol):
    """Creates a TrainingSink and reads back the rows it has made visible."""

    def create(self) -> TrainingSink:
        """Return a new empty sink for schema SINK_SCHEMA with three features."""
        ...

    def read_back(self) -> list[TrainingRow]:
        """Return the rows the created sink has made visible; empty before close."""
        ...


class _MemorySinkHarness:
    """SinkHarness for MemoryTrainingSink."""

    def __init__(self) -> None:
        """Create a harness with a MemoryTrainingSink for schema SINK_SCHEMA."""
        self._sink = MemoryTrainingSink(SINK_SCHEMA, len(SINK_FEATURES))

    def create(self) -> TrainingSink:
        """Return the harness's MemoryTrainingSink."""
        return self._sink

    def read_back(self) -> list[TrainingRow]:
        """Return the sink's published rows."""
        return list(self._sink.rows)


class _ParquetSinkHarness:
    """SinkHarness for ParquetTrainingSink writing into a temporary directory."""

    def __init__(self, directory: Path) -> None:
        """Create a harness that writes to directory/part.parquet."""
        self._path = directory / "part.parquet"

    def create(self) -> TrainingSink:
        """Return a new ParquetTrainingSink."""
        return ParquetTrainingSink(self._path, SINK_SCHEMA, SINK_FEATURES, rows_per_group=2)

    def read_back(self) -> list[TrainingRow]:
        """Return the rows stored in the final file, or none if it does not exist yet."""
        if not self._path.exists():
            return []
        data = pq.read_table(self._path).to_pydict()
        return [
            TrainingRow(
                seq=data["seq"][i],
                time=EventTime(data["time"][i]),
                tx_id=TxId(data["tx_id"][i]),
                vector=FeatureVector(SINK_SCHEMA, tuple(data[f][i] for f in SINK_FEATURES)),
                is_fraud=data["is_fraud"][i],
                split=Split(data["split"][i]),
            )
            for i in range(len(data["seq"]))
        ]


@pytest.fixture(params=["memory", "parquet"])
def sink_harness(request: pytest.FixtureRequest, tmp_path: Path) -> SinkHarness:
    """Return the harness for the parametrized TrainingSink adapter."""
    if request.param == "memory":
        return _MemorySinkHarness()
    return _ParquetSinkHarness(tmp_path)
