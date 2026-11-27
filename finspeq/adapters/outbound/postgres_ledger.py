"""Postgres implementation of DecisionLedger and DecisionReader (psycopg 3)."""

from __future__ import annotations

import json
from collections.abc import Iterator

import psycopg

from finspeq.application.errors import StoreUnavailableError
from finspeq.application.messages import (
    ApplyResult,
    Event,
    EventBatch,
    EventPosition,
    LedgerRow,
)
from finspeq.application.ports import DecisionLedger, DecisionReader
from finspeq.application.positions import unapplied_events
from finspeq.domain.features import FeatureVector
from finspeq.domain.model import EventTime, TxId
from finspeq.domain.policy import Decision, Score
from finspeq.domain.scored import DecisionRecord

LOCK_POSITION = "SELECT pg_advisory_xact_lock(hashtext(%s))"
SELECT_POSITION = """
SELECT last_offset FROM consumer_positions
WHERE consumer = %s AND topic = %s AND topic_partition = %s
"""
INSERT_DECISIONS = """
INSERT INTO decisions (
    tx_id, card, event_time, amount_cents, fraud_probability, decision, model_version,
    feature_schema, features, topic_partition, topic_offset
)
SELECT * FROM unnest(
    %s::char(32)[], %s::varchar(19)[], %s::bigint[], %s::bigint[], %s::double precision[],
    %s::text[], %s::text[], %s::text[], %s::jsonb[], %s::integer[], %s::bigint[]
)
ON CONFLICT (tx_id) DO NOTHING
"""
UPSERT_POSITION = """
INSERT INTO consumer_positions (consumer, topic, topic_partition, last_offset)
VALUES (%s, %s, %s, %s)
ON CONFLICT (consumer, topic, topic_partition) DO UPDATE SET last_offset = EXCLUDED.last_offset
"""
SELECT_ROWS = """
SELECT tx_id, card, event_time, amount_cents, fraud_probability, decision, model_version,
       feature_schema, features, topic_partition, topic_offset
FROM decisions ORDER BY tx_id
"""


class PostgresLedger(DecisionLedger, DecisionReader):
    """Decisions in the `decisions` table and positions in `consumer_positions`.

    Each record() call is one SQL transaction: lock the partition's position, check offsets,
    insert the new rows (the first row of a tx_id wins), and store the position.
    """

    def __init__(self, connection: psycopg.Connection, consumer: str, topic: str) -> None:
        """Create a ledger over an open connection.

        Args:
            connection: Connection used for every call; not shared with other threads.
            consumer: Name of this ledger's position rows (one per consumer and topic).
            topic: Topic whose positions are stored.
        """
        self._connection = connection
        self._consumer = consumer
        self._topic = topic

    def record(self, batch: EventBatch) -> ApplyResult:
        """Store one row per new scored event and the batch position in one transaction.

        Semantics match DecisionLedger.record.

        Raises:
            PositionGapError: The unapplied offsets do not continue from the stored position.
                The transaction is rolled back.
            StoreUnavailableError: Postgres did not respond. The transaction is rolled back.
        """
        try:
            with self._connection.transaction(), self._connection.cursor() as cursor:
                key = f"{self._consumer}|{self._topic}|{batch.partition}"
                cursor.execute(LOCK_POSITION, (key,))
                stored = self._stored_offset(cursor, batch.partition)
                pending = unapplied_events(batch, stored)
                if not pending:
                    position = None if stored is None else EventPosition(batch.partition, stored)
                    return ApplyResult(0, len(batch.events), position, 0)
                cursor.execute(INSERT_DECISIONS, _columns(pending))
                inserted = max(cursor.rowcount, 0)
                last = pending[-1].position
                cursor.execute(
                    UPSERT_POSITION, (self._consumer, self._topic, last.partition, last.offset)
                )
        except psycopg.OperationalError as exc:
            raise StoreUnavailableError(str(exc)) from exc
        return ApplyResult(inserted, len(batch.events) - inserted, last, 0)

    def position(self, partition: int) -> EventPosition | None:
        """Return the last recorded position for partition, or None if nothing was recorded.

        Raises:
            StoreUnavailableError: Postgres did not respond.
        """
        try:
            with self._connection.transaction(), self._connection.cursor() as cursor:
                stored = self._stored_offset(cursor, partition)
        except psycopg.OperationalError as exc:
            raise StoreUnavailableError(str(exc)) from exc
        return None if stored is None else EventPosition(partition, stored)

    def rows(self) -> Iterator[LedgerRow]:
        """Yield every recorded decision in tx_id order.

        Raises:
            StoreUnavailableError: Postgres did not respond.
        """
        try:
            with self._connection.transaction(), self._connection.cursor() as cursor:
                cursor.execute(SELECT_ROWS)
                fetched = cursor.fetchall()
        except psycopg.OperationalError as exc:
            raise StoreUnavailableError(str(exc)) from exc
        for row in fetched:
            yield _ledger_row(row)

    def _stored_offset(self, cursor: psycopg.Cursor, partition: int) -> int | None:
        """Return the stored last offset of partition, or None if there is none."""
        cursor.execute(SELECT_POSITION, (self._consumer, self._topic, partition))
        found = cursor.fetchone()
        return None if found is None else int(found[0])


def _columns(events: tuple[Event, ...]) -> tuple[list[object], ...]:
    """Return the eleven column arrays of INSERT_DECISIONS for the scored events."""
    columns: tuple[list[object], ...] = tuple([] for _ in range(11))
    for event in events:
        tx, record = event.transaction, event.record
        if record is None:
            continue
        values: tuple[object, ...] = (
            tx.tx_id.value,
            tx.card.value,
            tx.time.epoch_s,
            tx.amount.cents,
            record.score.fraud_probability,
            record.decision.value,
            record.model_version,
            record.vector.schema,
            json.dumps(list(record.vector.values)),
            event.position.partition,
            event.position.offset,
        )
        for column, value in zip(columns, values, strict=True):
            column.append(value)
    return columns


def _ledger_row(row: tuple[object, ...]) -> LedgerRow:
    """Return the LedgerRow stored in one SELECT_ROWS result row."""
    (tx_id, card, time, cents, probability, decision, model, schema, features, part, off) = row
    values = features if isinstance(features, list) else json.loads(str(features))
    record = DecisionRecord(
        vector=FeatureVector(str(schema), tuple(float(v) for v in values)),
        score=Score(float(str(probability))),
        decision=Decision(str(decision)),
        model_version=str(model),
    )
    return LedgerRow(
        tx_id=TxId(str(tx_id)),
        card=str(card).strip(),
        time=EventTime(int(str(time))),
        amount_cents=int(str(cents)),
        record=record,
        position=EventPosition(int(str(part)), int(str(off))),
    )
