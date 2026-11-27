"""DecisionLedger and DecisionReader contract, run against every adapter."""

from __future__ import annotations

import pytest

from finspeq.application.errors import PositionGapError
from finspeq.application.messages import Event, EventBatch, EventPosition
from finspeq.domain.features import FeatureVector
from finspeq.domain.model import Transaction
from finspeq.domain.policy import Decision, Score
from finspeq.domain.scored import DecisionRecord
from tests.contract.conftest import LedgerStore
from tests.factories import BASE_TIME, make_tx


def record_for(tx: Transaction, probability: float = 0.25) -> DecisionRecord:
    """Return a REVIEW decision record with a two-value vector for tx."""
    vector = FeatureVector("v2", (tx.amount.dollars, 1.5))
    return DecisionRecord(vector, Score(probability), Decision.REVIEW, "model-1")


def scored_batch(*items: tuple[int, Transaction, bool], partition: int = 0) -> EventBatch:
    """Return a batch of (offset, transaction, scored) events on one partition."""
    events = tuple(
        Event(EventPosition(partition, offset), tx, record_for(tx) if scored else None)
        for offset, tx, scored in items
    )
    return EventBatch(partition=partition, events=events)


def test_records_scored_events_only_and_stores_the_position(ledger: LedgerStore) -> None:
    """Write one row per scored event, skip raw events, and store the last offset."""
    first, raw, second = make_tx(1, BASE_TIME), make_tx(2, BASE_TIME + 1), make_tx(3)
    result = ledger.record(scored_batch((0, first, True), (1, raw, False), (2, second, True)))
    assert (result.applied, result.skipped) == (2, 1)
    assert ledger.position(0) == EventPosition(0, 2)
    rows = list(ledger.rows())
    assert [row.tx_id for row in rows] == sorted([first.tx_id, second.tx_id])
    row = next(r for r in rows if r.tx_id == first.tx_id)
    assert (row.card, row.time, row.amount_cents) == (first.card.value, first.time, 1_000)
    assert row.record == record_for(first)
    assert row.position == EventPosition(0, 0)


def test_redelivered_batch_changes_nothing(ledger: LedgerStore) -> None:
    """Skip every event at or below the stored position."""
    batch = scored_batch((0, make_tx(1), True), (1, make_tx(2), True))
    ledger.record(batch)
    result = ledger.record(batch)
    assert (result.applied, result.skipped) == (0, 2)
    assert len(list(ledger.rows())) == 2


def test_first_row_of_a_tx_id_wins(ledger: LedgerStore) -> None:
    """Keep the first row when the same transaction is logged again at a later offset."""
    tx = make_tx(1)
    ledger.record(scored_batch((0, tx, True)))
    result = ledger.record(scored_batch((1, tx, True)))
    assert (result.applied, result.skipped) == (0, 1)
    assert ledger.position(0) == EventPosition(0, 1)
    assert [row.position.offset for row in ledger.rows()] == [0]


def test_gap_raises_and_writes_nothing(ledger: LedgerStore) -> None:
    """Raise PositionGapError for a skipped offset; rows and position stay unchanged."""
    ledger.record(scored_batch((0, make_tx(1), True)))
    with pytest.raises(PositionGapError):
        ledger.record(scored_batch((2, make_tx(2), True), (3, make_tx(3), True)))
    assert ledger.position(0) == EventPosition(0, 0)
    assert len(list(ledger.rows())) == 1


def test_positions_are_tracked_per_partition(ledger: LedgerStore) -> None:
    """Store an independent position per partition; None where nothing was recorded."""
    ledger.record(scored_batch((4, make_tx(1), True)))
    ledger.record(scored_batch((9, make_tx(2), True), partition=1))
    assert ledger.position(0) == EventPosition(0, 4)
    assert ledger.position(1) == EventPosition(1, 9)
    assert ledger.position(2) is None
