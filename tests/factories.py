"""Builders of valid domain objects and messages for tests."""

from __future__ import annotations

from finspeq.application.messages import Event, EventBatch, EventPosition
from finspeq.domain.model import (
    CardId,
    Category,
    EventTime,
    GeoPoint,
    Money,
    Transaction,
    TxId,
)
from finspeq.domain.window import CardWindow, WindowEntry

BASE_TIME = 1_750_000_000
CARD = "4000000000000002"
OTHER_CARD = "4000000000000010"
HOME = GeoPoint(45.0, -90.0)


def tx_id(n: int) -> TxId:
    """Return the TxId whose value is n as 32 zero-padded hex digits."""
    return TxId(f"{n:032x}")


def make_tx(
    n: int,
    time: int = BASE_TIME,
    cents: int = 1_000,
    card: str = CARD,
    merchant: str = "merchant_a",
) -> Transaction:
    """Return a grocery_pos transaction with id tx_id(n) at the home location."""
    return Transaction(
        tx_id=tx_id(n),
        card=CardId(card),
        time=EventTime(time),
        amount=Money(cents),
        merchant=merchant,
        category=Category.GROCERY_POS,
        merchant_point=HOME,
        home_point=HOME,
    )


def window_for(tx: Transaction, history: list[Transaction]) -> CardWindow:
    """Return tx's window built from history, which must already be inside the window."""
    entries = tuple(
        sorted((WindowEntry.from_transaction(h) for h in history), key=lambda e: e.order_key)
    )
    return CardWindow(card=tx.card, until=tx.order_key, entries=entries)


def batch(*pairs: tuple[int, Transaction], partition: int = 0) -> EventBatch:
    """Return a batch of events from (offset, transaction) pairs on one partition."""
    events = tuple(
        Event(position=EventPosition(partition, offset), transaction=tx) for offset, tx in pairs
    )
    return EventBatch(partition=partition, events=events)
