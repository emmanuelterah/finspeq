"""WindowReader and WindowWriter contract, run against every adapter."""

from __future__ import annotations

import pytest

from finspeq.application.errors import PositionGapError
from finspeq.application.messages import EventPosition
from finspeq.domain.window import WINDOW_SECONDS
from tests.contract.conftest import StoreFactory
from tests.factories import BASE_TIME, OTHER_CARD, batch, make_tx


def test_read_returns_only_entries_before_until(make_store: StoreFactory) -> None:
    """Return the card's earlier entries and exclude later ones and other cards."""
    store = make_store(0)
    early, current, late = make_tx(1, BASE_TIME - 60), make_tx(2), make_tx(3, BASE_TIME + 60)
    other = make_tx(4, BASE_TIME - 30, card=OTHER_CARD)
    store.apply(batch((0, early), (1, current), (2, late), (3, other)))
    window = store.read(current.card, current.order_key)
    assert [e.tx_id for e in window.entries] == [early.tx_id]


def test_read_orders_same_second_entries_by_tx_id(make_store: StoreFactory) -> None:
    """Return entries with equal time in tx id order regardless of arrival order."""
    store = make_store(0)
    store.apply(batch((0, make_tx(3, BASE_TIME - 5)), (1, make_tx(2, BASE_TIME - 5))))
    current = make_tx(9)
    window = store.read(current.card, current.order_key)
    assert [e.tx_id for e in window.entries] == [make_tx(2).tx_id, make_tx(3).tx_id]


def test_reapplying_stored_batch_changes_nothing(make_store: StoreFactory) -> None:
    """Apply a batch twice and get the same state and position as one apply."""
    store = make_store(0)
    events = batch((0, make_tx(1, BASE_TIME - 60)), (1, make_tx(2, BASE_TIME - 30)))
    store.apply(events)
    result = store.apply(events)
    current = make_tx(9)
    assert (result.applied, result.skipped) == (0, 2)
    assert len(store.read(current.card, current.order_key).entries) == 2
    assert store.position(0) == EventPosition(0, 1)


def test_duplicate_tx_id_in_later_batch_is_stored_once(make_store: StoreFactory) -> None:
    """Store a transaction once when it arrives again at a new offset."""
    store = make_store(0)
    tx = make_tx(1, BASE_TIME - 60)
    store.apply(batch((0, tx)))
    result = store.apply(batch((1, tx)))
    current = make_tx(9)
    assert (result.applied, result.skipped) == (0, 1)
    assert store.position(0) == EventPosition(0, 1)
    assert len(store.read(current.card, current.order_key).entries) == 1


def test_gap_after_stored_position_raises_and_writes_nothing(make_store: StoreFactory) -> None:
    """Raise PositionGapError for a skipped offset and leave state and position unchanged."""
    store = make_store(0)
    store.apply(batch((0, make_tx(1, BASE_TIME - 60))))
    with pytest.raises(PositionGapError):
        store.apply(batch((2, make_tx(2, BASE_TIME - 30))))
    current = make_tx(9)
    assert len(store.read(current.card, current.order_key).entries) == 1
    assert store.position(0) == EventPosition(0, 0)


def test_gap_inside_batch_raises_and_writes_nothing(make_store: StoreFactory) -> None:
    """Raise PositionGapError when offsets inside one batch skip a value."""
    store = make_store(0)
    with pytest.raises(PositionGapError):
        store.apply(batch((0, make_tx(1, BASE_TIME - 60)), (2, make_tx(2, BASE_TIME - 30))))
    assert store.position(0) is None


def test_first_batch_may_start_at_any_offset(make_store: StoreFactory) -> None:
    """Accept a first batch that starts above offset 0."""
    store = make_store(0)
    store.apply(batch((41, make_tx(1)), (42, make_tx(2))))
    assert store.position(0) == EventPosition(0, 42)


def test_position_never_decreases(make_store: StoreFactory) -> None:
    """Keep the higher position after an older batch is applied again."""
    store = make_store(0)
    store.apply(batch((0, make_tx(1)), (1, make_tx(2)), (2, make_tx(3))))
    store.apply(batch((0, make_tx(1)), (1, make_tx(2))))
    assert store.position(0) == EventPosition(0, 2)


def test_positions_are_tracked_per_partition(make_store: StoreFactory) -> None:
    """Store an independent position for each partition."""
    store = make_store(0)
    store.apply(batch((5, make_tx(1)), partition=0))
    store.apply(batch((9, make_tx(2, card=OTHER_CARD)), partition=1))
    assert store.position(0) == EventPosition(0, 5)
    assert store.position(1) == EventPosition(1, 9)
    assert store.position(2) is None


def test_events_behind_their_card_are_counted(make_store: StoreFactory) -> None:
    """Count stored events earlier than their card's newest time; skip duplicates and others."""
    store = make_store(0)
    first = store.apply(batch((0, make_tx(1, BASE_TIME)), (1, make_tx(2, BASE_TIME - 60))))
    other_card = make_tx(3, BASE_TIME - 120, card=OTHER_CARD)
    second = store.apply(batch((2, make_tx(4, BASE_TIME - 30)), (3, other_card)))
    repeat = store.apply(batch((4, make_tx(2, BASE_TIME - 60))))
    assert (first.applied, first.behind) == (2, 1)
    assert (second.applied, second.behind) == (2, 1)
    assert (repeat.applied, repeat.behind) == (0, 0)


def test_late_event_beyond_lateness_is_pruned(make_store: StoreFactory) -> None:
    """Drop an entry older than the card's newest time minus window and lateness."""
    store = make_store(0)
    store.apply(batch((0, make_tx(1, BASE_TIME))))
    late = make_tx(2, BASE_TIME - WINDOW_SECONDS - 10)
    store.apply(batch((1, late)))
    probe = make_tx(3, late.time.epoch_s + 1)
    assert store.read(probe.card, probe.order_key).entries == ()


def test_late_event_within_lateness_is_kept(make_store: StoreFactory) -> None:
    """Keep an entry that is late by less than the allowed lateness."""
    store = make_store(60)
    store.apply(batch((0, make_tx(1, BASE_TIME))))
    late = make_tx(2, BASE_TIME - WINDOW_SECONDS - 10)
    store.apply(batch((1, late)))
    probe = make_tx(3, late.time.epoch_s + 1)
    assert [e.tx_id for e in store.read(probe.card, probe.order_key).entries] == [late.tx_id]


def test_merchant_with_separator_round_trips(make_store: StoreFactory) -> None:
    """Return a merchant name containing "|" unchanged."""
    store = make_store(0)
    tx = make_tx(1, BASE_TIME - 60, merchant="fraud_Smith | Sons")
    store.apply(batch((0, tx)))
    current = make_tx(9)
    (entry,) = store.read(current.card, current.order_key).entries
    assert entry.merchant == "fraud_Smith | Sons"
    assert entry.amount == tx.amount


def test_entries_older_than_window_are_not_read(make_store: StoreFactory) -> None:
    """Exclude entries more than WINDOW_SECONDS before the read bound."""
    store = make_store(0)
    store.apply(batch((0, make_tx(1, BASE_TIME - WINDOW_SECONDS - 1)), (1, make_tx(2))))
    probe = make_tx(3, BASE_TIME + 1)
    assert [e.tx_id for e in store.read(probe.card, probe.order_key).entries] == [make_tx(2).tx_id]
