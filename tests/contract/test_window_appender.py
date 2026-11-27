"""WindowAppender contract (read-your-writes), run against every window store adapter."""

from __future__ import annotations

from finspeq.application.messages import EventPosition
from finspeq.domain.window import WINDOW_SECONDS
from tests.contract.conftest import StoreFactory
from tests.factories import BASE_TIME, batch, make_tx


def test_appended_event_is_read_and_later_apply_skips_it(make_store: StoreFactory) -> None:
    """Read an appended entry at once; the worker's later apply stores nothing new."""
    store = make_store(0)
    tx = make_tx(1, BASE_TIME - 60)
    assert store.append(tx) is True
    assert store.position(0) is None
    probe = make_tx(9)
    assert [e.tx_id for e in store.read(probe.card, probe.order_key).entries] == [tx.tx_id]
    result = store.apply(batch((0, tx), (1, make_tx(2, BASE_TIME - 30))))
    assert (result.applied, result.skipped) == (1, 1)
    assert store.position(0) == EventPosition(0, 1)
    assert len(store.read(probe.card, probe.order_key).entries) == 2


def test_repeated_append_stores_once(make_store: StoreFactory) -> None:
    """Return False for a transaction id the card already holds."""
    store = make_store(0)
    tx = make_tx(1, BASE_TIME - 60)
    store.apply(batch((0, tx)))
    assert store.append(tx) is False
    probe = make_tx(9)
    assert len(store.read(probe.card, probe.order_key).entries) == 1


def test_append_prunes_like_apply(make_store: StoreFactory) -> None:
    """Drop the card's entries older than its newest time minus window and lateness."""
    store = make_store(0)
    old = make_tx(1, BASE_TIME - WINDOW_SECONDS - 120)
    store.apply(batch((0, old)))
    store.append(make_tx(2, BASE_TIME))
    probe = make_tx(3, old.time.epoch_s + 1)
    assert store.read(probe.card, probe.order_key).entries == ()


def test_appending_before_applying_gives_the_same_windows(make_store: StoreFactory) -> None:
    """End in the same state whether or not every other event was appended first."""
    plain, appended = make_store(0), make_store(0)
    txs = [make_tx(n + 1, BASE_TIME + 600 * n, cents=100 + n) for n in range(10)]
    for offset, tx in enumerate(txs):
        if offset % 2 == 0:
            appended.append(tx)
        plain.apply(batch((offset, tx)))
        appended.apply(batch((offset, tx)))
    probe = make_tx(99, BASE_TIME + 600 * 10)
    assert plain.read(probe.card, probe.order_key) == appended.read(probe.card, probe.order_key)
    assert plain.position(0) == appended.position(0)
