"""Tests for WindowEntry and CardWindow."""

from __future__ import annotations

import pytest

from finspeq.domain.errors import InvalidWindowError
from finspeq.domain.window import WINDOW_SECONDS, CardWindow, WindowEntry
from tests.factories import BASE_TIME, make_tx, window_for


def test_unordered_entries_are_rejected() -> None:
    """Reject entries that are not strictly increasing by order key."""
    current = make_tx(9)
    first, second = make_tx(1, BASE_TIME - 10), make_tx(2, BASE_TIME - 20)
    entries = (WindowEntry.from_transaction(first), WindowEntry.from_transaction(second))
    with pytest.raises(InvalidWindowError):
        CardWindow(card=current.card, until=current.order_key, entries=entries)


def test_entry_at_until_is_rejected() -> None:
    """Reject an entry whose order key equals the window's upper bound."""
    current = make_tx(5)
    with pytest.raises(InvalidWindowError):
        window_for(current, [current])


def test_same_second_entry_with_smaller_tx_id_is_inside() -> None:
    """Accept an entry at the same second as until when its tx id sorts first."""
    current = make_tx(5)
    assert len(window_for(current, [make_tx(4)]).entries) == 1


def test_entry_older_than_window_is_rejected() -> None:
    """Reject an entry one second older than WINDOW_SECONDS before until."""
    current = make_tx(9)
    with pytest.raises(InvalidWindowError):
        window_for(current, [make_tx(1, BASE_TIME - WINDOW_SECONDS - 1)])


def test_entry_exactly_window_seconds_old_is_inside() -> None:
    """Accept an entry exactly WINDOW_SECONDS before until."""
    current = make_tx(9)
    assert len(window_for(current, [make_tx(1, BASE_TIME - WINDOW_SECONDS)]).entries) == 1


def test_recent_returns_entries_within_lookback() -> None:
    """Return only entries at most `seconds` before until."""
    current = make_tx(9)
    history = [
        make_tx(1, BASE_TIME - 7_200),
        make_tx(2, BASE_TIME - 3_600),
        make_tx(3, BASE_TIME - 60),
    ]
    recent = window_for(current, history).recent(3_600)
    assert [e.tx_id for e in recent] == [history[1].tx_id, history[2].tx_id]


def test_last_is_none_for_empty_window() -> None:
    """Return None from last() when the window has no entries."""
    assert window_for(make_tx(1), []).last() is None
