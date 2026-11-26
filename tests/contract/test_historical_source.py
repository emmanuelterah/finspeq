"""HistoricalSource contract, run against every adapter."""

from __future__ import annotations

import pytest

from finspeq.application.errors import InvalidSourceDataError
from finspeq.application.messages import LabeledTransaction, TimeRange
from finspeq.domain.model import EventTime
from tests.contract.conftest import SourceFactory
from tests.factories import BASE_TIME, OTHER_CARD, make_tx


def canonical_items() -> list[LabeledTransaction]:
    """Return six transactions in canonical order, including two in the same second."""
    return [
        LabeledTransaction(0, make_tx(1, BASE_TIME - 100), is_fraud=False),
        LabeledTransaction(1, make_tx(2, BASE_TIME, card=OTHER_CARD), is_fraud=True),
        LabeledTransaction(2, make_tx(3, BASE_TIME), is_fraud=False),
        LabeledTransaction(3, make_tx(4, BASE_TIME + 50, cents=100_140), is_fraud=False),
        LabeledTransaction(4, make_tx(5, BASE_TIME + 100), is_fraud=True),
        LabeledTransaction(5, make_tx(6, BASE_TIME + 200), is_fraud=False),
    ]


def test_yields_every_item_in_order(make_source: SourceFactory) -> None:
    """Yield all items unchanged and in canonical order when no range is given."""
    items = canonical_items()
    assert list(make_source(items).iter_labeled(None)) == items


def test_range_is_half_open(make_source: SourceFactory) -> None:
    """Include items at the range start and exclude items at the range end."""
    items = canonical_items()
    time_range = TimeRange(EventTime(BASE_TIME), EventTime(BASE_TIME + 100))
    selected = list(make_source(items).iter_labeled(time_range))
    assert [item.seq for item in selected] == [1, 2, 3]


def test_out_of_order_items_are_rejected(make_source: SourceFactory) -> None:
    """Raise InvalidSourceDataError when an item's order key decreases."""
    items = canonical_items()
    items[2], items[3] = items[3], items[2]
    with pytest.raises(InvalidSourceDataError):
        list(make_source(items).iter_labeled(None))
