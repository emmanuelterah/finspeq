"""Tests for domain value objects and the Transaction entity."""

from __future__ import annotations

import math

import pytest

from finspeq.domain.errors import (
    InvalidCardIdError,
    InvalidCategoryError,
    InvalidEventTimeError,
    InvalidGeoPointError,
    InvalidMerchantError,
    InvalidMoneyError,
    InvalidTxIdError,
)
from finspeq.domain.model import (
    CardId,
    Category,
    EventTime,
    GeoPoint,
    Money,
    OrderKey,
    TxId,
)
from tests.factories import make_tx, tx_id


@pytest.mark.parametrize("value", ["4000000", "4000a00000000002", "4" * 20, "\u0664" * 16])
def test_invalid_card_id_is_rejected(value: str) -> None:
    """Reject ids that are too short, too long, contain letters, or use non-ASCII digits."""
    with pytest.raises(InvalidCardIdError):
        CardId(value)


def test_eleven_digit_card_id_is_accepted() -> None:
    """Accept an 11-digit card number, which occurs in the dataset."""
    assert CardId("60401057736").value == "60401057736"


def test_nineteen_digit_card_id_is_accepted() -> None:
    """Accept a 19-digit card number, which exceeds the signed 64-bit integer range."""
    assert CardId("9999999999999999999").value == "9999999999999999999"


@pytest.mark.parametrize("value", ["ABCDEF" + "0" * 26, "0" * 31, "g" * 32])
def test_invalid_tx_id_is_rejected(value: str) -> None:
    """Reject uppercase, short, and non-hex transaction ids."""
    with pytest.raises(InvalidTxIdError):
        TxId(value)


@pytest.mark.parametrize("cents", [0, -1])
def test_non_positive_money_is_rejected(cents: int) -> None:
    """Reject zero and negative amounts."""
    with pytest.raises(InvalidMoneyError):
        Money(cents)


def test_money_dollars_divides_cents_by_one_hundred() -> None:
    """Return 1001.4 dollars for 100140 cents."""
    assert Money(100_140).dollars == 1001.4


def test_negative_event_time_is_rejected() -> None:
    """Reject negative epoch seconds."""
    with pytest.raises(InvalidEventTimeError):
        EventTime(-1)


@pytest.mark.parametrize(("lat", "lon"), [(91.0, 0.0), (0.0, -181.0), (math.nan, 0.0)])
def test_out_of_range_geo_point_is_rejected(lat: float, lon: float) -> None:
    """Reject coordinates outside their range and NaN."""
    with pytest.raises(InvalidGeoPointError):
        GeoPoint(lat, lon)


def test_unknown_category_is_rejected() -> None:
    """Raise InvalidCategoryError for a name that is not a category value."""
    with pytest.raises(InvalidCategoryError):
        Category.parse("crypto")


def test_category_codes_follow_declaration_order() -> None:
    """Assign codes 0..13 in declaration order."""
    assert [c.code for c in Category] == list(range(14))
    assert Category.parse("entertainment").code == 0
    assert Category.parse("travel").code == 13


def test_order_key_breaks_time_ties_by_tx_id() -> None:
    """Order two keys with equal time by transaction id."""
    early = OrderKey(EventTime(10), tx_id(1))
    late = OrderKey(EventTime(10), tx_id(2))
    assert early < late
    assert OrderKey(EventTime(9), tx_id(99)) < early


def test_blank_merchant_is_rejected() -> None:
    """Reject a merchant name made of whitespace."""
    with pytest.raises(InvalidMerchantError):
        make_tx(1, merchant="   ")
