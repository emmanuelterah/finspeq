"""Value objects and the Transaction entity."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

from finspeq.domain.errors import (
    InvalidCardIdError,
    InvalidCategoryError,
    InvalidEventTimeError,
    InvalidGeoPointError,
    InvalidMerchantError,
    InvalidMoneyError,
    InvalidTxIdError,
)

CENTS_PER_DOLLAR = 100
MAX_ABS_LATITUDE = 90.0
MAX_ABS_LONGITUDE = 180.0

_CARD_ID_PATTERN = re.compile(r"[0-9]{8,19}")
_TX_ID_PATTERN = re.compile(r"[0-9a-f]{32}")


@dataclass(frozen=True, slots=True)
class CardId:
    """Primary account number of one payment card (ISO/IEC 7812: 8 to 19 digits)."""

    value: str

    def __post_init__(self) -> None:
        """Raise InvalidCardIdError unless value is 8 to 19 ASCII decimal digits."""
        if _CARD_ID_PATTERN.fullmatch(self.value) is None:
            raise InvalidCardIdError(f"card id must be 8-19 digits, got {self.value!r}")


@dataclass(frozen=True, slots=True, order=True)
class TxId:
    """Identifier of one transaction; ordered lexicographically."""

    value: str

    def __post_init__(self) -> None:
        """Raise InvalidTxIdError unless value is 32 lowercase hexadecimal characters."""
        if _TX_ID_PATTERN.fullmatch(self.value) is None:
            raise InvalidTxIdError(f"tx id must be 32 lowercase hex chars, got {self.value!r}")


@dataclass(frozen=True, slots=True)
class Money:
    """Positive amount of US currency stored as integer cents."""

    cents: int

    def __post_init__(self) -> None:
        """Raise InvalidMoneyError unless cents is greater than zero."""
        if self.cents <= 0:
            raise InvalidMoneyError(f"cents must be > 0, got {self.cents}")

    @property
    def dollars(self) -> float:
        """Return the amount in dollars as a float."""
        return self.cents / CENTS_PER_DOLLAR


@dataclass(frozen=True, slots=True, order=True)
class EventTime:
    """Time an event occurred, in whole seconds since the Unix epoch (UTC)."""

    epoch_s: int

    def __post_init__(self) -> None:
        """Raise InvalidEventTimeError if epoch_s is negative."""
        if self.epoch_s < 0:
            raise InvalidEventTimeError(f"epoch seconds must be >= 0, got {self.epoch_s}")


@dataclass(frozen=True, slots=True)
class GeoPoint:
    """Latitude and longitude in decimal degrees."""

    lat: float
    lon: float

    def __post_init__(self) -> None:
        """Raise InvalidGeoPointError unless lat is in [-90, 90] and lon is in [-180, 180]."""
        if not -MAX_ABS_LATITUDE <= self.lat <= MAX_ABS_LATITUDE:
            raise InvalidGeoPointError(f"latitude out of range: {self.lat}")
        if not -MAX_ABS_LONGITUDE <= self.lon <= MAX_ABS_LONGITUDE:
            raise InvalidGeoPointError(f"longitude out of range: {self.lon}")


class Category(Enum):
    """Merchant category of a transaction; declaration order defines the numeric code."""

    ENTERTAINMENT = "entertainment"
    FOOD_DINING = "food_dining"
    GAS_TRANSPORT = "gas_transport"
    GROCERY_NET = "grocery_net"
    GROCERY_POS = "grocery_pos"
    HEALTH_FITNESS = "health_fitness"
    HOME = "home"
    KIDS_PETS = "kids_pets"
    MISC_NET = "misc_net"
    MISC_POS = "misc_pos"
    PERSONAL_CARE = "personal_care"
    SHOPPING_NET = "shopping_net"
    SHOPPING_POS = "shopping_pos"
    TRAVEL = "travel"

    @classmethod
    def parse(cls, name: str) -> Category:
        """Return the category whose value equals name.

        Raises:
            InvalidCategoryError: name is not a known category value.
        """
        try:
            return cls(name)
        except ValueError as exc:
            raise InvalidCategoryError(f"unknown category: {name!r}") from exc

    @property
    def code(self) -> int:
        """Return the zero-based position of this category in declaration order."""
        return _CATEGORY_CODES[self]


_CATEGORY_CODES: dict[Category, int] = {member: index for index, member in enumerate(Category)}


@dataclass(frozen=True, slots=True, order=True)
class OrderKey:
    """Canonical position of an event: event time first, transaction id second."""

    time: EventTime
    tx_id: TxId


@dataclass(frozen=True, slots=True)
class Transaction:
    """One card payment submitted for authorization."""

    tx_id: TxId
    card: CardId
    time: EventTime
    amount: Money
    merchant: str
    category: Category
    merchant_point: GeoPoint
    home_point: GeoPoint

    def __post_init__(self) -> None:
        """Raise InvalidMerchantError if merchant is empty or only whitespace."""
        if not self.merchant.strip():
            raise InvalidMerchantError("merchant must not be blank")

    @property
    def order_key(self) -> OrderKey:
        """Return the transaction's canonical ordering key."""
        return OrderKey(time=self.time, tx_id=self.tx_id)
