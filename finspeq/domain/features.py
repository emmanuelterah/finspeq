"""Feature definitions and the versioned feature set used for training and scoring."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Protocol

from finspeq.domain.errors import (
    InvalidFeatureSetError,
    InvalidFeatureVectorError,
    InvalidWindowError,
)
from finspeq.domain.model import CENTS_PER_DOLLAR, Transaction
from finspeq.domain.window import WINDOW_SECONDS, CardWindow

SECONDS_PER_HOUR = 3_600
HOURS_PER_DAY = 24
EARTH_RADIUS_KM = 6_371.0088


@dataclass(frozen=True, slots=True)
class FeatureContext:
    """A transaction and its card's window before it: the input of every feature."""

    tx: Transaction
    window: CardWindow

    def __post_init__(self) -> None:
        """Raise InvalidWindowError unless window belongs to tx's card and ends at tx's key."""
        if self.window.card != self.tx.card or self.window.until != self.tx.order_key:
            raise InvalidWindowError("window must belong to tx's card and end at tx's order key")


class Feature(Protocol):
    """One named numeric input to the fraud model."""

    @property
    def name(self) -> str:
        """Return the feature's column name, unique within a feature set."""
        ...

    def compute(self, context: FeatureContext) -> float:
        """Return the feature value for context.tx given context.window."""
        ...


def duration_label(seconds: int) -> str:
    """Return "<n>h" when seconds is a whole number of hours, otherwise "<n>s"."""
    if seconds % SECONDS_PER_HOUR == 0:
        return f"{seconds // SECONDS_PER_HOUR}h"
    return f"{seconds}s"


@dataclass(frozen=True, slots=True)
class AmountDollars:
    """The transaction amount in dollars."""

    @property
    def name(self) -> str:
        """Return "amount_dollars"."""
        return "amount_dollars"

    def compute(self, context: FeatureContext) -> float:
        """Return the transaction amount in dollars."""
        return context.tx.amount.dollars


@dataclass(frozen=True, slots=True)
class TxCount:
    """Number of the card's transactions in the last `seconds` before tx."""

    seconds: int

    @property
    def name(self) -> str:
        """Return "tx_count_<duration>", for example "tx_count_1h"."""
        return f"tx_count_{duration_label(self.seconds)}"

    def compute(self, context: FeatureContext) -> float:
        """Return the number of window entries in the look-back period."""
        return float(len(context.window.recent(self.seconds)))


@dataclass(frozen=True, slots=True)
class SpendDollars:
    """Total amount of the card's transactions in the last `seconds` before tx, in dollars."""

    seconds: int

    @property
    def name(self) -> str:
        """Return "spend_<duration>", for example "spend_24h"."""
        return f"spend_{duration_label(self.seconds)}"

    def compute(self, context: FeatureContext) -> float:
        """Return the sum of entry amounts in the look-back period, in dollars."""
        entries = context.window.recent(self.seconds)
        total_cents = sum(entry.amount.cents for entry in entries)
        return total_cents / CENTS_PER_DOLLAR


@dataclass(frozen=True, slots=True)
class AmountToMeanRatio:
    """Ratio of tx's amount to the mean amount of the card's recent transactions."""

    seconds: int

    @property
    def name(self) -> str:
        """Return "amount_to_mean_<duration>", for example "amount_to_mean_24h"."""
        return f"amount_to_mean_{duration_label(self.seconds)}"

    def compute(self, context: FeatureContext) -> float:
        """Return amount * count / total of the look-back entries, or 0.0 when there are none."""
        entries = context.window.recent(self.seconds)
        if not entries:
            return 0.0
        total_cents = sum(entry.amount.cents for entry in entries)
        return (context.tx.amount.cents * len(entries)) / total_cents


@dataclass(frozen=True, slots=True)
class AmountToMaxRatio:
    """Ratio of tx's amount to the largest amount among the card's recent transactions."""

    seconds: int

    @property
    def name(self) -> str:
        """Return "amount_to_max_<duration>", for example "amount_to_max_24h"."""
        return f"amount_to_max_{duration_label(self.seconds)}"

    def compute(self, context: FeatureContext) -> float:
        """Return amount / largest look-back amount, or 0.0 when there are no entries."""
        entries = context.window.recent(self.seconds)
        if not entries:
            return 0.0
        largest = max(entry.amount.cents for entry in entries)
        return context.tx.amount.cents / largest


@dataclass(frozen=True, slots=True)
class SecondsSinceLast:
    """Seconds between the card's previous transaction in the window and tx."""

    @property
    def name(self) -> str:
        """Return "secs_since_last"."""
        return "secs_since_last"

    def compute(self, context: FeatureContext) -> float:
        """Return tx time minus the last entry's time, or WINDOW_SECONDS if the window is empty."""
        last = context.window.last()
        if last is None:
            return float(WINDOW_SECONDS)
        return float(context.tx.time.epoch_s - last.time.epoch_s)


@dataclass(frozen=True, slots=True)
class DistinctMerchants:
    """Number of distinct merchants among the card's transactions in the last `seconds`."""

    seconds: int

    @property
    def name(self) -> str:
        """Return "distinct_merchants_<duration>", for example "distinct_merchants_24h"."""
        return f"distinct_merchants_{duration_label(self.seconds)}"

    def compute(self, context: FeatureContext) -> float:
        """Return the count of distinct merchant names in the look-back period."""
        entries = context.window.recent(self.seconds)
        return float(len({entry.merchant for entry in entries}))


@dataclass(frozen=True, slots=True)
class HourOfDayUtc:
    """Hour of the day of tx in UTC."""

    @property
    def name(self) -> str:
        """Return "hour_of_day_utc"."""
        return "hour_of_day_utc"

    def compute(self, context: FeatureContext) -> float:
        """Return the UTC hour of the transaction's event time, from 0 to 23."""
        return float((context.tx.time.epoch_s % WINDOW_SECONDS) // SECONDS_PER_HOUR)


@dataclass(frozen=True, slots=True)
class CategoryCode:
    """Numeric code of tx's merchant category."""

    @property
    def name(self) -> str:
        """Return "category_code"."""
        return "category_code"

    def compute(self, context: FeatureContext) -> float:
        """Return the category code (its position in Category declaration order)."""
        return float(context.tx.category.code)


@dataclass(frozen=True, slots=True)
class HomeMerchantDistanceKm:
    """Great-circle distance between the cardholder's home and the merchant, in kilometres."""

    @property
    def name(self) -> str:
        """Return "home_merchant_km"."""
        return "home_merchant_km"

    def compute(self, context: FeatureContext) -> float:
        """Return the haversine distance between the home point and the merchant point."""
        tx = context.tx
        lat1 = math.radians(tx.home_point.lat)
        lat2 = math.radians(tx.merchant_point.lat)
        d_lat = lat2 - lat1
        d_lon = math.radians(tx.merchant_point.lon - tx.home_point.lon)
        h = math.sin(d_lat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(d_lon / 2) ** 2
        return 2 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(h)))


@dataclass(frozen=True, slots=True)
class FeatureVector:
    """Model input values in the column order of the feature set named by `schema`."""

    schema: str
    values: tuple[float, ...]

    def __post_init__(self) -> None:
        """Raise InvalidFeatureVectorError if any value is NaN or infinite."""
        for index, value in enumerate(self.values):
            if not math.isfinite(value):
                raise InvalidFeatureVectorError(f"value {index} is not finite: {value}")


@dataclass(frozen=True, slots=True)
class FeatureSet:
    """Ordered, versioned list of features that defines the model input columns."""

    version: str
    features: tuple[Feature, ...]

    def __post_init__(self) -> None:
        """Raise InvalidFeatureSetError if version is blank, features is empty, or names repeat."""
        if not self.version.strip():
            raise InvalidFeatureSetError("version must not be blank")
        if not self.features:
            raise InvalidFeatureSetError("feature set must contain at least one feature")
        names = self.names
        if len(set(names)) != len(names):
            raise InvalidFeatureSetError(f"duplicate feature names in {names}")

    @property
    def names(self) -> tuple[str, ...]:
        """Return the feature names in column order."""
        return tuple(feature.name for feature in self.features)

    def compute(self, tx: Transaction, window: CardWindow) -> FeatureVector:
        """Return every feature's value for tx, in column order.

        Raises:
            InvalidWindowError: window belongs to another card or does not end at tx.
        """
        context = FeatureContext(tx=tx, window=window)
        values = tuple(feature.compute(context) for feature in self.features)
        return FeatureVector(schema=self.version, values=values)


FEATURE_SET_V1 = FeatureSet(
    version="v1",
    features=(
        AmountDollars(),
        TxCount(SECONDS_PER_HOUR),
        TxCount(WINDOW_SECONDS),
        SpendDollars(SECONDS_PER_HOUR),
        SpendDollars(WINDOW_SECONDS),
        AmountToMeanRatio(WINDOW_SECONDS),
        SecondsSinceLast(),
        DistinctMerchants(WINDOW_SECONDS),
        HourOfDayUtc(),
        CategoryCode(),
        HomeMerchantDistanceKm(),
    ),
)

FEATURE_SET_V2 = FeatureSet(
    version="v2",
    features=(
        AmountDollars(),
        TxCount(SECONDS_PER_HOUR),
        TxCount(WINDOW_SECONDS),
        SpendDollars(SECONDS_PER_HOUR),
        SpendDollars(WINDOW_SECONDS),
        AmountToMeanRatio(WINDOW_SECONDS),
        AmountToMaxRatio(WINDOW_SECONDS),
        SecondsSinceLast(),
        DistinctMerchants(WINDOW_SECONDS),
        HourOfDayUtc(),
        HomeMerchantDistanceKm(),
    ),
)

FEATURE_SETS: dict[str, FeatureSet] = {
    feature_set.version: feature_set for feature_set in (FEATURE_SET_V1, FEATURE_SET_V2)
}
