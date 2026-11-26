"""Tests for individual features and the FeatureSet."""

from __future__ import annotations

import dataclasses
import math

import pytest

from finspeq.domain.errors import (
    InvalidFeatureSetError,
    InvalidFeatureVectorError,
    InvalidWindowError,
)
from finspeq.domain.features import (
    FEATURE_SET_V1,
    FEATURE_SET_V2,
    FEATURE_SETS,
    AmountDollars,
    AmountToMaxRatio,
    AmountToMeanRatio,
    CategoryCode,
    DistinctMerchants,
    FeatureContext,
    FeatureSet,
    FeatureVector,
    HomeMerchantDistanceKm,
    HourOfDayUtc,
    SecondsSinceLast,
    SpendDollars,
    TxCount,
    duration_label,
)
from finspeq.domain.model import Category, GeoPoint
from finspeq.domain.window import WINDOW_SECONDS
from tests.factories import BASE_TIME, OTHER_CARD, make_tx, window_for

HISTORY = [
    make_tx(1, BASE_TIME - 20_000, cents=2_000, merchant="merchant_a"),
    make_tx(2, BASE_TIME - 1_800, cents=4_000, merchant="merchant_b"),
    make_tx(3, BASE_TIME - 600, cents=6_000, merchant="merchant_a"),
]
CURRENT = make_tx(10, cents=12_000)
WINDOW = window_for(CURRENT, HISTORY)
CONTEXT = FeatureContext(CURRENT, WINDOW)
EMPTY = FeatureContext(CURRENT, window_for(CURRENT, []))


def test_duration_label_uses_hours_when_whole() -> None:
    """Label 3600 as 1h, 86400 as 24h, and 90 as 90s."""
    assert [duration_label(s) for s in (3_600, 86_400, 90)] == ["1h", "24h", "90s"]


def test_amount_dollars() -> None:
    """Return the current amount in dollars."""
    assert AmountDollars().compute(CONTEXT) == 120.0


def test_tx_count_counts_entries_in_lookback() -> None:
    """Count two entries in the last hour and three in the last day."""
    assert TxCount(3_600).compute(CONTEXT) == 2.0
    assert TxCount(WINDOW_SECONDS).compute(CONTEXT) == 3.0


def test_spend_sums_amounts_in_lookback() -> None:
    """Sum 40 + 60 dollars in the last hour."""
    assert SpendDollars(3_600).compute(CONTEXT) == 100.0


def test_amount_to_mean_ratio() -> None:
    """Return 120 / mean(20, 40, 60) = 3.0, and 0.0 for an empty window."""
    feature = AmountToMeanRatio(WINDOW_SECONDS)
    assert feature.compute(CONTEXT) == 3.0
    assert feature.compute(EMPTY) == 0.0


def test_seconds_since_last() -> None:
    """Return 600 seconds, or WINDOW_SECONDS when the window is empty."""
    assert SecondsSinceLast().compute(CONTEXT) == 600.0
    assert SecondsSinceLast().compute(EMPTY) == float(WINDOW_SECONDS)


def test_distinct_merchants() -> None:
    """Count merchant_a and merchant_b once each."""
    assert DistinctMerchants(WINDOW_SECONDS).compute(CONTEXT) == 2.0


def test_hour_of_day_utc() -> None:
    """Return 15 for 1750000000 (2025-06-15 15:06:40 UTC)."""
    assert HourOfDayUtc().compute(CONTEXT) == 15.0


def test_category_code() -> None:
    """Return the grocery_pos code."""
    assert CategoryCode().compute(CONTEXT) == float(Category.GROCERY_POS.code)


def test_home_merchant_distance() -> None:
    """Return 0 for identical points and about 111.2 km for one degree of latitude."""
    assert HomeMerchantDistanceKm().compute(CONTEXT) == 0.0
    moved = dataclasses.replace(CURRENT, merchant_point=GeoPoint(46.0, -90.0))
    distance = HomeMerchantDistanceKm().compute(FeatureContext(moved, window_for(moved, [])))
    assert math.isclose(distance, 111.195, abs_tol=0.01)


def test_feature_set_v1_columns() -> None:
    """Keep the v1 column names and order fixed."""
    assert FEATURE_SET_V1.names == (
        "amount_dollars",
        "tx_count_1h",
        "tx_count_24h",
        "spend_1h",
        "spend_24h",
        "amount_to_mean_24h",
        "secs_since_last",
        "distinct_merchants_24h",
        "hour_of_day_utc",
        "category_code",
        "home_merchant_km",
    )


def test_feature_set_compute_returns_values_in_column_order() -> None:
    """Return one value per feature, tagged with the set's version."""
    vector = FEATURE_SET_V1.compute(CURRENT, WINDOW)
    assert vector.schema == "v1"
    assert vector.values[:3] == (120.0, 2.0, 3.0)
    assert len(vector.values) == len(FEATURE_SET_V1.names)


def test_feature_set_rejects_duplicate_names() -> None:
    """Reject two features with the same name."""
    with pytest.raises(InvalidFeatureSetError):
        FeatureSet(version="x", features=(TxCount(3_600), TxCount(3_600)))


def test_feature_set_rejects_empty_features() -> None:
    """Reject a feature set without features."""
    with pytest.raises(InvalidFeatureSetError):
        FeatureSet(version="x", features=())


def test_feature_set_rejects_window_of_other_transaction() -> None:
    """Reject a window whose upper bound is not the transaction's order key."""
    other = make_tx(11)
    with pytest.raises(InvalidWindowError):
        FEATURE_SET_V1.compute(other, WINDOW)


def test_feature_context_rejects_window_of_other_card() -> None:
    """Reject a window that belongs to a different card."""
    other = make_tx(10, card=OTHER_CARD)
    with pytest.raises(InvalidWindowError):
        FeatureContext(other, WINDOW)


def test_feature_vector_rejects_non_finite_values() -> None:
    """Reject NaN and infinity."""
    for bad in (math.nan, math.inf):
        with pytest.raises(InvalidFeatureVectorError):
            FeatureVector(schema="v1", values=(1.0, bad))


def test_amount_to_max_ratio() -> None:
    """Return 120 / max(20, 40, 60) = 2.0, and 0.0 for an empty window."""
    feature = AmountToMaxRatio(WINDOW_SECONDS)
    assert feature.name == "amount_to_max_24h"
    assert feature.compute(CONTEXT) == 2.0
    assert feature.compute(EMPTY) == 0.0


def test_feature_set_v2_drops_category_and_adds_amount_to_max() -> None:
    """Keep v2 free of category_code and register both versions."""
    assert "category_code" not in FEATURE_SET_V2.names
    assert "amount_to_max_24h" in FEATURE_SET_V2.names
    assert len(FEATURE_SET_V2.names) == len(FEATURE_SET_V1.names)
    assert FEATURE_SETS == {"v1": FEATURE_SET_V1, "v2": FEATURE_SET_V2}
