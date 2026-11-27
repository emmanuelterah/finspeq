"""Tests for PSI and the drift report."""

from __future__ import annotations

import math

import pytest

from finspeq.application.drift import (
    DriftReference,
    FeatureBins,
    MeasureDrift,
    bin_counts,
    daily_columns,
    psi,
)
from finspeq.application.errors import InvalidMessageError
from finspeq.application.messages import EventPosition, LedgerRow
from finspeq.domain.features import FeatureVector
from finspeq.domain.model import EventTime
from finspeq.domain.policy import Decision, Score
from finspeq.domain.scored import DecisionRecord
from tests.factories import tx_id

DAY = 86_400
POS = EventPosition(0, 0)


def record(values: tuple[float, ...]) -> DecisionRecord:
    """Return an APPROVE record with the given feature values."""
    return DecisionRecord(FeatureVector("v2", values), Score(0.1), Decision.APPROVE, "m")


REFERENCE = DriftReference(
    rows=100,
    features=(
        FeatureBins("amount", (10.0, 20.0), (0.25, 0.5, 0.25)),
        FeatureBins("count", (1.0,), (0.5, 0.5)),
    ),
)


def test_bins_put_edge_values_in_the_upper_bin() -> None:
    """Count a value equal to an edge in the bin above it."""
    assert bin_counts([5.0, 10.0, 15.0, 20.0, 25.0], (10.0, 20.0)) == [1, 2, 2]


def test_psi_is_zero_for_the_same_distribution_and_grows_with_shift() -> None:
    """Return 0 for identical shares and the textbook value for a shifted one."""
    assert psi((0.25, 0.5, 0.25), (25, 50, 25)) == 0.0
    shifted = psi((0.5, 0.5), (90, 10))
    expected = (0.9 - 0.5) * math.log(0.9 / 0.5) + (0.1 - 0.5) * math.log(0.1 / 0.5)
    assert shifted == pytest.approx(expected)


def test_empty_bins_are_floored_instead_of_infinite() -> None:
    """Treat an empty bin as a tiny share so PSI stays finite."""
    assert math.isfinite(psi((0.5, 0.5), (10, 0)))


def test_report_flags_features_above_the_alert_level() -> None:
    """Flag the feature whose distribution moved and leave the stable one."""
    columns = {"amount": [5.0, 15.0, 15.0, 25.0], "count": [0.0] * 9 + [2.0]}
    report = MeasureDrift(REFERENCE).from_values("2026-03", columns)
    assert report.rows == 4
    assert dict(report.psi)["amount"] == 0.0
    assert report.alerts == ("count",)


def test_invalid_bins_and_missing_counts_are_rejected() -> None:
    """Reject decreasing edges, mismatched shares, and a missing feature."""
    with pytest.raises(InvalidMessageError):
        FeatureBins("x", (2.0, 1.0), (0.3, 0.3, 0.4))
    with pytest.raises(InvalidMessageError):
        FeatureBins("x", (1.0,), (1.0,))
    with pytest.raises(InvalidMessageError):
        MeasureDrift(REFERENCE).from_counts("p", {"amount": [1, 1, 1]})


def test_ledger_rows_are_grouped_by_utc_day() -> None:
    """Split recorded vectors into per-day feature columns."""
    rows = [
        LedgerRow(tx_id(1), "4000000000000002", EventTime(DAY + 5), 100, record((1.0, 2.0)), POS),
        LedgerRow(tx_id(2), "4000000000000002", EventTime(DAY + 9), 100, record((3.0, 4.0)), POS),
        LedgerRow(tx_id(3), "4000000000000002", EventTime(2 * DAY), 100, record((5.0, 6.0)), POS),
    ]
    days = daily_columns(rows, ["a", "b"])
    assert days == {1: {"a": [1.0, 3.0], "b": [2.0, 4.0]}, 2: {"a": [5.0], "b": [6.0]}}
    with pytest.raises(InvalidMessageError):
        daily_columns(rows, ["a"])
