"""Population stability index (PSI) per feature against a reference distribution."""

from __future__ import annotations

import math
from bisect import bisect_right
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from finspeq.application.errors import InvalidMessageError
from finspeq.application.messages import LedgerRow

DEFAULT_ALERT_PSI = 0.2
SHARE_FLOOR = 1e-4


@dataclass(frozen=True, slots=True)
class FeatureBins:
    """Bin edges of one feature and the reference share of each bin.

    A value v falls in bin bisect_right(edges, v): bin 0 is below the first edge and bin
    len(edges) is at or above the last. shares has len(edges) + 1 entries summing to 1.
    """

    name: str
    edges: tuple[float, ...]
    shares: tuple[float, ...]

    def __post_init__(self) -> None:
        """Raise InvalidMessageError unless edges increase and shares match them."""
        if any(b <= a for a, b in zip(self.edges, self.edges[1:], strict=False)):
            raise InvalidMessageError(f"{self.name}: edges must strictly increase")
        if len(self.shares) != len(self.edges) + 1:
            raise InvalidMessageError(f"{self.name}: need {len(self.edges) + 1} shares")


@dataclass(frozen=True, slots=True)
class DriftReference:
    """The reference (training) distribution of every feature, and its row count."""

    rows: int
    features: tuple[FeatureBins, ...]


@dataclass(frozen=True, slots=True)
class DriftReport:
    """PSI of each feature for one period, and the features above the alert level.

    Attributes:
        period: Label of the period, for example "2026-03" or "2026-03-14".
        rows: Rows measured in the period.
        psi: Feature name and PSI, in reference order.
        alerts: Names of features whose PSI is above the alert level.
    """

    period: str
    rows: int
    psi: tuple[tuple[str, float], ...]
    alerts: tuple[str, ...]


def bin_counts(values: Sequence[float], edges: Sequence[float]) -> list[int]:
    """Return how many values fall in each of the len(edges) + 1 bins."""
    counts = [0] * (len(edges) + 1)
    for value in values:
        counts[bisect_right(edges, value)] += 1
    return counts


def psi(expected: Sequence[float], counts: Sequence[int]) -> float:
    """Return sum((a - e) * ln(a / e)) over bins, shares floored at SHARE_FLOOR.

    Args:
        expected: Reference share of each bin.
        counts: Observed count of each bin.

    Raises:
        InvalidMessageError: The lengths differ or there are no observations.
    """
    total = sum(counts)
    if len(expected) != len(counts) or total == 0:
        raise InvalidMessageError("need equal bin counts and at least one observation")
    value = 0.0
    for share, count in zip(expected, counts, strict=True):
        e = max(share, SHARE_FLOOR)
        a = max(count / total, SHARE_FLOOR)
        value += (a - e) * math.log(a / e)
    return value


class MeasureDrift:
    """Compares a period's feature values with the reference and flags drifting features."""

    def __init__(self, reference: DriftReference, alert_psi: float = DEFAULT_ALERT_PSI) -> None:
        """Create the use case; a PSI above alert_psi raises an alert for that feature."""
        self._reference = reference
        self._alert_psi = alert_psi

    def from_counts(self, period: str, counts: Mapping[str, Sequence[int]]) -> DriftReport:
        """Return the drift report for per-feature bin counts already computed for period.

        Raises:
            InvalidMessageError: A feature's counts are missing or do not match its bins.
        """
        scores = []
        for feature in self._reference.features:
            if feature.name not in counts:
                raise InvalidMessageError(f"no counts for feature {feature.name}")
            scores.append((feature.name, psi(feature.shares, counts[feature.name])))
        rows = sum(counts[self._reference.features[0].name])
        alerts = tuple(name for name, value in scores if value > self._alert_psi)
        return DriftReport(period, rows, tuple(scores), alerts)

    def from_values(self, period: str, columns: Mapping[str, Sequence[float]]) -> DriftReport:
        """Return the drift report for raw per-feature values of period."""
        counts = {
            feature.name: bin_counts(columns[feature.name], feature.edges)
            for feature in self._reference.features
            if feature.name in columns
        }
        return self.from_counts(period, counts)


SECONDS_PER_DAY = 86_400


def daily_columns(
    rows: Iterable[LedgerRow], names: Sequence[str]
) -> dict[int, dict[str, list[float]]]:
    """Return, per UTC day number of each row's event time, the feature values by name.

    Raises:
        InvalidMessageError: A row's vector does not have one value per name.
    """
    days: dict[int, dict[str, list[float]]] = {}
    for row in rows:
        values = row.record.vector.values
        if len(values) != len(names):
            raise InvalidMessageError(f"row {row.tx_id.value} has {len(values)} values")
        columns = days.setdefault(row.time.epoch_s // SECONDS_PER_DAY, {n: [] for n in names})
        for name, value in zip(names, values, strict=True):
            columns[name].append(value)
    return days
