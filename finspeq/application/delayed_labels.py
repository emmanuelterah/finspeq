"""Fraud labels that arrive late (chargebacks) and recall as it can be known on a given day."""

from __future__ import annotations

import random
from collections.abc import Sequence
from dataclasses import dataclass

from finspeq.application.errors import InvalidMessageError

SECONDS_PER_DAY = 86_400


@dataclass(frozen=True, slots=True)
class LabelDelay:
    """Days from a fraudulent transaction to its chargeback: triangular(min, mode, max), seeded."""

    min_days: float = 7.0
    mode_days: float = 21.0
    max_days: float = 60.0
    seed: int = 0

    def __post_init__(self) -> None:
        """Raise InvalidMessageError unless 0 <= min_days <= mode_days <= max_days."""
        if not 0 <= self.min_days <= self.mode_days <= self.max_days:
            raise InvalidMessageError(f"need 0 <= min <= mode <= max, got {self}")


@dataclass(frozen=True, slots=True)
class Observation:
    """One scored transaction: event time (epoch seconds), whether it was flagged, its label."""

    time: int
    flagged: bool
    is_fraud: bool


@dataclass(frozen=True, slots=True)
class RecallPoint:
    """Recall of the transactions in [window_start, as_of), as known on as_of and in hindsight.

    Attributes:
        as_of: The day the estimate is made, epoch seconds.
        window_start: Start of the measured window, epoch seconds.
        frauds: Fraudulent transactions in the window (known in hindsight).
        labelled: Those whose chargeback arrived before as_of.
        coverage: labelled / frauds.
        observed_recall: Flagged share of the labelled frauds; 0.0 when none are labelled.
        true_recall: Flagged share of all frauds in the window.
    """

    as_of: int
    window_start: int
    frauds: int
    labelled: int
    coverage: float
    observed_recall: float
    true_recall: float


def label_arrivals(observations: Sequence[Observation], delay: LabelDelay) -> list[int | None]:
    """Return when each fraud's label arrives (epoch seconds), or None for legitimate ones.

    One draw per fraudulent observation, in input order, from Random(delay.seed).
    """
    rng = random.Random(delay.seed)
    arrivals: list[int | None] = []
    for observation in observations:
        if not observation.is_fraud:
            arrivals.append(None)
            continue
        days = rng.triangular(delay.min_days, delay.max_days, delay.mode_days)
        arrivals.append(observation.time + round(days * SECONDS_PER_DAY))
    return arrivals


class RollingRecall:
    """Measures recall over a trailing window as it would be known on each as-of day."""

    def __init__(self, window_days: int) -> None:
        """Create the use case for a trailing window of window_days days."""
        self._window_s = window_days * SECONDS_PER_DAY

    def execute(
        self,
        observations: Sequence[Observation],
        arrivals: Sequence[int | None],
        as_of_times: Sequence[int],
    ) -> tuple[RecallPoint, ...]:
        """Return one RecallPoint per as-of time.

        Raises:
            InvalidMessageError: observations and arrivals differ in length.
        """
        if len(observations) != len(arrivals):
            raise InvalidMessageError("need one arrival per observation")
        frauds = [
            (obs.time, obs.flagged, arrival)
            for obs, arrival in zip(observations, arrivals, strict=True)
            if obs.is_fraud and arrival is not None
        ]
        points = []
        for as_of in as_of_times:
            start = as_of - self._window_s
            window = [
                (flagged, arrival) for time, flagged, arrival in frauds if start <= time < as_of
            ]
            known = [flagged for flagged, arrival in window if arrival < as_of]
            points.append(
                RecallPoint(
                    as_of=as_of,
                    window_start=start,
                    frauds=len(window),
                    labelled=len(known),
                    coverage=_share(len(known), len(window)),
                    observed_recall=_share(sum(known), len(known)),
                    true_recall=_share(sum(flagged for flagged, _ in window), len(window)),
                )
            )
        return tuple(points)


def _share(part: int, whole: int) -> float:
    """Return part / whole, or 0.0 when whole is 0."""
    return part / whole if whole else 0.0
