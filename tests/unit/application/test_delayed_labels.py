"""Tests for chargeback delays and rolling recall."""

from __future__ import annotations

import pytest

from finspeq.application.delayed_labels import (
    SECONDS_PER_DAY,
    LabelDelay,
    Observation,
    RollingRecall,
    label_arrivals,
)
from finspeq.application.errors import InvalidMessageError

DAY = SECONDS_PER_DAY


def test_arrivals_are_seeded_bounded_and_only_for_fraud() -> None:
    """Draw a delay in [min, max] days for each fraud, none for legitimate transactions."""
    observations = [Observation(n * DAY, flagged=False, is_fraud=n % 2 == 0) for n in range(200)]
    delay = LabelDelay(seed=4)
    arrivals = label_arrivals(observations, delay)
    assert arrivals == label_arrivals(observations, delay)
    for observation, arrival in zip(observations, arrivals, strict=True):
        if observation.is_fraud:
            assert arrival is not None
            assert 7 * DAY <= arrival - observation.time <= 60 * DAY
        else:
            assert arrival is None


def test_recall_counts_only_labels_that_have_arrived() -> None:
    """Estimate recall from arrived labels and report coverage and the hindsight recall."""
    observations = [
        Observation(1 * DAY, flagged=True, is_fraud=True),
        Observation(2 * DAY, flagged=False, is_fraud=True),
        Observation(3 * DAY, flagged=True, is_fraud=True),
        Observation(4 * DAY, flagged=True, is_fraud=False),
    ]
    arrivals: list[int | None] = [10 * DAY, 40 * DAY, 12 * DAY, None]
    early, late = RollingRecall(window_days=60).execute(
        observations, arrivals, [20 * DAY, 45 * DAY]
    )
    assert (early.frauds, early.labelled, early.observed_recall) == (3, 2, 1.0)
    assert early.true_recall == pytest.approx(2 / 3)
    assert early.coverage == pytest.approx(2 / 3)
    assert (late.frauds, late.labelled, late.coverage) == (3, 3, 1.0)
    assert late.observed_recall == late.true_recall == pytest.approx(2 / 3)


def test_invalid_delay_and_lengths_are_rejected() -> None:
    """Reject a mode outside [min, max] and mismatched inputs."""
    with pytest.raises(InvalidMessageError):
        LabelDelay(min_days=10, mode_days=5, max_days=60)
    with pytest.raises(InvalidMessageError):
        RollingRecall(30).execute([Observation(0, True, True)], [], [DAY])
