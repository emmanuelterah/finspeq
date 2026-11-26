"""Tests for Score, Thresholds, and DecisionPolicy."""

from __future__ import annotations

import math

import pytest

from finspeq.domain.errors import InvalidScoreError, InvalidThresholdsError
from finspeq.domain.policy import Decision, DecisionPolicy, Score, Thresholds

POLICY = DecisionPolicy(Thresholds(review=0.4, block=0.8))


@pytest.mark.parametrize("value", [-0.01, 1.01, math.nan])
def test_score_outside_unit_interval_is_rejected(value: float) -> None:
    """Reject probabilities below 0, above 1, and NaN."""
    with pytest.raises(InvalidScoreError):
        Score(value)


@pytest.mark.parametrize(("review", "block"), [(0.5, 0.5), (0.6, 0.5), (-0.1, 0.5), (0.5, 1.1)])
def test_invalid_thresholds_are_rejected(review: float, block: float) -> None:
    """Reject thresholds that do not satisfy 0 <= review < block <= 1."""
    with pytest.raises(InvalidThresholdsError):
        Thresholds(review=review, block=block)


@pytest.mark.parametrize(
    ("probability", "expected"),
    [
        (0.0, Decision.APPROVE),
        (0.399, Decision.APPROVE),
        (0.4, Decision.REVIEW),
        (0.799, Decision.REVIEW),
        (0.8, Decision.BLOCK),
        (1.0, Decision.BLOCK),
    ],
)
def test_decide_uses_inclusive_lower_bounds(probability: float, expected: Decision) -> None:
    """Return REVIEW from review inclusive and BLOCK from block inclusive."""
    assert POLICY.decide(Score(probability)) is expected
