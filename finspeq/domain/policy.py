"""Fraud score, decision thresholds, and the score-to-decision rule."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from finspeq.domain.errors import InvalidScoreError, InvalidThresholdsError


class Decision(Enum):
    """Outcome returned for one transaction."""

    APPROVE = "APPROVE"
    REVIEW = "REVIEW"
    BLOCK = "BLOCK"


@dataclass(frozen=True, slots=True)
class Score:
    """Model output: probability that a transaction is fraudulent."""

    fraud_probability: float

    def __post_init__(self) -> None:
        """Raise InvalidScoreError unless fraud_probability is in [0, 1]."""
        if not 0.0 <= self.fraud_probability <= 1.0:
            raise InvalidScoreError(f"probability must be in [0, 1], got {self.fraud_probability}")


@dataclass(frozen=True, slots=True)
class Thresholds:
    """Probability cut-offs: REVIEW at or above `review`, BLOCK at or above `block`."""

    review: float
    block: float

    def __post_init__(self) -> None:
        """Raise InvalidThresholdsError unless 0 <= review < block <= 1."""
        if not 0.0 <= self.review < self.block <= 1.0:
            raise InvalidThresholdsError(
                f"need 0 <= review < block <= 1, got review={self.review}, block={self.block}"
            )


@dataclass(frozen=True, slots=True)
class DecisionPolicy:
    """Maps a fraud score to a decision using fixed thresholds."""

    thresholds: Thresholds

    def decide(self, score: Score) -> Decision:
        """Return BLOCK at or above block, REVIEW at or above review, otherwise APPROVE."""
        if score.fraud_probability >= self.thresholds.block:
            return Decision.BLOCK
        if score.fraud_probability >= self.thresholds.review:
            return Decision.REVIEW
        return Decision.APPROVE
