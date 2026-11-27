"""Result of scoring one transaction."""

from __future__ import annotations

from dataclasses import dataclass

from finspeq.domain.features import FeatureVector
from finspeq.domain.model import Transaction
from finspeq.domain.policy import Decision, Score


@dataclass(frozen=True, slots=True)
class ShadowDecision:
    """What a second model, run alongside the live one, would have decided."""

    score: Score
    decision: Decision
    model_version: str


@dataclass(frozen=True, slots=True)
class DecisionRecord:
    """Model input, model output, and decision produced for one transaction.

    shadow holds a second model's verdict on the same vector; it never changes `decision`.
    """

    vector: FeatureVector
    score: Score
    decision: Decision
    model_version: str
    shadow: ShadowDecision | None = None


@dataclass(frozen=True, slots=True)
class ScoredTransaction:
    """A transaction together with the decision record produced for it."""

    transaction: Transaction
    record: DecisionRecord
