"""Tests for the pure-Python evaluation metrics."""

from __future__ import annotations

import math

import pytest

from finspeq.application.errors import EvaluationError
from finspeq.application.evaluation import confusion_at, evaluate

SCORES = [0.9, 0.8, 0.7, 0.6, 0.5]
LABELS = [True, False, True, False, False]


def test_average_precision_and_roc_auc() -> None:
    """Match hand-computed AP = (1/2)(1) + (1/2)(2/3) and ROC-AUC = 5/6."""
    result = evaluate(SCORES, LABELS, ())
    assert math.isclose(result.pr_auc, 0.5 * 1.0 + 0.5 * (2 / 3))
    assert math.isclose(result.roc_auc, 5 / 6)
    assert (result.rows, result.positives) == (5, 2)


def test_tied_scores_count_one_half() -> None:
    """Give AP 0.5 and ROC-AUC 0.5 when a positive and a negative share one score."""
    result = evaluate([0.5, 0.5], [True, False], ())
    assert math.isclose(result.pr_auc, 0.5)
    assert math.isclose(result.roc_auc, 0.5)


def test_operating_point_uses_lowest_threshold_within_budget() -> None:
    """Pick 0.9 for a zero FPR budget and 0.7 for a budget of 0.34 (one of three negatives)."""
    strict, loose = evaluate(SCORES, LABELS, (0.0, 0.34)).points
    assert strict.threshold == 0.9
    assert (strict.confusion.true_positives, strict.confusion.false_positives) == (1, 0)
    assert loose.threshold == 0.7
    assert (loose.confusion.true_positives, loose.confusion.false_positives) == (2, 1)
    assert math.isclose(loose.confusion.false_positive_rate, 1 / 3)


def test_operating_point_flags_nothing_when_top_score_is_negative() -> None:
    """Return a threshold above the top score when even it exceeds the budget."""
    (point,) = evaluate([0.9, 0.1], [False, True], (0.0,)).points
    assert point.threshold > 0.9
    assert point.confusion.true_positives == 0
    assert point.confusion.false_positives == 0


def test_confusion_at_threshold_is_inclusive() -> None:
    """Flag scores equal to the threshold."""
    confusion = confusion_at(SCORES, LABELS, 0.7)
    assert (confusion.true_positives, confusion.false_positives) == (2, 1)
    assert (confusion.true_negatives, confusion.false_negatives) == (2, 0)
    assert math.isclose(confusion.recall, 1.0)
    assert math.isclose(confusion.precision, 2 / 3)


@pytest.mark.parametrize(
    ("scores", "labels"),
    [([], []), ([0.1, 0.2], [True]), ([0.1, 0.2], [True, True]), ([0.1], [False])],
)
def test_invalid_inputs_are_rejected(scores: list[float], labels: list[bool]) -> None:
    """Reject empty input, unequal lengths, and single-class labels."""
    with pytest.raises(EvaluationError):
        evaluate(scores, labels, ())
