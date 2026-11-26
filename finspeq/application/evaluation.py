"""Ranking and operating-point metrics for fraud scores; pure Python, no third-party code."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

from finspeq.application.errors import EvaluationError


@dataclass(frozen=True, slots=True)
class Confusion:
    """Counts of a decision rule `score >= threshold` against the labels."""

    true_positives: int
    false_positives: int
    true_negatives: int
    false_negatives: int

    @property
    def recall(self) -> float:
        """Return TP / (TP + FN), or 0.0 when there are no positives."""
        positives = self.true_positives + self.false_negatives
        return self.true_positives / positives if positives else 0.0

    @property
    def precision(self) -> float:
        """Return TP / (TP + FP), or 1.0 when nothing is flagged."""
        flagged = self.true_positives + self.false_positives
        return self.true_positives / flagged if flagged else 1.0

    @property
    def false_positive_rate(self) -> float:
        """Return FP / (FP + TN), or 0.0 when there are no negatives."""
        negatives = self.false_positives + self.true_negatives
        return self.false_positives / negatives if negatives else 0.0


@dataclass(frozen=True, slots=True)
class OperatingPoint:
    """The lowest threshold whose false-positive rate is at most max_fpr, and its counts."""

    max_fpr: float
    threshold: float
    confusion: Confusion


@dataclass(frozen=True, slots=True)
class Evaluation:
    """Ranking quality and operating points of one set of scores."""

    rows: int
    positives: int
    pr_auc: float
    roc_auc: float
    points: tuple[OperatingPoint, ...]


@dataclass(frozen=True, slots=True)
class _Step:
    """Cumulative counts after flagging every row with score >= threshold."""

    threshold: float
    true_positives: int
    false_positives: int


def evaluate(
    scores: Sequence[float], labels: Sequence[bool], max_fprs: Sequence[float]
) -> Evaluation:
    """Return PR-AUC (average precision), ROC-AUC, and one operating point per max_fpr.

    Average precision is the sum over distinct thresholds of (recall increase) x precision.
    ROC-AUC is the trapezoidal area under the ROC curve; tied scores count one half.

    Raises:
        EvaluationError: Inputs are empty, differ in length, or contain one class only.
    """
    steps = _steps(scores, labels)
    positives = steps[-1].true_positives
    negatives = steps[-1].false_positives
    return Evaluation(
        rows=len(scores),
        positives=positives,
        pr_auc=_average_precision(steps, positives),
        roc_auc=_roc_auc(steps, positives, negatives),
        points=tuple(_operating_point(steps, positives, negatives, m) for m in max_fprs),
    )


def confusion_at(scores: Sequence[float], labels: Sequence[bool], threshold: float) -> Confusion:
    """Return the confusion counts of flagging every row with score >= threshold."""
    tp = fp = tn = fn = 0
    for score, label in zip(scores, labels, strict=True):
        flagged = score >= threshold
        tp += flagged and label
        fp += flagged and not label
        tn += not flagged and not label
        fn += not flagged and label
    return Confusion(tp, fp, tn, fn)


def _steps(scores: Sequence[float], labels: Sequence[bool]) -> list[_Step]:
    """Return cumulative counts at each distinct score, from the highest score down.

    Raises:
        EvaluationError: Inputs are empty, differ in length, or contain one class only.
    """
    if len(scores) != len(labels) or not scores:
        raise EvaluationError(f"need equal non-empty inputs, got {len(scores)}/{len(labels)}")
    order = sorted(range(len(scores)), key=scores.__getitem__, reverse=True)
    steps: list[_Step] = []
    tp = fp = 0
    for position, index in enumerate(order):
        tp += labels[index]
        fp += not labels[index]
        last = position == len(order) - 1
        if last or scores[order[position + 1]] != scores[index]:
            steps.append(_Step(scores[index], tp, fp))
    if tp == 0 or fp == 0:
        raise EvaluationError("labels must contain both classes")
    return steps


def _average_precision(steps: Sequence[_Step], positives: int) -> float:
    """Return the sum of (recall increase) x precision over the steps."""
    total = 0.0
    previous_tp = 0
    for step in steps:
        flagged = step.true_positives + step.false_positives
        total += (step.true_positives - previous_tp) / positives * step.true_positives / flagged
        previous_tp = step.true_positives
    return total


def _roc_auc(steps: Sequence[_Step], positives: int, negatives: int) -> float:
    """Return the trapezoidal area under the ROC curve through the steps."""
    area = 0.0
    previous_tpr = previous_fpr = 0.0
    for step in steps:
        tpr = step.true_positives / positives
        fpr = step.false_positives / negatives
        area += (fpr - previous_fpr) * (tpr + previous_tpr) / 2
        previous_tpr, previous_fpr = tpr, fpr
    return area


def _operating_point(
    steps: Sequence[_Step], positives: int, negatives: int, max_fpr: float
) -> OperatingPoint:
    """Return the lowest step threshold with FPR <= max_fpr.

    If even the highest score exceeds max_fpr, the threshold is just above the highest score
    and nothing is flagged.
    """
    threshold = math.nextafter(steps[0].threshold, math.inf)
    tp = fp = 0
    for step in steps:
        if step.false_positives / negatives > max_fpr:
            break
        threshold, tp, fp = step.threshold, step.true_positives, step.false_positives
    confusion = Confusion(tp, fp, negatives - fp, positives - tp)
    return OperatingPoint(max_fpr=max_fpr, threshold=threshold, confusion=confusion)
