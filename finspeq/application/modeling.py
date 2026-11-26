"""Use cases for model development: evaluate, train, select thresholds, verify, benchmark."""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from finspeq.application.errors import EvaluationError
from finspeq.application.evaluation import (
    Confusion,
    Evaluation,
    OperatingPoint,
    confusion_at,
    evaluate,
)
from finspeq.application.messages import DatasetRef, ModelArtifact, ScoredSplit, Split, TrainParams
from finspeq.application.ports import BatchScorer, Clock, FraudModel, ModelTrainer
from finspeq.domain.features import FeatureVector
from finspeq.domain.policy import Decision, DecisionPolicy, Score, Thresholds

DEFAULT_MAX_FPRS = (0.001, 0.005, 0.01)
MILLISECONDS_PER_SECOND = 1_000.0


@dataclass(frozen=True, slots=True)
class SplitEvaluation:
    """Evaluation of one split."""

    split: Split
    evaluation: Evaluation


@dataclass(frozen=True, slots=True)
class ThresholdPolicy:
    """False-positive budgets that define the REVIEW and BLOCK thresholds."""

    review_max_fpr: float = 0.01
    block_max_fpr: float = 0.001


@dataclass(frozen=True, slots=True)
class ThresholdSelection:
    """Thresholds chosen on validation data and the operating points that produced them."""

    thresholds: Thresholds
    review_point: OperatingPoint
    block_point: OperatingPoint


@dataclass(frozen=True, slots=True)
class DevelopmentReport:
    """Trained model, its validation and test evaluations, and test results at thresholds."""

    artifact: ModelArtifact
    evaluations: tuple[SplitEvaluation, ...]
    selection: ThresholdSelection
    test_at_review: Confusion
    test_at_block: Confusion


@dataclass(frozen=True, slots=True)
class ParityReport:
    """Agreement between a reference scorer and a candidate scorer on one split."""

    split: Split
    rows: int
    max_abs_diff: float
    mean_abs_diff: float
    rows_over_tolerance: int
    decision_mismatches: int


@dataclass(frozen=True, slots=True)
class LatencyReport:
    """Single-call latency percentiles in milliseconds."""

    calls: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


def select_thresholds(validation: ScoredSplit, policy: ThresholdPolicy) -> ThresholdSelection:
    """Return REVIEW and BLOCK thresholds at the policy's false-positive budgets on validation.

    Raises:
        EvaluationError: validation has one class only, or is not the VALID split.
        InvalidThresholdsError: The two budgets produce thresholds that are not ordered.
    """
    if validation.split is not Split.VALID:
        raise EvaluationError(f"thresholds must come from VALID, got {validation.split.value}")
    budgets = (policy.review_max_fpr, policy.block_max_fpr)
    review, block = evaluate(validation.scores, validation.labels, budgets).points
    thresholds = Thresholds(review=review.threshold, block=block.threshold)
    return ThresholdSelection(thresholds=thresholds, review_point=review, block_point=block)


class EvaluateScorer:
    """Evaluates a batch scorer on dataset splits."""

    def __init__(self, scorer: BatchScorer, max_fprs: Sequence[float] = DEFAULT_MAX_FPRS) -> None:
        """Create the use case for scorer, reporting an operating point per max_fprs value."""
        self._scorer = scorer
        self._max_fprs = tuple(max_fprs)

    def execute(self, dataset: DatasetRef, splits: Sequence[Split]) -> tuple[SplitEvaluation, ...]:
        """Score and evaluate each split of dataset, in the given order."""
        results = []
        for split in splits:
            scored = self._scorer.score(dataset, split)
            evaluation = evaluate(scored.scores, scored.labels, self._max_fprs)
            results.append(SplitEvaluation(split=split, evaluation=evaluation))
        return tuple(results)


class DevelopModel:
    """Trains a model, evaluates it, and selects thresholds on validation data only."""

    def __init__(
        self,
        trainer: ModelTrainer,
        scorer_for: Callable[[ModelArtifact], BatchScorer],
        policy: ThresholdPolicy,
    ) -> None:
        """Create the use case.

        Args:
            trainer: Fits the model.
            scorer_for: Returns a batch scorer for a trained artifact.
            policy: False-positive budgets for REVIEW and BLOCK.
        """
        self._trainer = trainer
        self._scorer_for = scorer_for
        self._policy = policy

    def execute(self, dataset: DatasetRef, params: TrainParams, output: str) -> DevelopmentReport:
        """Train, score VALID and TEST, select thresholds on VALID, and apply them to TEST."""
        artifact = self._trainer.train(dataset, params, output)
        scorer = self._scorer_for(artifact)
        validation = scorer.score(dataset, Split.VALID)
        test = scorer.score(dataset, Split.TEST)
        selection = select_thresholds(validation, self._policy)
        evaluations = tuple(
            SplitEvaluation(s.split, evaluate(s.scores, s.labels, DEFAULT_MAX_FPRS))
            for s in (validation, test)
        )
        return DevelopmentReport(
            artifact=artifact,
            evaluations=evaluations,
            selection=selection,
            test_at_review=confusion_at(test.scores, test.labels, selection.thresholds.review),
            test_at_block=confusion_at(test.scores, test.labels, selection.thresholds.block),
        )


class VerifyParity:
    """Compares a candidate scorer with a reference scorer row by row."""

    def __init__(self, reference: BatchScorer, candidate: BatchScorer) -> None:
        """Create the use case for the two scorers."""
        self._reference = reference
        self._candidate = candidate

    def execute(
        self, dataset: DatasetRef, split: Split, thresholds: Thresholds, tolerance: float
    ) -> ParityReport:
        """Return score differences and decision mismatches between the scorers on split.

        Raises:
            EvaluationError: The scorers return different labels or row counts.
        """
        reference = self._reference.score(dataset, split)
        candidate = self._candidate.score(dataset, split)
        if reference.labels != candidate.labels:
            raise EvaluationError("scorers disagree on the rows of the split")
        policy = DecisionPolicy(thresholds)
        diffs = [abs(a - b) for a, b in zip(reference.scores, candidate.scores, strict=True)]
        mismatches = sum(
            _decide(policy, a) is not _decide(policy, b)
            for a, b in zip(reference.scores, candidate.scores, strict=True)
        )
        return ParityReport(
            split=split,
            rows=len(diffs),
            max_abs_diff=max(diffs, default=0.0),
            mean_abs_diff=math.fsum(diffs) / len(diffs) if diffs else 0.0,
            rows_over_tolerance=sum(d > tolerance for d in diffs),
            decision_mismatches=mismatches,
        )


class BenchmarkModel:
    """Measures single-vector prediction latency of a fraud model."""

    def __init__(self, model: FraudModel, clock: Clock) -> None:
        """Create the use case for model, timed with clock."""
        self._model = model
        self._clock = clock

    def execute(
        self, vectors: Sequence[FeatureVector], warmup_calls: int, calls: int
    ) -> LatencyReport:
        """Call predict warmup_calls times untimed, then calls times timed, cycling vectors.

        Raises:
            EvaluationError: vectors is empty or calls is not positive.
        """
        if not vectors or calls <= 0:
            raise EvaluationError("need at least one vector and one timed call")
        for index in range(warmup_calls):
            self._model.predict(vectors[index % len(vectors)])
        durations = []
        for index in range(calls):
            vector = vectors[index % len(vectors)]
            start = self._clock.now()
            self._model.predict(vector)
            durations.append((self._clock.now() - start) * MILLISECONDS_PER_SECOND)
        durations.sort()
        return LatencyReport(
            calls=calls,
            p50_ms=_percentile(durations, 0.50),
            p95_ms=_percentile(durations, 0.95),
            p99_ms=_percentile(durations, 0.99),
            max_ms=durations[-1],
        )


def _decide(policy: DecisionPolicy, probability: float) -> Decision:
    """Return the policy decision for a probability clamped into [0, 1]."""
    return policy.decide(Score(min(1.0, max(0.0, probability))))


def _percentile(sorted_values: Sequence[float], fraction: float) -> float:
    """Return the nearest-rank percentile of ascending sorted_values."""
    rank = max(1, math.ceil(fraction * len(sorted_values)))
    return sorted_values[rank - 1]
