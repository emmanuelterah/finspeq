"""Tests for the model-development use cases with fake ports."""

from __future__ import annotations

import pytest

from finspeq.application.errors import EvaluationError
from finspeq.application.messages import (
    DatasetRef,
    ModelArtifact,
    ScoredSplit,
    Split,
    TrainParams,
)
from finspeq.application.modeling import (
    BenchmarkModel,
    DevelopModel,
    ThresholdPolicy,
    VerifyParity,
    select_thresholds,
)
from finspeq.domain.features import FeatureVector
from finspeq.domain.policy import Score, Thresholds

DATASET = DatasetRef(path="memory", rows=0, schema="v1")
VALID_SCORES = tuple(i / 1000 for i in range(1000))
VALID_LABELS = tuple(i >= 990 for i in range(1000))
TEST_SCORES = (0.995, 0.5, 0.993, 0.1)
TEST_LABELS = (True, False, True, False)
ARTIFACT = ModelArtifact("model.txt", "abc", "v1", ("f",), 10, 1.0, "")


class FakeScorer:
    """BatchScorer returning fixed scores per split."""

    def __init__(self, splits: dict[Split, ScoredSplit]) -> None:
        """Create a scorer that returns splits[split] for each request."""
        self._splits = splits

    def score(self, dataset: DatasetRef, split: Split) -> ScoredSplit:
        """Return the fixed ScoredSplit for split, checking the dataset reference."""
        assert dataset == DATASET
        return self._splits[split]


class FakeTrainer:
    """ModelTrainer returning ARTIFACT and recording its output argument."""

    def __init__(self) -> None:
        """Create a trainer with no recorded calls."""
        self.outputs: list[str] = []

    def train(self, dataset: DatasetRef, params: TrainParams, output: str) -> ModelArtifact:
        """Record output and return ARTIFACT."""
        assert dataset == DATASET
        assert params == TrainParams()
        self.outputs.append(output)
        return ARTIFACT


class SteppingClock:
    """Clock that advances by `step` seconds on every call."""

    def __init__(self, step: float) -> None:
        """Create a clock at time zero that advances by step per call."""
        self._step = step
        self._time = 0.0

    def now(self) -> float:
        """Return the current time, then advance it."""
        current = self._time
        self._time += self._step
        return current


class ConstantModel:
    """FraudModel that returns 0.5 and counts its calls."""

    version = "test"
    schema = "v1"

    def __init__(self) -> None:
        """Create a model with zero calls."""
        self.calls = 0

    def predict(self, vector: FeatureVector) -> Score:
        """Count the call and return probability 0.5."""
        assert vector.schema == self.schema
        self.calls += 1
        return Score(0.5)


def scored(split: Split, scores: tuple[float, ...], labels: tuple[bool, ...]) -> ScoredSplit:
    """Return a ScoredSplit for split."""
    return ScoredSplit(split=split, scores=scores, labels=labels)


def test_thresholds_follow_validation_fpr_budgets() -> None:
    """Choose REVIEW at <= 1% FPR and BLOCK at <= 0.1% FPR on validation scores."""
    selection = select_thresholds(
        scored(Split.VALID, VALID_SCORES, VALID_LABELS), ThresholdPolicy()
    )
    assert selection.review_point.confusion.false_positive_rate <= 0.01
    assert selection.block_point.confusion.false_positive_rate <= 0.001
    assert selection.thresholds.review < selection.thresholds.block


def test_thresholds_from_test_split_are_rejected() -> None:
    """Refuse to select thresholds on the TEST split."""
    with pytest.raises(EvaluationError):
        select_thresholds(scored(Split.TEST, VALID_SCORES, VALID_LABELS), ThresholdPolicy())


def test_develop_model_applies_validation_thresholds_to_test() -> None:
    """Train once, select thresholds on VALID, and report TEST counts at them."""
    trainer = FakeTrainer()
    scorer = FakeScorer(
        {
            Split.VALID: scored(Split.VALID, VALID_SCORES, VALID_LABELS),
            Split.TEST: scored(Split.TEST, TEST_SCORES, TEST_LABELS),
        }
    )

    def scorer_for(artifact: ModelArtifact) -> FakeScorer:
        """Return the fake scorer for the artifact the trainer produced."""
        assert artifact == ARTIFACT
        return scorer

    report = DevelopModel(trainer, scorer_for, ThresholdPolicy()).execute(
        DATASET, TrainParams(), "out"
    )
    assert trainer.outputs == ["out"]
    assert [e.split for e in report.evaluations] == [Split.VALID, Split.TEST]
    assert report.test_at_block.true_positives == 2
    assert report.test_at_block.false_positives == 0


def test_parity_reports_differences_and_decision_mismatches() -> None:
    """Count rows over tolerance and rows whose decision changes."""
    labels = (True, False, False)
    reference = FakeScorer({Split.TEST: scored(Split.TEST, (0.79, 0.2, 0.1), labels)})
    candidate = FakeScorer({Split.TEST: scored(Split.TEST, (0.81, 0.2, 0.1 + 1e-9), labels)})
    report = VerifyParity(reference, candidate).execute(
        DATASET, Split.TEST, Thresholds(review=0.5, block=0.8), tolerance=1e-6
    )
    assert report.rows == 3
    assert report.rows_over_tolerance == 1
    assert report.decision_mismatches == 1
    assert report.max_abs_diff == pytest.approx(0.02)


def test_parity_rejects_scorers_with_different_rows() -> None:
    """Raise EvaluationError when the scorers' labels differ."""
    reference = FakeScorer({Split.TEST: scored(Split.TEST, (0.1, 0.2), (True, False))})
    candidate = FakeScorer({Split.TEST: scored(Split.TEST, (0.1, 0.2), (False, True))})
    with pytest.raises(EvaluationError):
        VerifyParity(reference, candidate).execute(DATASET, Split.TEST, Thresholds(0.5, 0.8), 1e-6)


def test_benchmark_times_only_the_timed_calls() -> None:
    """Make warmup plus timed calls and report 1 ms per call from a 1 ms clock."""
    model = ConstantModel()
    vectors = [FeatureVector("v1", (1.0,)), FeatureVector("v1", (2.0,))]
    report = BenchmarkModel(model, SteppingClock(0.001)).execute(vectors, 5, 100)
    assert model.calls == 105
    assert report.calls == 100
    assert report.p50_ms == pytest.approx(1.0)
    assert report.p99_ms == pytest.approx(1.0)
