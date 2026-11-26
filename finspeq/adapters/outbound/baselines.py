"""Baseline batch scorers: a fixed business rule and logistic regression."""

from __future__ import annotations

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import StandardScaler

from finspeq.adapters.outbound.training_data import feature_layout, load_split
from finspeq.application.messages import DatasetRef, ScoredSplit, Split
from finspeq.application.ports import BatchScorer

NIGHT_HOURS_UTC = frozenset({19, 20, 21, 22, 23, 0})
NIGHT_OFFSET = 1_000_000.0
LOGISTIC_MAX_ITER = 1_000


class RuleBaselineScorer(BatchScorer):
    """Ranks night-time (19:00-00:59 UTC) transactions first, then by amount."""

    def score(self, dataset: DatasetRef, split: Split) -> ScoredSplit:
        """Return amount_dollars plus NIGHT_OFFSET for night hours, per row of split."""
        _, names = feature_layout(dataset)
        matrix, labels = load_split(dataset, split)
        amount = matrix[:, names.index("amount_dollars")].astype(np.float64)
        hour = matrix[:, names.index("hour_of_day_utc")].astype(np.int64)
        night = np.isin(hour, list(NIGHT_HOURS_UTC))
        scores = amount + NIGHT_OFFSET * night
        return ScoredSplit(split, tuple(scores.tolist()), tuple(labels.tolist()))


class LogisticBaselineScorer(BatchScorer):
    """Standardized logistic regression fitted on every `stride`-th TRAIN row."""

    def __init__(self, stride: int) -> None:
        """Create an unfitted scorer that fits on TRAIN rows 0, stride, 2*stride, ..."""
        self._stride = stride
        self._fitted: dict[str, Pipeline] = {}

    def score(self, dataset: DatasetRef, split: Split) -> ScoredSplit:
        """Fit on dataset's TRAIN rows on first use, then return probabilities for split."""
        pipeline = self._fitted.get(dataset.path)
        if pipeline is None:
            train_x, train_y = load_split(dataset, Split.TRAIN)
            pipeline = make_pipeline(
                StandardScaler(), LogisticRegression(max_iter=LOGISTIC_MAX_ITER)
            )
            pipeline.fit(train_x[:: self._stride], train_y[:: self._stride])
            self._fitted[dataset.path] = pipeline
        matrix, labels = load_split(dataset, split)
        scores = pipeline.predict_proba(matrix)[:, 1]
        return ScoredSplit(split, tuple(scores.tolist()), tuple(labels.tolist()))
