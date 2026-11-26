"""LightGBM implementations of ModelTrainer and BatchScorer."""

from __future__ import annotations

import hashlib
import math
import os
from pathlib import Path

import lightgbm as lgb
import numpy as np

from finspeq.adapters.outbound.training_data import feature_layout, load_split, manifest_sha256
from finspeq.application.messages import (
    DatasetRef,
    Imbalance,
    ModelArtifact,
    ScoredSplit,
    Split,
    TrainParams,
)
from finspeq.application.ports import BatchScorer, ModelTrainer

MODEL_FILE = "model.txt"
VERSION_HEX_CHARS = 12


class LightGBMTrainer(ModelTrainer):
    """Binary LightGBM classifier trained with deterministic settings."""

    def train(self, dataset: DatasetRef, params: TrainParams, output: str) -> ModelArtifact:
        """Fit on TRAIN with early stopping on VALID average precision; write output/model.txt.

        The model file holds the best iteration only. It is written to a temporary name and
        renamed into place.
        """
        schema, names = feature_layout(dataset)
        train_x, train_y = load_split(dataset, Split.TRAIN)
        valid_x, valid_y = load_split(dataset, Split.VALID)
        weight = positive_weight(params.imbalance, int(train_y.sum()), len(train_y))
        train_set = lgb.Dataset(train_x, label=train_y, feature_name=list(names))
        valid_set = lgb.Dataset(valid_x, label=valid_y, reference=train_set)
        booster = lgb.train(
            _library_params(params, weight),
            train_set,
            num_boost_round=params.max_rounds,
            valid_sets=[valid_set],
            valid_names=["valid"],
            callbacks=[lgb.early_stopping(params.early_stopping_rounds, verbose=False)],
        )
        text = booster.model_to_string(num_iteration=booster.best_iteration)
        path = Path(output) / MODEL_FILE
        _write_atomically(path, text)
        return ModelArtifact(
            path=path.as_posix(),
            version=hashlib.sha256(text.encode()).hexdigest()[:VERSION_HEX_CHARS],
            schema=schema,
            feature_names=names,
            best_iteration=int(booster.best_iteration),
            positive_weight=weight,
            dataset_manifest_sha256=manifest_sha256(dataset),
        )


class LightGBMScorer(BatchScorer):
    """Scores dataset splits with a saved LightGBM model."""

    def __init__(self, model_path: str, num_threads: int) -> None:
        """Load the model file at model_path; predict with num_threads threads."""
        self._booster = lgb.Booster(model_file=model_path)
        self._num_threads = num_threads

    def score(self, dataset: DatasetRef, split: Split) -> ScoredSplit:
        """Return the model's fraud probability and the label of every row of split."""
        matrix, labels = load_split(dataset, split)
        scores = np.asarray(self._booster.predict(matrix, num_threads=self._num_threads))
        return ScoredSplit(split, tuple(scores.tolist()), tuple(labels.tolist()))


def positive_weight(imbalance: Imbalance, positives: int, rows: int) -> float:
    """Return the weight of a fraud row: 1, sqrt(neg/pos), or neg/pos."""
    if imbalance is Imbalance.NONE or positives == 0:
        return 1.0
    ratio = (rows - positives) / positives
    return math.sqrt(ratio) if imbalance is Imbalance.SQRT else ratio


def _library_params(params: TrainParams, weight: float) -> dict[str, object]:
    """Return the LightGBM parameter dictionary for params and the positive weight."""
    return {
        "objective": "binary",
        "metric": "average_precision",
        "learning_rate": params.learning_rate,
        "num_leaves": params.num_leaves,
        "min_data_in_leaf": params.min_data_in_leaf,
        "feature_fraction": params.feature_fraction,
        "bagging_fraction": params.bagging_fraction,
        "bagging_freq": params.bagging_freq,
        "lambda_l2": params.lambda_l2,
        "scale_pos_weight": weight,
        "deterministic": True,
        "force_row_wise": True,
        "num_threads": params.num_threads,
        "seed": params.seed,
        "bagging_seed": params.seed,
        "feature_fraction_seed": params.seed,
        "data_random_seed": params.seed,
        "verbosity": -1,
    }


def _write_atomically(path: Path, text: str) -> None:
    """Write text with LF line endings to path through a temporary file in the same directory.

    LightGBM model files record tree sizes in bytes, so CRLF translation corrupts them.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(text, encoding="utf-8", newline="\n")
    os.replace(temporary, path)
