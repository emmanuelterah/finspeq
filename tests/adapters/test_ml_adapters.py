"""LightGBM, ONNX, and baseline adapters on a small synthetic dataset."""

from __future__ import annotations

import random
from dataclasses import replace
from pathlib import Path

import pytest

pytest.importorskip("lightgbm")
pytest.importorskip("onnxruntime")
pytest.importorskip("onnxmltools")
pytest.importorskip("sklearn")

from finspeq.adapters.outbound.baselines import LogisticBaselineScorer, RuleBaselineScorer
from finspeq.adapters.outbound.dataset_manifest import dataset_ref, write_manifest
from finspeq.adapters.outbound.lightgbm_model import LightGBMScorer, LightGBMTrainer
from finspeq.adapters.outbound.onnx_model import (
    OnnxBatchScorer,
    OnnxExporter,
    OnnxFraudModel,
    float32_exact_model,
)
from finspeq.adapters.outbound.parquet import ParquetTrainingSink
from finspeq.adapters.outbound.training_data import load_split
from finspeq.application.messages import DatasetRef, Split, TrainingRow, TrainParams
from finspeq.application.modeling import EvaluateScorer, VerifyParity
from finspeq.domain.errors import SchemaMismatchError
from finspeq.domain.features import FEATURE_SET_V1, FeatureVector
from finspeq.domain.model import EventTime
from finspeq.domain.policy import Thresholds
from tests.factories import tx_id

SPLIT_SIZES = ((Split.TRAIN, 3_000), (Split.VALID, 1_000), (Split.TEST, 1_000))
PARAMS = TrainParams(max_rounds=40, early_stopping_rounds=10, min_data_in_leaf=20, num_threads=1)
THRESHOLDS = Thresholds(review=0.3, block=0.7)


def synthetic_row(rng: random.Random, seq: int, split: Split) -> TrainingRow:
    """Return a row whose label depends on amount and hour, with label noise."""
    width = len(FEATURE_SET_V1.names)
    values = [rng.random() * 10 for _ in range(width)]
    values[0] = rng.lognormvariate(3.5, 1.2)
    values[FEATURE_SET_V1.names.index("hour_of_day_utc")] = float(rng.randrange(24))
    is_fraud = (values[0] > 150 and rng.random() < 0.8) or rng.random() < 0.01
    return TrainingRow(
        seq=seq,
        time=EventTime(1_700_000_000 + seq),
        tx_id=tx_id(seq + 1),
        vector=FeatureVector(FEATURE_SET_V1.version, tuple(values)),
        is_fraud=is_fraud,
        split=split,
    )


@pytest.fixture(scope="module")
def dataset(tmp_path_factory: pytest.TempPathFactory) -> DatasetRef:
    """Write a 5,000-row synthetic dataset part and manifest; return its reference."""
    directory = tmp_path_factory.mktemp("dataset")
    sink = ParquetTrainingSink(
        directory / "part-0000.parquet", FEATURE_SET_V1.version, FEATURE_SET_V1.names
    )
    rng = random.Random(7)
    seq = 0
    for split, size in SPLIT_SIZES:
        for _ in range(size):
            sink.write(synthetic_row(rng, seq, split))
            seq += 1
    sink.close()
    write_manifest(directory)
    return dataset_ref(directory, FEATURE_SET_V1.version)


def test_load_split_returns_rows_of_one_split(dataset: DatasetRef) -> None:
    """Return 1,000 VALID rows with one column per feature."""
    matrix, labels = load_split(dataset, Split.VALID)
    assert matrix.shape == (1_000, len(FEATURE_SET_V1.names))
    assert labels.shape == (1_000,)


def test_training_is_deterministic(dataset: DatasetRef, tmp_path: Path) -> None:
    """Train twice with the same inputs and get the same model version."""
    first = LightGBMTrainer().train(dataset, PARAMS, str(tmp_path / "a"))
    second = LightGBMTrainer().train(dataset, PARAMS, str(tmp_path / "b"))
    assert first.version == second.version
    assert first.dataset_manifest_sha256 != ""


def test_model_file_has_lf_line_endings(dataset: DatasetRef, tmp_path: Path) -> None:
    """Write the model with LF only; CRLF corrupts LightGBM's recorded tree sizes."""
    artifact = LightGBMTrainer().train(dataset, PARAMS, str(tmp_path / "model"))
    assert b"\r" not in Path(artifact.path).read_bytes()


def test_onnx_export_matches_lightgbm(dataset: DatasetRef, tmp_path: Path) -> None:
    """Export to ONNX and match LightGBM within 1e-5 with no decision mismatches."""
    artifact = LightGBMTrainer().train(dataset, PARAMS, str(tmp_path / "model"))
    ref = OnnxExporter().export(artifact, THRESHOLDS, str(tmp_path / "release"))
    report = VerifyParity(LightGBMScorer(artifact.path, 1), OnnxBatchScorer(ref.directory)).execute(
        dataset, Split.TEST, THRESHOLDS, tolerance=1e-5
    )
    assert report.rows == 1_000
    assert report.max_abs_diff <= 1e-5
    assert report.decision_mismatches == 0
    assert Path(ref.directory).name == ref.version


def test_export_is_byte_reproducible(dataset: DatasetRef, tmp_path: Path) -> None:
    """Export the same artifact twice into separate roots and get the same version."""
    artifact = LightGBMTrainer().train(dataset, PARAMS, str(tmp_path / "model"))
    first = OnnxExporter().export(artifact, THRESHOLDS, str(tmp_path / "a"))
    second = OnnxExporter().export(artifact, THRESHOLDS, str(tmp_path / "b"))
    assert first.version == second.version


def test_float32_exact_model_keeps_lightgbm_predictions(
    dataset: DatasetRef, tmp_path: Path
) -> None:
    """Predict identically with the rewritten model on float32 inputs; store float32 thresholds."""
    import lightgbm as lgb
    import numpy as np

    artifact = LightGBMTrainer().train(dataset, PARAMS, str(tmp_path / "model"))
    text = Path(artifact.path).read_text(encoding="utf-8")
    rewritten = float32_exact_model(text)
    matrix, _ = load_split(dataset, Split.TEST)
    original = lgb.Booster(model_str=text).predict(matrix)
    exact = lgb.Booster(model_str=rewritten).predict(matrix)
    assert np.array_equal(original, exact)
    assert "tree_sizes=" not in rewritten
    for line in rewritten.split("\n"):
        if line.startswith("threshold="):
            for value in line.split("=", 1)[1].split():
                assert float(np.float32(float(value))) == float(value)


def test_float32_exact_model_rounds_thresholds_down() -> None:
    """Replace a threshold between two float32 values by the lower one."""
    import numpy as np

    upper = np.float32(100.1)
    lower = np.nextafter(upper, np.float32(0))
    midpoint = (float(lower) + float(upper)) / 2
    rewritten = float32_exact_model(f"tree_sizes=12\nthreshold={midpoint!r} 1.5\nend")
    assert rewritten == f"threshold={float(lower)!r} 1.5\nend"


def test_single_vector_model_matches_batch_scorer(dataset: DatasetRef, tmp_path: Path) -> None:
    """Return the batch scorer's probability for each single vector, and check its schema."""
    artifact = LightGBMTrainer().train(dataset, PARAMS, str(tmp_path / "model"))
    ref = OnnxExporter().export(artifact, THRESHOLDS, str(tmp_path / "release"))
    model = OnnxFraudModel(ref.directory)
    batch = OnnxBatchScorer(ref.directory).score(dataset, Split.TEST)
    matrix, _ = load_split(dataset, Split.TEST)
    for row, expected in zip(matrix[:50].tolist(), batch.scores[:50], strict=True):
        vector = FeatureVector(FEATURE_SET_V1.version, tuple(row))
        assert model.predict(vector).fraud_probability == pytest.approx(expected, abs=1e-7)
    assert model.thresholds == THRESHOLDS
    with pytest.raises(SchemaMismatchError):
        model.predict(replace(FeatureVector("v1", tuple(matrix[0].tolist())), schema="v0"))


def test_baselines_rank_better_than_chance(dataset: DatasetRef) -> None:
    """Give both baselines a TEST ROC-AUC above 0.5 on the synthetic rule."""
    for scorer in (RuleBaselineScorer(), LogisticBaselineScorer(stride=2)):
        (result,) = EvaluateScorer(scorer).execute(dataset, (Split.TEST,))
        assert result.evaluation.roc_auc > 0.5
