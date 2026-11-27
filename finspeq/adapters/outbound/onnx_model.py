"""ONNX export of LightGBM models and batch scoring of dataset splits with ONNX Runtime."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from importlib.metadata import version as package_version
from pathlib import Path

import lightgbm as lgb
import numpy as np
from onnxmltools import convert_lightgbm
from onnxmltools.convert.common.data_types import FloatTensorType

from finspeq.adapters.outbound.onnx_runtime import (
    INPUT_NAME,
    METADATA_FILE,
    MODEL_FILE,
    PROBABILITIES_OUTPUT,
    OnnxFraudModel,
    onnx_session,
)
from finspeq.adapters.outbound.training_data import load_split
from finspeq.application.messages import (
    DatasetRef,
    ModelArtifact,
    ModelRef,
    ScoredSplit,
    Split,
)
from finspeq.application.ports import BatchScorer, ModelExporter
from finspeq.domain.policy import Thresholds

__all__ = ["OnnxBatchScorer", "OnnxExporter", "OnnxFraudModel", "float32_exact_model"]

VERSION_HEX_CHARS = 12
BATCH_ROWS = 100_000
LIBRARIES = ("lightgbm", "onnx", "onnxmltools", "onnxruntime")
THRESHOLD_PREFIX = "threshold="
GRAPH_NAME_PREFIX = "finspeq_lightgbm_"
TREE_SIZES_PREFIX = "tree_sizes="


class OnnxExporter(ModelExporter):
    """Converts LightGBM model files to ONNX with a metadata.json beside them."""

    def export(self, artifact: ModelArtifact, thresholds: Thresholds, root: str) -> ModelRef:
        """Write model.onnx and metadata.json into root/<version>/ and return its reference.

        Converts float32_exact_model(artifact) and names the graph after artifact.version; the
        same artifact yields the same model.onnx bytes. <version> is the first 12 hex
        characters of the SHA-256 of model.onnx. Files are written into a temporary directory
        that is renamed when complete. An existing directory with the same version is left
        unchanged.
        """
        model_text = Path(artifact.path).read_text(encoding="utf-8")
        booster = lgb.Booster(model_str=float32_exact_model(model_text))
        initial_types = [(INPUT_NAME, FloatTensorType([None, len(artifact.feature_names)]))]
        model = convert_lightgbm(booster, initial_types=initial_types, zipmap=False)
        model.graph.name = f"{GRAPH_NAME_PREFIX}{artifact.version}"
        model_bytes = model.SerializeToString()
        version = hashlib.sha256(model_bytes).hexdigest()[:VERSION_HEX_CHARS]
        target = Path(root) / version
        if not target.exists():
            temporary = Path(root) / f"{version}.tmp"
            shutil.rmtree(temporary, ignore_errors=True)
            temporary.mkdir(parents=True)
            (temporary / MODEL_FILE).write_bytes(model_bytes)
            metadata = _metadata(artifact, thresholds, version)
            text = json.dumps(metadata, indent=2) + "\n"
            (temporary / METADATA_FILE).write_text(text, encoding="utf-8", newline="\n")
            os.replace(temporary, target)
        return ModelRef(directory=target.as_posix(), version=version, schema=artifact.schema)


class OnnxBatchScorer(BatchScorer):
    """Scores dataset splits with an exported ONNX model in batches."""

    def __init__(self, directory: str) -> None:
        """Load model.onnx from an exported model directory."""
        self._session = onnx_session(Path(directory) / MODEL_FILE)

    def score(self, dataset: DatasetRef, split: Split) -> ScoredSplit:
        """Return the model's fraud probability and the label of every row of split."""
        matrix, labels = load_split(dataset, split)
        scores: list[float] = []
        for start in range(0, len(matrix), BATCH_ROWS):
            batch = matrix[start : start + BATCH_ROWS]
            output = self._session.run([PROBABILITIES_OUTPUT], {INPUT_NAME: batch})[0]
            scores.extend(np.asarray(output[:, 1], dtype=np.float64).tolist())
        return ScoredSplit(split, tuple(scores), tuple(labels.tolist()))


def float32_exact_model(model_text: str) -> str:
    """Return LightGBM model text with every split threshold rounded down to a float32 value.

    Each threshold t becomes the largest float32 not greater than t. For float32 inputs the
    returned model makes the same split decisions as the original. The `tree_sizes` header line
    is removed because the rewritten trees have different byte sizes.
    """
    lines = []
    for line in model_text.split("\n"):
        if line.startswith(TREE_SIZES_PREFIX):
            continue
        if line.startswith(THRESHOLD_PREFIX):
            values = line[len(THRESHOLD_PREFIX) :].split()
            line = THRESHOLD_PREFIX + " ".join(repr(_float32_floor(float(v))) for v in values)
        lines.append(line)
    return "\n".join(lines)


def _float32_floor(value: float) -> float:
    """Return the largest float32 value not greater than value, as a float."""
    rounded = np.float32(value)
    if float(rounded) > value:
        rounded = np.nextafter(rounded, np.float32(-np.inf))
    return float(rounded)


def _metadata(artifact: ModelArtifact, thresholds: Thresholds, version: str) -> dict[str, object]:
    """Return the metadata.json content for an exported model."""
    return {
        "model_version": version,
        "feature_schema": artifact.schema,
        "feature_names": list(artifact.feature_names),
        "thresholds": {"review": thresholds.review, "block": thresholds.block},
        "source_model_version": artifact.version,
        "best_iteration": artifact.best_iteration,
        "positive_weight": artifact.positive_weight,
        "dataset_manifest_sha256": artifact.dataset_manifest_sha256,
        "libraries": {name: package_version(name) for name in LIBRARIES},
    }
