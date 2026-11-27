"""ONNX Runtime inference of exported fraud models (FraudModel), for the scoring API."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import onnxruntime as ort

from finspeq.application.ports import FraudModel
from finspeq.domain.errors import SchemaMismatchError
from finspeq.domain.features import FeatureVector
from finspeq.domain.policy import Score, Thresholds

MODEL_FILE = "model.onnx"
METADATA_FILE = "metadata.json"
INPUT_NAME = "features"
PROBABILITIES_OUTPUT = "probabilities"


class OnnxFraudModel(FraudModel):
    """Single-vector fraud scoring with ONNX Runtime on one CPU thread."""

    def __init__(self, directory: str) -> None:
        """Load model.onnx and metadata.json from an exported model directory."""
        metadata_path = Path(directory) / METADATA_FILE
        self._metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        self._session = onnx_session(Path(directory) / MODEL_FILE)

    @property
    def version(self) -> str:
        """Return the exported model's version."""
        return str(self._metadata["model_version"])

    @property
    def schema(self) -> str:
        """Return the feature-set version the model was trained on."""
        return str(self._metadata["feature_schema"])

    @property
    def thresholds(self) -> Thresholds:
        """Return the REVIEW and BLOCK thresholds stored with the model."""
        values = self._metadata["thresholds"]
        return Thresholds(review=float(values["review"]), block=float(values["block"]))

    def predict(self, vector: FeatureVector) -> Score:
        """Return the fraud probability for vector, computed on float32 inputs.

        Raises:
            SchemaMismatchError: vector.schema differs from the model's schema.
        """
        if vector.schema != self.schema:
            raise SchemaMismatchError(f"model expects {self.schema}, got {vector.schema}")
        inputs = np.asarray([vector.values], dtype=np.float32)
        probabilities = self._session.run([PROBABILITIES_OUTPUT], {INPUT_NAME: inputs})[0]
        return Score(min(1.0, max(0.0, float(probabilities[0][1]))))


def onnx_session(path: Path) -> ort.InferenceSession:
    """Return an ONNX Runtime CPU session for path with one intra-op and one inter-op thread."""
    options = ort.SessionOptions()
    options.intra_op_num_threads = 1
    options.inter_op_num_threads = 1
    return ort.InferenceSession(str(path), sess_options=options, providers=["CPUExecutionProvider"])
