"""Loading of training dataset parts into numpy arrays for model adapters."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import numpy.typing as npt
import pyarrow.compute as pc
import pyarrow.parquet as pq

from finspeq.adapters.outbound.dataset_manifest import MANIFEST_NAME, PART_PATTERN
from finspeq.adapters.outbound.parquet import FEATURE_NAMES_KEY, FEATURE_SCHEMA_KEY
from finspeq.application.errors import InvalidSourceDataError
from finspeq.application.messages import DatasetRef, Split

READ_BATCH_ROWS = 262_144
Matrix = npt.NDArray[np.float32]
Labels = npt.NDArray[np.bool_]


def part_paths(dataset: DatasetRef) -> list[Path]:
    """Return the dataset's part files in name order.

    Raises:
        InvalidSourceDataError: The dataset directory has no part files.
    """
    parts = sorted(Path(dataset.path).glob(PART_PATTERN))
    if not parts:
        raise InvalidSourceDataError(f"no {PART_PATTERN} files in {dataset.path}")
    return parts


def feature_layout(dataset: DatasetRef) -> tuple[str, tuple[str, ...]]:
    """Return the feature schema and feature names shared by every part of the dataset.

    Raises:
        InvalidSourceDataError: Parts disagree on schema or names, or metadata is missing.
    """
    layouts = set()
    for path in part_paths(dataset):
        metadata = pq.read_schema(path).metadata or {}
        if FEATURE_SCHEMA_KEY not in metadata or FEATURE_NAMES_KEY not in metadata:
            raise InvalidSourceDataError(f"{path.name} has no feature metadata")
        names = tuple(json.loads(metadata[FEATURE_NAMES_KEY]))
        layouts.add((metadata[FEATURE_SCHEMA_KEY].decode(), names))
    if len(layouts) != 1:
        raise InvalidSourceDataError(f"parts disagree on feature layout: {sorted(layouts)}")
    schema, names = layouts.pop()
    return schema, names


def manifest_sha256(dataset: DatasetRef) -> str:
    """Return the SHA-256 of the dataset's DATASET.json, or "" if it does not exist."""
    path = Path(dataset.path) / MANIFEST_NAME
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else ""


def load_split(dataset: DatasetRef, split: Split) -> tuple[Matrix, Labels]:
    """Return the feature matrix (float32, feature order) and labels of split, in dataset order.

    Raises:
        InvalidSourceDataError: The dataset has no parts or inconsistent feature metadata.
    """
    _, names = feature_layout(dataset)
    matrices: list[Matrix] = []
    labels: list[Labels] = []
    columns = [*names, "is_fraud", "split"]
    for path in part_paths(dataset):
        for batch in pq.ParquetFile(path).iter_batches(batch_size=READ_BATCH_ROWS, columns=columns):
            rows = batch.filter(pc.equal(batch.column("split"), split.value))
            if rows.num_rows == 0:
                continue
            matrix = np.empty((rows.num_rows, len(names)), dtype=np.float32)
            for index, name in enumerate(names):
                matrix[:, index] = rows.column(name).to_numpy()
            matrices.append(matrix)
            labels.append(rows.column("is_fraud").to_numpy(zero_copy_only=False).astype(np.bool_))
    if not matrices:
        return np.empty((0, len(names)), dtype=np.float32), np.empty(0, dtype=np.bool_)
    return np.concatenate(matrices), np.concatenate(labels)


def load_split_times(dataset: DatasetRef, split: Split) -> npt.NDArray[np.int64]:
    """Return the event time (epoch seconds) of every row of split, in load_split's order."""
    times: list[npt.NDArray[np.int64]] = []
    for path in part_paths(dataset):
        columns = ["time", "split"]
        for batch in pq.ParquetFile(path).iter_batches(batch_size=READ_BATCH_ROWS, columns=columns):
            rows = batch.filter(pc.equal(batch.column("split"), split.value))
            if rows.num_rows:
                times.append(rows.column("time").to_numpy().astype(np.int64))
    return np.concatenate(times) if times else np.empty(0, dtype=np.int64)
