"""Drift statistics over training dataset parts, computed with numpy."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import numpy as np
import numpy.typing as npt
import pyarrow.parquet as pq

from finspeq.adapters.outbound.training_data import (
    READ_BATCH_ROWS,
    feature_layout,
    load_split,
    part_paths,
)
from finspeq.application.drift import DriftReference, FeatureBins
from finspeq.application.messages import DatasetRef, Split

PART_PREFIX = "part-"


def reference_from_split(dataset: DatasetRef, split: Split, bins: int) -> DriftReference:
    """Return decile-style bins of every feature over split, with each bin's share.

    Edges are the split's quantiles at 1/bins, 2/bins, ..., (bins-1)/bins; repeated edges
    (features with few distinct values) are merged.
    """
    _, names = feature_layout(dataset)
    matrix, _ = load_split(dataset, split)
    quantiles = np.linspace(0, 1, bins + 1)[1:-1]
    features = []
    for index, name in enumerate(names):
        column = matrix[:, index].astype(np.float64)
        edges = np.unique(np.quantile(column, quantiles))
        counts = _counts(column, edges)
        shares = counts / counts.sum()
        features.append(FeatureBins(name, tuple(edges.tolist()), tuple(float(s) for s in shares)))
    return DriftReference(int(matrix.shape[0]), tuple(features))


def part_counts(
    dataset: DatasetRef, reference: DriftReference
) -> Iterator[tuple[str, dict[str, list[int]]]]:
    """Yield each part's label ("2026-03") and its per-feature bin counts over all its rows."""
    for path in part_paths(dataset):
        totals = {f.name: np.zeros(len(f.edges) + 1, dtype=np.int64) for f in reference.features}
        names = [f.name for f in reference.features]
        for batch in pq.ParquetFile(path).iter_batches(batch_size=READ_BATCH_ROWS, columns=names):
            for feature in reference.features:
                column = batch.column(feature.name).to_numpy().astype(np.float64)
                totals[feature.name] += _counts(column, np.asarray(feature.edges))
        yield _label(path), {name: counts.tolist() for name, counts in totals.items()}


def _counts(
    column: npt.NDArray[np.float64], edges: npt.NDArray[np.float64]
) -> npt.NDArray[np.int64]:
    """Return the count of column values in each bin, as bisect_right on edges assigns them."""
    indexes = np.searchsorted(edges, column, side="right")
    return np.bincount(indexes, minlength=len(edges) + 1).astype(np.int64)


def _label(path: Path) -> str:
    """Return the month label of a part file name, for example "2026-03"."""
    return path.stem.removeprefix(PART_PREFIX)
