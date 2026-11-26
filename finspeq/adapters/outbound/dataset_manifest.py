"""Manifest of a training dataset directory: parts, row counts per split, and hashes."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq

from finspeq.application.messages import DatasetRef

PART_PATTERN = "part-*.parquet"
MANIFEST_NAME = "DATASET.json"
HASH_CHUNK_BYTES = 1 << 20


def write_manifest(directory: Path) -> Path:
    """Write DATASET.json describing every part file in directory and return its path.

    Each part entry holds the file name, rows, fraud rows and rows per split, and SHA-256.
    Parts are listed in file-name order.
    """
    parts = [_describe(path) for path in sorted(directory.glob(PART_PATTERN))]
    totals: dict[str, int] = {}
    for part in parts:
        for key, value in _counts(part).items():
            totals[key] = totals.get(key, 0) + value
    manifest = {"parts": parts, "totals": totals}
    path = directory / MANIFEST_NAME
    text = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    path.write_text(text, encoding="utf-8", newline="\n")
    return path


def dataset_ref(directory: Path, schema: str) -> DatasetRef:
    """Return a reference to the dataset in directory with the total rows from DATASET.json.

    Rows is 0 when DATASET.json does not exist.
    """
    path = directory / MANIFEST_NAME
    rows = 0
    if path.exists():
        rows = int(json.loads(path.read_text(encoding="utf-8"))["totals"].get("rows", 0))
    return DatasetRef(path=directory.as_posix(), rows=rows, schema=schema)


def _describe(path: Path) -> dict[str, object]:
    """Return the manifest entry for one part file."""
    table = pq.read_table(path, columns=["split", "is_fraud"])
    rows_by_split: dict[str, int] = {}
    frauds_by_split: dict[str, int] = {}
    for split, is_fraud in zip(
        table.column("split").to_pylist(), table.column("is_fraud").to_pylist(), strict=True
    ):
        rows_by_split[split] = rows_by_split.get(split, 0) + 1
        frauds_by_split[split] = frauds_by_split.get(split, 0) + int(is_fraud)
    return {
        "file": path.name,
        "rows": table.num_rows,
        "rows_by_split": rows_by_split,
        "frauds_by_split": frauds_by_split,
        "sha256": _sha256(path),
    }


def _counts(part: dict[str, object]) -> dict[str, int]:
    """Return a part's counts as flat keys: rows, rows_<split>, frauds_<split>."""
    counts = {"rows": int(str(part["rows"]))}
    for prefix in ("rows_by_split", "frauds_by_split"):
        values = part[prefix]
        if isinstance(values, dict):
            label = prefix.split("_", 1)[0]
            counts.update({f"{label}_{split}": int(n) for split, n in values.items()})
    return counts


def _sha256(path: Path) -> str:
    """Return the hexadecimal SHA-256 of the file's bytes."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(HASH_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()
