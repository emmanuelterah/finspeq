"""JSON storage of a drift reference (bin edges and shares per feature)."""

from __future__ import annotations

import json
from pathlib import Path

from finspeq.application.drift import DriftReference, FeatureBins


def write_reference(reference: DriftReference, path: Path) -> None:
    """Write reference to path as indented JSON with LF line endings."""
    payload = {
        "rows": reference.rows,
        "features": [
            {"name": f.name, "edges": list(f.edges), "shares": list(f.shares)}
            for f in reference.features
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8", newline="\n")


def read_reference(path: Path) -> DriftReference:
    """Return the reference stored by write_reference at path."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    features = tuple(
        FeatureBins(
            str(item["name"]),
            tuple(float(edge) for edge in item["edges"]),
            tuple(float(share) for share in item["shares"]),
        )
        for item in payload["features"]
    )
    return DriftReference(int(payload["rows"]), features)
