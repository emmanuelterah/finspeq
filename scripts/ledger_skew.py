"""Compare the feature vectors recorded in the ledger with the offline ones for the same events.

Reads every ledger row, looks up the same tx_id in the training dataset parts, and reports per
feature: the share of events whose live value differs from the offline value, and the PSI of
the live values and of the offline values against the drift reference. Rows are split into
all rows and rows from --steady-from onwards (after the warm-up history has filled).
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import date
from pathlib import Path

import psycopg
import pyarrow.compute as pc
import pyarrow.parquet as pq

from finspeq.adapters.outbound.drift_reference import read_reference
from finspeq.adapters.outbound.postgres_ledger import PostgresLedger
from finspeq.application.drift import DriftReference, bin_counts, psi
from finspeq.application.messages import LedgerRow

READER = "reader"
TOLERANCE = 1e-4
SECONDS_PER_DAY = 86_400


def offline_vectors(
    dataset: Path, names: list[str], tx_ids: set[str]
) -> dict[str, tuple[float, ...]]:
    """Return the dataset's feature vector of every tx_id in tx_ids that the parts contain."""
    wanted = pc.field("tx_id").isin(list(tx_ids))
    vectors: dict[str, tuple[float, ...]] = {}
    for path in sorted(dataset.glob("part-*.parquet")):
        table = pq.read_table(path, columns=["tx_id", *names], filters=wanted)
        columns = [table.column(name).to_pylist() for name in names]
        for row, tx_id in enumerate(table.column("tx_id").to_pylist()):
            vectors[tx_id] = tuple(float(column[row]) for column in columns)
    return vectors


def differs(live: float, offline: float) -> bool:
    """Return True if live and offline differ by more than TOLERANCE, relative to their size."""
    return abs(live - offline) > TOLERANCE * max(1.0, abs(offline))


def compare(
    pairs: list[tuple[tuple[float, ...], tuple[float, ...]]], reference: DriftReference
) -> dict[str, object]:
    """Return per-feature mismatch shares and live and offline PSI over pairs."""
    features: dict[str, dict[str, float]] = {}
    for index, bins in enumerate(reference.features):
        live = [pair[0][index] for pair in pairs]
        offline = [pair[1][index] for pair in pairs]
        mismatched = sum(1 for a, b in zip(live, offline, strict=True) if differs(a, b))
        features[bins.name] = {
            "mismatch": mismatched / len(pairs) if pairs else math.nan,
            "psi_live": psi(bins.shares, bin_counts(live, bins.edges)),
            "psi_offline": psi(bins.shares, bin_counts(offline, bins.edges)),
        }
    any_mismatch = sum(1 for live, offline in pairs if any(map(differs, live, offline)))
    return {
        "rows": len(pairs),
        "vectors_mismatched": any_mismatch / len(pairs) if pairs else math.nan,
        "features": features,
    }


def main() -> None:
    """Run the comparison and write the report as JSON."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", default="postgresql://finspeq:finspeq@127.0.0.1:15432/finspeq")
    parser.add_argument("--dataset-dir", type=Path, default=Path("data/processed/training_v2"))
    parser.add_argument("--reference", type=Path, default=Path("notes/m6/drift_reference.json"))
    parser.add_argument("--steady-from", type=date.fromisoformat, default=date(2026, 3, 12))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    reference = read_reference(args.reference)
    names = [bins.name for bins in reference.features]
    with psycopg.connect(args.dsn, autocommit=True) as connection:
        rows: list[LedgerRow] = list(PostgresLedger(connection, READER, READER).rows())
    offline = offline_vectors(args.dataset_dir, names, {row.tx_id.value for row in rows})
    steady = (args.steady_from - date(1970, 1, 1)).days * SECONDS_PER_DAY
    pairs = [
        (row.record.vector.values, offline[row.tx_id.value], row.time.epoch_s >= steady)
        for row in rows
        if row.tx_id.value in offline
    ]
    report = {
        "ledger_rows": len(rows),
        "found_offline": len(pairs),
        "all": compare([(a, b) for a, b, _ in pairs], reference),
        "steady": compare([(a, b) for a, b, late in pairs if late], reference),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(report, indent=2) + "\n"
    args.output.write_text(text, encoding="utf-8", newline="\n")
    print(json.dumps({k: v for k, v in report.items() if k != "all"}, indent=2))


if __name__ == "__main__":
    main()
