"""Compare the project's PSI with Evidently's PSI and default drift test on the same samples.

Draws a seeded sample of TRAIN rows as the reference and a seeded sample of each monthly part
as the current data. For every feature and month it records: the project's PSI (decile edges
from the drift reference, as `finspeq-offline drift` uses), Evidently's PSI (equal-width
Sturges bins over reference and current together), and Evidently's default test for numerical
columns with more than 1,000 rows (normed Wasserstein distance), each with its alert, and the
time each tool took.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import numpy.typing as npt
import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq
from evidently import DataDefinition, Dataset, Report
from evidently.metrics import ValueDrift

from finspeq.adapters.outbound.drift_reference import read_reference
from finspeq.application.drift import DriftReference, psi

PSI_ALERT = 0.2
WASSERSTEIN_ALERT = 0.1
TRAIN = "train"


@dataclass(frozen=True, slots=True)
class Scores:
    """One feature's drift scores for one month from the three methods."""

    project_psi: float
    evidently_psi: float
    evidently_wasserstein: float


@dataclass(frozen=True, slots=True)
class MonthResult:
    """One month's sample size, each tool's time in seconds, and the scores per feature."""

    month: str
    rows: int
    project_s: float
    evidently_s: float
    features: dict[str, Scores]


def sample_rows(
    path: Path, names: list[str], fraction: float, rng: np.random.Generator, split: str | None
) -> pd.DataFrame:
    """Return a seeded fraction of path's rows (only rows of split, when given) as a frame."""
    columns = [*names, "split"] if split else names
    table = pq.read_table(path, columns=columns)
    if split:
        table = table.filter(pc.equal(table.column("split"), split)).select(names)
    keep = rng.random(table.num_rows) < fraction
    return table.filter(keep).to_pandas().astype(np.float64)


def project_psi(reference: DriftReference, frame: pd.DataFrame) -> dict[str, float]:
    """Return the project's PSI of every reference feature over frame's values."""
    result = {}
    for bins in reference.features:
        values: npt.NDArray[np.float64] = frame[bins.name].to_numpy()
        indexes = np.searchsorted(np.asarray(bins.edges), values, side="right")
        counts = np.bincount(indexes, minlength=len(bins.edges) + 1)
        result[bins.name] = psi(bins.shares, [int(n) for n in counts])
    return result


def evidently_scores(
    reference: pd.DataFrame, current: pd.DataFrame, names: list[str]
) -> tuple[dict[str, float], dict[str, float]]:
    """Return Evidently's PSI and default-test score of every column in names."""
    definition = DataDefinition(numerical_columns=names)
    metrics = [ValueDrift(column=name, method="psi", threshold=PSI_ALERT) for name in names]
    metrics += [ValueDrift(column=name) for name in names]
    snapshot = Report(metrics).run(
        Dataset.from_pandas(current, data_definition=definition),
        Dataset.from_pandas(reference, data_definition=definition),
    )
    values = [float(metric["value"]) for metric in snapshot.dict()["metrics"]]
    count = len(names)
    return dict(zip(names, values[:count], strict=True)), dict(
        zip(names, values[count:], strict=True)
    )


def main() -> None:
    """Run the comparison over every monthly part and write the report as JSON."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dataset-dir", type=Path, default=Path("data/processed/training_v2"))
    parser.add_argument("--reference", type=Path, default=Path("notes/m6/drift_reference.json"))
    parser.add_argument("--reference-rows", type=int, default=200_000)
    parser.add_argument("--month-rows", type=int, default=100_000)
    parser.add_argument("--skip", default="2024-12", help="months left out, comma list")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    reference = read_reference(args.reference)
    names = [bins.name for bins in reference.features]
    rng = np.random.default_rng(args.seed)
    parts = sorted(args.dataset_dir.glob("part-*.parquet"))
    fraction = args.reference_rows / reference.rows
    baseline = pd.concat([sample_rows(p, names, fraction, rng, TRAIN) for p in parts])
    skipped = set(args.skip.split(","))
    months = []
    for path in parts:
        label = path.stem.removeprefix("part-")
        if label in skipped:
            continue
        rows = pq.ParquetFile(path).metadata.num_rows
        current = sample_rows(path, names, min(1.0, args.month_rows / rows), rng, None)
        started = time.perf_counter()
        ours = project_psi(reference, current)
        ours_s = time.perf_counter() - started
        started = time.perf_counter()
        their_psi, their_default = evidently_scores(baseline, current, names)
        theirs_s = time.perf_counter() - started
        features = {
            name: Scores(ours[name], their_psi[name], their_default[name]) for name in names
        }
        months.append(MonthResult(label, len(current), ours_s, theirs_s, features))
        print(label, f"project {ours_s:.2f} s, evidently {theirs_s:.2f} s", flush=True)
    report = {
        "reference_rows": len(baseline),
        "psi_alert": PSI_ALERT,
        "wasserstein_alert": WASSERSTEIN_ALERT,
        "alerts": alert_summary(months, names),
        "months": [asdict(month) for month in months],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(report, indent=2) + "\n"
    args.output.write_text(text, encoding="utf-8", newline="\n")
    print(json.dumps(report["alerts"], indent=2))


def alert_summary(months: list[MonthResult], names: list[str]) -> dict[str, object]:
    """Return, per feature, how many months each method alerts in and the months PSIs disagree.

    The project alerts when its PSI is above PSI_ALERT; Evidently alerts at or above its
    threshold.
    """
    summary: dict[str, object] = {}
    for name in names:
        flags: dict[str, list[str]] = {"project_psi": [], "evidently_psi": [], "wasserstein": []}
        for month in months:
            scores = month.features[name]
            if scores.project_psi > PSI_ALERT:
                flags["project_psi"].append(month.month)
            if scores.evidently_psi >= PSI_ALERT:
                flags["evidently_psi"].append(month.month)
            if scores.evidently_wasserstein >= WASSERSTEIN_ALERT:
                flags["wasserstein"].append(month.month)
        summary[name] = {key: len(value) for key, value in flags.items()} | {
            "psi_disagree": sorted(set(flags["project_psi"]) ^ set(flags["evidently_psi"]))
        }
    return summary


if __name__ == "__main__":
    main()
