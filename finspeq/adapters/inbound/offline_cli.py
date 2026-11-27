"""Command-line arguments of the offline commands: dataset, training, export, lateness sweep."""

from __future__ import annotations

import argparse
import calendar
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from finspeq.adapters.inbound.selection import (
    EventSelection,
    add_disorder,
    add_selection,
    disorder_from,
    selection_from,
)
from finspeq.application.delayed_labels import LabelDelay
from finspeq.application.disorder import Disorder
from finspeq.application.messages import Imbalance, ModelArtifact, TimeRange
from finspeq.domain.model import EventTime
from finspeq.domain.policy import Thresholds

MONTHS_PER_YEAR = 12
SECONDS_PER_DAY = 86_400
EPOCH = date(1970, 1, 1)


@dataclass(frozen=True, slots=True)
class BuildDatasetCommand:
    """Arguments of `build-dataset`: one dataset part is built per month."""

    source: Path
    output_dir: Path
    months: tuple[TimeRange, ...]
    valid_start: EventTime
    test_start: EventTime
    jobs: int
    feature_set: str


@dataclass(frozen=True, slots=True)
class DatasetManifestCommand:
    """Arguments of `dataset-manifest`."""

    directory: Path


@dataclass(frozen=True, slots=True)
class TrainCommand:
    """Arguments of `train`: one candidate model per invocation."""

    dataset_dir: Path
    name: str
    imbalance: Imbalance
    threads: int
    models_dir: Path
    review_max_fpr: float
    block_max_fpr: float


@dataclass(frozen=True, slots=True)
class BaselinesCommand:
    """Arguments of `baselines`."""

    dataset_dir: Path
    output: Path
    logistic_stride: int


@dataclass(frozen=True, slots=True)
class ExportCommand:
    """Arguments of `export`: ONNX export, parity check, and latency benchmark."""

    candidate_dir: Path
    dataset_dir: Path
    models_dir: Path
    tolerance: float
    warmup_calls: int
    calls: int
    threads: int


@dataclass(frozen=True, slots=True)
class LatenessSweepCommand:
    """Arguments of `lateness-sweep`: in-process comparison of arrival and canonical order."""

    selection: EventSelection
    disorder: Disorder
    lateness_values: tuple[int, ...]
    model_dir: Path | None
    output: Path
    round_robin_partitions: int | None
    consumer_batch_size: int


@dataclass(frozen=True, slots=True)
class DriftCommand:
    """Arguments of `drift`: PSI of every dataset part (month) against the TRAIN split."""

    dataset_dir: Path
    bins: int
    alert_psi: float
    reference_output: Path
    output: Path


@dataclass(frozen=True, slots=True)
class DelayedLabelsCommand:
    """Arguments of `delayed-labels`: rolling recall on TEST as chargebacks arrive."""

    dataset_dir: Path
    model_dir: Path
    delay: LabelDelay
    window_days: int
    output: Path


Command = (
    BuildDatasetCommand
    | DriftCommand
    | DelayedLabelsCommand
    | DatasetManifestCommand
    | TrainCommand
    | BaselinesCommand
    | ExportCommand
    | LatenessSweepCommand
)
DEFAULT_LATENESS_VALUES = "0,60,600,3600,21600"
FEATURE_SET_CHOICES = ("v1", "v2")
DEFAULT_FEATURE_SET = "v2"
DEFAULT_DATASET_DIR = Path(f"data/processed/training_{DEFAULT_FEATURE_SET}")


def dataset_dir_for(feature_set: str) -> Path:
    """Return the default dataset directory for a feature-set version."""
    return Path(f"data/processed/training_{feature_set}")


def parse(argv: Sequence[str]) -> Command:
    """Return the command described by argv; exit with status 2 on invalid arguments."""
    parser = argparse.ArgumentParser(prog="finspeq-offline")
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build-dataset", help="build training-set parts, one per month")
    build.add_argument("--source", type=Path, default=Path("data/processed/events_sorted.parquet"))
    build.add_argument("--feature-set", choices=FEATURE_SET_CHOICES, default=DEFAULT_FEATURE_SET)
    build.add_argument("--output-dir", type=Path, help="default: data/processed/training_<set>")
    build.add_argument("--from-month", default="2024-12", help="first month, YYYY-MM")
    build.add_argument("--to-month", default="2026-06", help="last month (inclusive), YYYY-MM")
    build.add_argument("--valid-start", default="2026-01-01", help="first VALID day, YYYY-MM-DD")
    build.add_argument("--test-start", default="2026-03-01", help="first TEST day, YYYY-MM-DD")
    build.add_argument("--jobs", type=int, default=1, help="months built in parallel")
    manifest = commands.add_parser("dataset-manifest", help="write DATASET.json for parts")
    manifest.add_argument("--dir", type=Path, default=DEFAULT_DATASET_DIR)
    _add_model_commands(commands)
    sweep = commands.add_parser("lateness-sweep", help="feature error per allowed lateness")
    add_selection(sweep)
    add_disorder(sweep)
    sweep.add_argument("--lateness", default=DEFAULT_LATENESS_VALUES, help="seconds, comma list")
    sweep.add_argument("--model-dir", type=Path, help="ONNX model directory; omit for features")
    sweep.add_argument("--output", type=Path, default=Path("notes/m3/lateness_sweep.json"))
    sweep.add_argument(
        "--round-robin-partitions",
        type=int,
        help="model unkeyed publishing over this many partitions (M4 experiment)",
    )
    sweep.add_argument("--consumer-batch-size", type=int, default=500)
    _add_monitoring_commands(commands)
    args = parser.parse_args(list(argv))
    if args.command == "drift":
        return DriftCommand(
            args.dataset_dir, args.bins, args.alert_psi, args.reference_output, args.output
        )
    if args.command == "delayed-labels":
        delay = LabelDelay(args.min_days, args.mode_days, args.max_days, args.seed)
        return DelayedLabelsCommand(
            args.dataset_dir, args.model_dir, delay, args.window_days, args.output
        )
    if args.command == "build-dataset":
        return BuildDatasetCommand(
            source=args.source,
            output_dir=args.output_dir or dataset_dir_for(args.feature_set),
            months=month_ranges(_parse_month(args.from_month), _parse_month(args.to_month)),
            valid_start=_day_start(date.fromisoformat(args.valid_start)),
            test_start=_day_start(date.fromisoformat(args.test_start)),
            jobs=args.jobs,
            feature_set=args.feature_set,
        )
    if args.command == "dataset-manifest":
        return DatasetManifestCommand(directory=args.dir)
    if args.command == "lateness-sweep":
        return LatenessSweepCommand(
            selection=selection_from(args),
            disorder=disorder_from(args),
            lateness_values=tuple(int(value) for value in args.lateness.split(",")),
            model_dir=args.model_dir,
            output=args.output,
            round_robin_partitions=args.round_robin_partitions,
            consumer_batch_size=args.consumer_batch_size,
        )
    return _model_command(args)


def _add_model_commands(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Add the train, baselines, and export sub-commands to commands."""
    train = commands.add_parser("train", help="train and evaluate one candidate model")
    train.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR)
    train.add_argument("--name", required=True, help="candidate directory name")
    train.add_argument("--imbalance", choices=[i.value for i in Imbalance], default="none")
    train.add_argument("--threads", type=int, default=4)
    train.add_argument("--models-dir", type=Path, default=Path("models/candidates"))
    train.add_argument("--review-max-fpr", type=float, default=0.01)
    train.add_argument("--block-max-fpr", type=float, default=0.001)
    baselines = commands.add_parser("baselines", help="evaluate rule and logistic baselines")
    baselines.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR)
    baselines.add_argument("--output", type=Path, default=Path("notes/m0_baselines.json"))
    baselines.add_argument("--logistic-stride", type=int, default=4)
    export = commands.add_parser("export", help="export a candidate to ONNX and verify it")
    export.add_argument("--candidate-dir", type=Path, required=True)
    export.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR)
    export.add_argument("--models-dir", type=Path, default=Path("models"))
    export.add_argument("--tolerance", type=float, default=1e-5)
    export.add_argument("--warmup-calls", type=int, default=1_000)
    export.add_argument("--calls", type=int, default=10_000)
    export.add_argument("--threads", type=int, default=4)


def _add_monitoring_commands(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Add the drift and delayed-labels sub-commands to commands."""
    drift = commands.add_parser("drift", help="PSI per feature and month against TRAIN")
    drift.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR)
    drift.add_argument("--bins", type=int, default=10)
    drift.add_argument("--alert-psi", type=float, default=0.2)
    drift.add_argument(
        "--reference-output", type=Path, default=Path("notes/m6/drift_reference.json")
    )
    drift.add_argument("--output", type=Path, default=Path("notes/m6/drift_monthly.json"))
    labels = commands.add_parser("delayed-labels", help="recall as chargebacks arrive")
    labels.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR)
    labels.add_argument("--model-dir", type=Path, required=True)
    labels.add_argument("--min-days", type=float, default=7.0)
    labels.add_argument("--mode-days", type=float, default=21.0)
    labels.add_argument("--max-days", type=float, default=60.0)
    labels.add_argument("--seed", type=int, default=0)
    labels.add_argument("--window-days", type=int, default=30)
    labels.add_argument("--output", type=Path, default=Path("notes/m6/delayed_labels.json"))


def _model_command(args: argparse.Namespace) -> Command:
    """Return the TrainCommand, BaselinesCommand, or ExportCommand described by args."""
    if args.command == "train":
        return TrainCommand(
            dataset_dir=args.dataset_dir,
            name=args.name,
            imbalance=Imbalance(args.imbalance),
            threads=args.threads,
            models_dir=args.models_dir,
            review_max_fpr=args.review_max_fpr,
            block_max_fpr=args.block_max_fpr,
        )
    if args.command == "baselines":
        return BaselinesCommand(args.dataset_dir, args.output, args.logistic_stride)
    return ExportCommand(
        candidate_dir=args.candidate_dir,
        dataset_dir=args.dataset_dir,
        models_dir=args.models_dir,
        tolerance=args.tolerance,
        warmup_calls=args.warmup_calls,
        calls=args.calls,
        threads=args.threads,
    )


def read_candidate(path: Path) -> tuple[ModelArtifact, Thresholds]:
    """Return the model artifact and selected thresholds stored in a candidate report.json."""
    report = json.loads(path.read_text(encoding="utf-8"))["report"]
    fields = report["artifact"]
    artifact = ModelArtifact(
        path=fields["path"],
        version=fields["version"],
        schema=fields["schema"],
        feature_names=tuple(fields["feature_names"]),
        best_iteration=int(fields["best_iteration"]),
        positive_weight=float(fields["positive_weight"]),
        dataset_manifest_sha256=fields["dataset_manifest_sha256"],
    )
    values = report["selection"]["thresholds"]
    return artifact, Thresholds(review=float(values["review"]), block=float(values["block"]))


def month_ranges(first: date, last: date) -> tuple[TimeRange, ...]:
    """Return one UTC calendar-month TimeRange per month from first to last, inclusive."""
    ranges = []
    current = first.replace(day=1)
    while current <= last:
        following = _next_month(current)
        ranges.append(TimeRange(start=_day_start(current), end=_day_start(following)))
        current = following
    return tuple(ranges)


def part_name(month: TimeRange) -> str:
    """Return the part file name for a month range, for example "part-2025-01.parquet"."""
    return f"part-{month_label(month)}.parquet"


def month_label(month: TimeRange) -> str:
    """Return "YYYY-MM" of the month that contains month.start (UTC)."""
    year, month_number = _year_month(month.start)
    return f"{year:04d}-{month_number:02d}"


def _parse_month(text: str) -> date:
    """Return the first day of the month given as YYYY-MM."""
    return date.fromisoformat(f"{text}-01")


def _next_month(day: date) -> date:
    """Return the first day of the month after day's month."""
    if day.month == MONTHS_PER_YEAR:
        return date(day.year + 1, 1, 1)
    return date(day.year, day.month + 1, 1)


def _day_start(day: date) -> EventTime:
    """Return 00:00:00 UTC of day as an EventTime."""
    return EventTime(calendar.timegm(day.timetuple()))


def _year_month(time: EventTime) -> tuple[int, int]:
    """Return the UTC year and month of time."""
    day = EPOCH + timedelta(days=time.epoch_s // SECONDS_PER_DAY)
    return day.year, day.month
