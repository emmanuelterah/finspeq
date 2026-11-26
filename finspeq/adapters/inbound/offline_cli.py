"""Command-line arguments of the offline commands: dataset, training, baselines, export."""

from __future__ import annotations

import argparse
import calendar
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

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


Command = (
    BuildDatasetCommand | DatasetManifestCommand | TrainCommand | BaselinesCommand | ExportCommand
)
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
    args = parser.parse_args(list(argv))
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
