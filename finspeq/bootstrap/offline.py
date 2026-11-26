"""Composition root of the offline process: datasets, training, baselines, export, lateness."""

from __future__ import annotations

import sys
from collections.abc import Callable, Sequence
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from finspeq.adapters.inbound import offline_cli
from finspeq.adapters.inbound.output import render, write_json
from finspeq.adapters.outbound.baselines import LogisticBaselineScorer, RuleBaselineScorer
from finspeq.adapters.outbound.clock import SystemClock
from finspeq.adapters.outbound.dataset_manifest import dataset_ref, write_manifest
from finspeq.adapters.outbound.lightgbm_model import LightGBMScorer, LightGBMTrainer
from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.adapters.outbound.onnx_model import OnnxBatchScorer, OnnxExporter, OnnxFraudModel
from finspeq.adapters.outbound.parquet import ParquetHistoricalSource, ParquetTrainingSink
from finspeq.adapters.outbound.training_data import feature_layout, load_split
from finspeq.application.build_dataset import BuildPlan, BuildTrainingSet, SplitBoundaries
from finspeq.application.disorder import arrival_order, round_robin_consumption
from finspeq.application.lateness import LatenessReport, LatenessSweep, Scoring
from finspeq.application.messages import (
    DatasetSummary,
    ModelArtifact,
    Split,
    TimeRange,
    TrainParams,
)
from finspeq.application.modeling import (
    BenchmarkModel,
    DevelopmentReport,
    DevelopModel,
    EvaluateScorer,
    ThresholdPolicy,
    VerifyParity,
)
from finspeq.domain.errors import SchemaMismatchError
from finspeq.domain.features import FEATURE_SET_V2, FEATURE_SETS, FeatureSet, FeatureVector
from finspeq.domain.policy import DecisionPolicy

BENCHMARK_VECTORS = 1_000
EVALUATED_SPLITS = (Split.VALID, Split.TEST)


def build_dataset_part(
    source: Path, output: Path, month: TimeRange, plan: BuildPlan
) -> DatasetSummary:
    """Build the training rows of one month with plan's feature set and write them to output.

    Uses a fresh in-memory window store, warmed up with the 24 hours before the month.
    """
    feature_set = plan.feature_set
    use_case = BuildTrainingSet(
        source=ParquetHistoricalSource(source),
        store=MemoryWindowStore(),
        sink=ParquetTrainingSink(output, feature_set.version, feature_set.names),
        plan=plan,
    )
    return use_case.execute(month)


def run_build_dataset(command: offline_cli.BuildDatasetCommand) -> list[DatasetSummary]:
    """Build every month of command as a part file, then rewrite the dataset manifest.

    Months run in up to command.jobs worker processes. Results are returned in month order.
    """
    boundaries = SplitBoundaries(command.valid_start, command.test_start)
    plan = BuildPlan(feature_set=FEATURE_SETS[command.feature_set], boundaries=boundaries)
    outputs = [command.output_dir / offline_cli.part_name(month) for month in command.months]
    arguments = (
        [command.source] * len(outputs),
        outputs,
        list(command.months),
        [plan] * len(outputs),
    )
    if command.jobs <= 1:
        summaries = list(map(build_dataset_part, *arguments))
    else:
        with ProcessPoolExecutor(max_workers=command.jobs) as pool:
            summaries = list(pool.map(build_dataset_part, *arguments))
    write_manifest(command.output_dir)
    return summaries


def run_train(command: offline_cli.TrainCommand) -> DevelopmentReport:
    """Train one candidate, evaluate it, and write models_dir/name/report.json."""
    output = command.models_dir / command.name
    params = TrainParams(imbalance=command.imbalance, num_threads=command.threads)

    def scorer_for(artifact: ModelArtifact) -> LightGBMScorer:
        """Return a LightGBM batch scorer for artifact."""
        return LightGBMScorer(artifact.path, command.threads)

    use_case = DevelopModel(
        LightGBMTrainer(),
        scorer_for,
        ThresholdPolicy(command.review_max_fpr, command.block_max_fpr),
    )
    dataset = dataset_ref(command.dataset_dir, _feature_set_of(command.dataset_dir).version)
    report = use_case.execute(dataset, params, str(output))
    write_json(output / "report.json", {"params": params, "report": report})
    return report


def run_baselines(command: offline_cli.BaselinesCommand) -> dict[str, object]:
    """Evaluate the rule and logistic baselines on VALID and TEST and write the results."""
    dataset = dataset_ref(command.dataset_dir, _feature_set_of(command.dataset_dir).version)
    results: dict[str, object] = {
        "rule": EvaluateScorer(RuleBaselineScorer()).execute(dataset, EVALUATED_SPLITS),
        "logistic": EvaluateScorer(LogisticBaselineScorer(command.logistic_stride)).execute(
            dataset, EVALUATED_SPLITS
        ),
    }
    write_json(command.output, results)
    return results


def run_export(command: offline_cli.ExportCommand) -> dict[str, object]:
    """Export a candidate to ONNX, verify parity on TEST, benchmark it, and write a report.

    Raises:
        SchemaMismatchError: The model's schema differs from the dataset's feature set.
    """
    artifact, thresholds = offline_cli.read_candidate(command.candidate_dir / "report.json")
    feature_set = _feature_set_of(command.dataset_dir)
    dataset = dataset_ref(command.dataset_dir, feature_set.version)
    ref = OnnxExporter().export(artifact, thresholds, str(command.models_dir))
    model = OnnxFraudModel(ref.directory)
    if model.schema != feature_set.version:
        raise SchemaMismatchError(f"model {model.schema} != features {feature_set.version}")
    parity = VerifyParity(
        LightGBMScorer(artifact.path, command.threads), OnnxBatchScorer(ref.directory)
    ).execute(dataset, Split.TEST, thresholds, command.tolerance)
    matrix, _ = load_split(dataset, Split.TEST)
    vectors = [
        FeatureVector(feature_set.version, tuple(float(v) for v in row))
        for row in matrix[:BENCHMARK_VECTORS].tolist()
    ]
    latency = BenchmarkModel(model, SystemClock()).execute(
        vectors, command.warmup_calls, command.calls
    )
    report: dict[str, object] = {"model": ref, "parity": parity, "latency": latency}
    write_json(command.candidate_dir / "export_report.json", report)
    return report


def run_lateness_sweep(command: offline_cli.LatenessSweepCommand) -> LatenessReport:
    """Run the lateness sweep over the selection and write the report to command.output.

    Uses the model's feature set and stored thresholds when command.model_dir is given,
    otherwise FEATURE_SET_V2 without decisions. With round_robin_partitions set, the arrivals
    are reordered as round_robin_consumption would apply them.
    """
    selection = command.selection
    items = [
        item
        for item in ParquetHistoricalSource(selection.source).iter_labeled(selection.time_range)
        if selection.cards.contains(item.transaction.card)
    ]
    arrivals = list(arrival_order(items, command.disorder))
    if command.round_robin_partitions is not None:
        arrivals = round_robin_consumption(
            arrivals, command.round_robin_partitions, command.consumer_batch_size
        )
    scoring = None
    feature_set = FEATURE_SET_V2
    model_version = None
    if command.model_dir is not None:
        model = OnnxFraudModel(str(command.model_dir))
        scoring = Scoring(model, DecisionPolicy(model.thresholds))
        feature_set = FEATURE_SETS[model.schema]
        model_version = model.version
    sweep = LatenessSweep(MemoryWindowStore, feature_set, scoring)
    report = sweep.execute(items, arrivals, command.lateness_values)
    write_json(
        command.output,
        {
            "source": selection.source.as_posix(),
            "time_range": selection.time_range,
            "cards": selection.cards,
            "disorder": command.disorder,
            "round_robin_partitions": command.round_robin_partitions,
            "consumer_batch_size": command.consumer_batch_size,
            "feature_set": feature_set.version,
            "model_version": model_version,
            "report": report,
        },
    )
    return report


def _feature_set_of(dataset_dir: Path) -> FeatureSet:
    """Return the registered feature set whose version and names match the dataset's parts.

    Raises:
        SchemaMismatchError: The dataset's layout matches no registered feature set.
    """
    schema, names = feature_layout(dataset_ref(dataset_dir, ""))
    feature_set = FEATURE_SETS.get(schema)
    if feature_set is None or feature_set.names != names:
        raise SchemaMismatchError(f"dataset layout {schema} {names} is not registered")
    return feature_set


def main(argv: Sequence[str]) -> int:
    """Parse argv, run the offline command, print its result as JSON, and return 0."""
    command = offline_cli.parse(argv)
    handlers: dict[type, Callable[..., object]] = {
        offline_cli.BuildDatasetCommand: run_build_dataset,
        offline_cli.DatasetManifestCommand: lambda c: str(write_manifest(c.directory)),
        offline_cli.TrainCommand: run_train,
        offline_cli.BaselinesCommand: run_baselines,
        offline_cli.ExportCommand: run_export,
        offline_cli.LatenessSweepCommand: run_lateness_sweep,
    }
    print(render(handlers[type(command)](command)))
    return 0


def run() -> None:
    """Run main with the process arguments and exit with its status (console entry point)."""
    sys.exit(main(sys.argv[1:]))


if __name__ == "__main__":
    run()
