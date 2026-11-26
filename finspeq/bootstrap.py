"""Composition roots: build adapters and inject them into use cases, one function per process."""

from __future__ import annotations

import sys
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from finspeq.adapters.inbound import cli
from finspeq.adapters.outbound.baselines import LogisticBaselineScorer, RuleBaselineScorer
from finspeq.adapters.outbound.clock import SystemClock
from finspeq.adapters.outbound.dataset_manifest import dataset_ref, write_manifest
from finspeq.adapters.outbound.lightgbm_model import LightGBMScorer, LightGBMTrainer
from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.adapters.outbound.onnx_model import OnnxBatchScorer, OnnxExporter, OnnxFraudModel
from finspeq.adapters.outbound.parquet import ParquetHistoricalSource, ParquetTrainingSink
from finspeq.adapters.outbound.training_data import feature_layout, load_split
from finspeq.application.build_dataset import BuildPlan, BuildTrainingSet, SplitBoundaries
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
from finspeq.domain.features import FEATURE_SETS, FeatureSet, FeatureVector

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


def run_build_dataset(command: cli.BuildDatasetCommand) -> list[DatasetSummary]:
    """Build every month of command as a part file, then rewrite the dataset manifest.

    Months run in up to command.jobs worker processes. Results are returned in month order.
    """
    boundaries = SplitBoundaries(command.valid_start, command.test_start)
    plan = BuildPlan(feature_set=FEATURE_SETS[command.feature_set], boundaries=boundaries)
    outputs = [command.output_dir / cli.part_name(month) for month in command.months]
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


def run_train(command: cli.TrainCommand) -> DevelopmentReport:
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
    cli.write_json(output / "report.json", {"params": params, "report": report})
    return report


def run_baselines(command: cli.BaselinesCommand) -> dict[str, object]:
    """Evaluate the rule and logistic baselines on VALID and TEST and write the results."""
    dataset = dataset_ref(command.dataset_dir, _feature_set_of(command.dataset_dir).version)
    results: dict[str, object] = {
        "rule": EvaluateScorer(RuleBaselineScorer()).execute(dataset, EVALUATED_SPLITS),
        "logistic": EvaluateScorer(LogisticBaselineScorer(command.logistic_stride)).execute(
            dataset, EVALUATED_SPLITS
        ),
    }
    cli.write_json(command.output, results)
    return results


def run_export(command: cli.ExportCommand) -> dict[str, object]:
    """Export a candidate to ONNX, verify parity on TEST, benchmark it, and write a report.

    Raises:
        SchemaMismatchError: The model's schema differs from the dataset's feature set.
    """
    artifact, thresholds = cli.read_candidate(command.candidate_dir / "report.json")
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
    cli.write_json(command.candidate_dir / "export_report.json", report)
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
    """Parse argv, run the command, print its result as JSON, and return the exit status."""
    command = cli.parse(argv)
    result: object
    if isinstance(command, cli.BuildDatasetCommand):
        result = run_build_dataset(command)
    elif isinstance(command, cli.DatasetManifestCommand):
        result = str(write_manifest(command.directory))
    elif isinstance(command, cli.TrainCommand):
        result = run_train(command)
    elif isinstance(command, cli.BaselinesCommand):
        result = run_baselines(command)
    else:
        result = run_export(command)
    print(cli.render(result))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
