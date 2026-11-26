"""Composition roots: build adapters and inject them into use cases, one function per process."""

from __future__ import annotations

import signal
import sys
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING

from finspeq.adapters.inbound import cli, stream_cli
from finspeq.adapters.inbound.feature_worker import FeatureWorker, WorkerLimits, WorkerReport
from finspeq.adapters.outbound.baselines import LogisticBaselineScorer, RuleBaselineScorer
from finspeq.adapters.outbound.clock import SystemClock
from finspeq.adapters.outbound.dataset_manifest import dataset_ref, write_manifest
from finspeq.adapters.outbound.lightgbm_model import LightGBMScorer, LightGBMTrainer
from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.adapters.outbound.null_metrics import NullMetrics
from finspeq.adapters.outbound.onnx_model import OnnxBatchScorer, OnnxExporter, OnnxFraudModel
from finspeq.adapters.outbound.parquet import ParquetHistoricalSource, ParquetTrainingSink
from finspeq.adapters.outbound.training_data import feature_layout, load_split
from finspeq.application.apply_events import ApplyEvents
from finspeq.application.build_dataset import BuildPlan, BuildTrainingSet, SplitBoundaries
from finspeq.application.messages import (
    DatasetSummary,
    LabeledTransaction,
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
from finspeq.application.ports import Metrics
from finspeq.application.replay import partitioned_batches
from finspeq.application.simulate import ReplayHistory, SimulationPlan, SimulationReport
from finspeq.application.verify import (
    ReplayReport,
    ReplayScenario,
    StateReport,
    VerifyFinalState,
    VerifyReplay,
)
from finspeq.domain.errors import SchemaMismatchError
from finspeq.domain.features import FEATURE_SETS, FeatureSet, FeatureVector

if TYPE_CHECKING:
    import redis

    from finspeq.adapters.outbound.redis_window import RedisWindowStore

BENCHMARK_VECTORS = 1_000
EVALUATED_SPLITS = (Split.VALID, Split.TEST)
PUBLISH_TIMEOUT_S = 30.0
POLL_TIMEOUT_S = 0.5
VERIFY_TOPIC = "verify-replay"
STATE_BATCH_SIZE = 500


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


def run_simulate(command: stream_cli.SimulateCommand) -> SimulationReport:
    """Publish the selected history to the topic and return the achieved rate."""
    from confluent_kafka import Producer

    from finspeq.adapters.outbound.redpanda import RedpandaPublisher, producer_config

    producer = Producer(producer_config(command.bootstrap, command.linger_ms))
    publisher = RedpandaPublisher(producer, command.topic, PUBLISH_TIMEOUT_S)
    selection = command.selection
    plan = SimulationPlan(
        selection.time_range, selection.cards, command.limit, command.rate, command.chunk_size
    )
    source = ParquetHistoricalSource(selection.source)
    return ReplayHistory(source, publisher, SystemClock(), time.sleep).execute(plan)


def run_worker(command: stream_cli.WorkerCommand) -> WorkerReport:
    """Run the feature worker until Ctrl+C, SIGTERM, or the idle limit; return its totals."""
    from confluent_kafka import Consumer

    from finspeq.adapters.outbound.redpanda import RedpandaSource, consumer_config

    store = _redis_store(command.redis_url, command.topic, command.lateness_s)
    metrics: Metrics = NullMetrics()
    if command.metrics_port is not None:
        from finspeq.adapters.outbound.prometheus import PrometheusMetrics

        prometheus = PrometheusMetrics()
        prometheus.serve(command.metrics_port)
        metrics = prometheus
    consumer = Consumer(consumer_config(command.bootstrap, command.group))
    source = RedpandaSource(consumer, command.topic, store.position, POLL_TIMEOUT_S)
    source.start()
    clock = SystemClock()
    limits = WorkerLimits(command.batch_size, command.idle_exit_s, command.start_timeout_s)
    worker = FeatureWorker(source, ApplyEvents(store, metrics, clock), clock, limits)
    stop = threading.Event()
    _stop_on_signals(stop)
    try:
        return worker.run(stop)
    finally:
        source.close()


def run_verify_replay(command: stream_cli.VerifyReplayCommand) -> ReplayReport:
    """Replay the selection into Redis and into memory in process, and compare them.

    Raises:
        SystemExit: The Redis database holds keys and --flush was not given.
    """
    client = _redis_client(command.redis_url)
    if command.flush:
        client.flushdb()
    elif client.dbsize() > 0:
        raise SystemExit(f"{command.redis_url} is not empty; pass --flush to empty it")
    from finspeq.adapters.outbound.redis_window import RedisWindowStore

    candidate = RedisWindowStore(client, VERIFY_TOPIC, command.lateness_s)
    batches = partitioned_batches(
        _selected_items(command.selection), command.partitions, command.batch_size
    )
    scenario = ReplayScenario(redeliver_every=command.redeliver_every)
    return VerifyReplay(MemoryWindowStore(command.lateness_s), candidate).execute(batches, scenario)


def run_verify_state(command: stream_cli.VerifyStateCommand) -> StateReport:
    """Compare the worker-filled Redis store with an in-process replay of the selection."""
    candidate = _redis_store(command.redis_url, command.topic, command.lateness_s)
    use_case = VerifyFinalState(
        MemoryWindowStore(command.lateness_s), candidate, command.partitions
    )
    return use_case.execute(_selected_items(command.selection), STATE_BATCH_SIZE)


def _selected_items(selection: stream_cli.EventSelection) -> Iterator[LabeledTransaction]:
    """Yield the selection's historical transactions in canonical order."""
    for item in ParquetHistoricalSource(selection.source).iter_labeled(selection.time_range):
        if selection.cards.contains(item.transaction.card):
            yield item


def _redis_client(url: str) -> redis.Redis:
    """Return a Redis client for url with binary responses."""
    import redis

    return redis.Redis.from_url(url)


def _redis_store(url: str, topic: str, lateness_s: int) -> RedisWindowStore:
    """Return a RedisWindowStore over the server at url."""
    from finspeq.adapters.outbound.redis_window import RedisWindowStore

    return RedisWindowStore(_redis_client(url), topic, lateness_s)


def _stop_on_signals(stop: threading.Event) -> None:
    """Set stop on SIGINT and, where the platform has it, SIGTERM."""

    def handle(_signum: int, _frame: object) -> None:
        """Set the stop event."""
        stop.set()

    signal.signal(signal.SIGINT, handle)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle)


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
    handlers: dict[type, Callable[..., object]] = {
        cli.BuildDatasetCommand: run_build_dataset,
        cli.DatasetManifestCommand: lambda c: str(write_manifest(c.directory)),
        cli.TrainCommand: run_train,
        cli.BaselinesCommand: run_baselines,
        cli.ExportCommand: run_export,
        stream_cli.SimulateCommand: run_simulate,
        stream_cli.WorkerCommand: run_worker,
        stream_cli.VerifyReplayCommand: run_verify_replay,
        stream_cli.VerifyStateCommand: run_verify_state,
    }
    print(cli.render(handlers[type(command)](command)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
