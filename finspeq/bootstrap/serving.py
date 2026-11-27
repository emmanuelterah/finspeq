"""Composition root of the scoring API process (finspeq-serve)."""

from __future__ import annotations

import os
import sys
from collections.abc import Sequence

import redis
import uvicorn
from confluent_kafka import Producer
from fastapi import FastAPI

from finspeq.adapters.inbound import serve_cli
from finspeq.adapters.inbound.http_api import ApiDeps, create_api
from finspeq.adapters.outbound.clock import SystemClock
from finspeq.adapters.outbound.onnx_runtime import OnnxFraudModel
from finspeq.adapters.outbound.prometheus import PrometheusMetrics
from finspeq.adapters.outbound.redis_window import RedisWindowStore
from finspeq.adapters.outbound.redpanda import ConcurrentPublisher, producer_config
from finspeq.application.score import ScoreTransaction, ScoringDeps, ShadowScoring
from finspeq.domain.errors import SchemaMismatchError
from finspeq.domain.features import FEATURE_SETS, FeatureVector
from finspeq.domain.policy import DecisionPolicy

WARMUP_PREDICTIONS = 1_000
READY_TIMEOUT_S = 2.0
REDIS_TIMEOUT_S = 2.0
MILLISECONDS_PER_SECOND = 1_000


def create_app() -> FastAPI:
    """Build the API from FINSPEQ_* environment settings; called once per uvicorn worker.

    Loads the model (and the shadow model, which must share its schema), refuses to start
    when the schema has no registered feature set, runs WARMUP_PREDICTIONS predictions, and
    starts the producer's delivery thread.

    Raises:
        SchemaMismatchError: The model's schema is not a registered feature set, or the
            shadow model's schema differs from it.
        KeyError: FINSPEQ_MODEL_DIR is not set.
    """
    settings = serve_cli.from_environment(os.environ)
    model = OnnxFraudModel(str(settings.model_dir))
    feature_set = FEATURE_SETS.get(model.schema)
    if feature_set is None:
        raise SchemaMismatchError(f"no feature set for model schema {model.schema!r}")
    shadow = None
    if settings.shadow_model_dir is not None:
        shadow_model = OnnxFraudModel(str(settings.shadow_model_dir))
        if shadow_model.schema != model.schema:
            raise SchemaMismatchError(f"shadow schema {shadow_model.schema} != {model.schema}")
        shadow = ShadowScoring(shadow_model, DecisionPolicy(shadow_model.thresholds))
    zeros = FeatureVector(feature_set.version, tuple(0.0 for _ in feature_set.names))
    for _ in range(WARMUP_PREDICTIONS):
        model.predict(zeros)
        if shadow is not None:
            shadow.model.predict(zeros)
    client = redis.Redis.from_url(
        settings.redis_url, socket_timeout=REDIS_TIMEOUT_S, socket_connect_timeout=REDIS_TIMEOUT_S
    )
    store = RedisWindowStore(client, settings.topic, settings.lateness_s)
    timeout_ms = int(settings.publish_timeout_s * MILLISECONDS_PER_SECOND)
    producer = Producer(producer_config(settings.bootstrap, settings.linger_ms, timeout_ms))
    publisher = ConcurrentPublisher(producer, settings.topic, settings.publish_timeout_s)
    publisher.start()
    metrics = PrometheusMetrics()
    deps = ScoringDeps(
        store, feature_set, model, DecisionPolicy(model.thresholds), publisher, shadow
    )
    appender = store if settings.read_your_writes else None
    scoring = ScoreTransaction(deps, metrics, SystemClock(), appender)

    def broker_ready() -> bool:
        """Return True if the broker lists the topic within READY_TIMEOUT_S."""
        topics = producer.list_topics(settings.topic, timeout=READY_TIMEOUT_S).topics
        return settings.topic in topics

    def redis_ready() -> bool:
        """Return True if Redis answers PING."""
        return bool(client.ping())

    checks = {"model": lambda: True, "redis": redis_ready, "broker": broker_ready}
    app = create_api(ApiDeps(scoring, checks, metrics.page))
    app.router.add_event_handler("shutdown", publisher.close)
    return app


def main(argv: Sequence[str]) -> int:
    """Parse argv, export the settings to the environment, and run uvicorn; return 0."""
    command = serve_cli.parse(argv)
    os.environ.update(serve_cli.to_environment(command.settings))
    uvicorn.run(
        "finspeq.bootstrap.serving:create_app",
        factory=True,
        host=command.host,
        port=command.port,
        workers=command.workers,
        access_log=False,
        log_level="warning",
    )
    return 0


def run() -> None:
    """Run main with the process arguments and exit with its status (console entry point)."""
    sys.exit(main(sys.argv[1:]))


if __name__ == "__main__":
    run()
