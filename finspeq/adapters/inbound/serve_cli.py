"""Command-line arguments and environment settings of the scoring API process."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

DEFAULT_BOOTSTRAP = "127.0.0.1:19092"
DEFAULT_TOPIC = "transactions"
DEFAULT_REDIS_URL = "redis://127.0.0.1:6379/0"
DEFAULT_LATENESS_S = 3_600
DEFAULT_LINGER_MS = 0
DEFAULT_PUBLISH_TIMEOUT_S = 5.0
ENV_PREFIX = "FINSPEQ_"
TRUE_TEXT = "1"


@dataclass(frozen=True, slots=True)
class ServeSettings:
    """What one API process connects to and how it scores.

    Attributes:
        model_dir: Exported ONNX model directory.
        redis_url: Window store.
        bootstrap: Kafka bootstrap servers.
        topic: Topic the scored events are published to.
        lateness_s: Allowed lateness of the window store (ADR-011).
        read_your_writes: Also write each scored event to Redis after the log ack.
        linger_ms: Producer linger.
        publish_timeout_s: Longest wait for one broker acknowledgement.
        shadow_model_dir: Exported model scored alongside the live one, or None.
    """

    model_dir: Path
    redis_url: str
    bootstrap: str
    topic: str
    lateness_s: int
    read_your_writes: bool
    linger_ms: int
    publish_timeout_s: float
    shadow_model_dir: Path | None = None


@dataclass(frozen=True, slots=True)
class ServeCommand:
    """Arguments of `finspeq-serve`: the listening address, process count, and settings."""

    host: str
    port: int
    workers: int
    settings: ServeSettings


def parse(argv: Sequence[str]) -> ServeCommand:
    """Return the serve command described by argv; exit with status 2 on invalid arguments."""
    parser = argparse.ArgumentParser(prog="finspeq-serve")
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--workers", type=int, default=1, help="uvicorn worker processes")
    parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--lateness-s", type=int, default=DEFAULT_LATENESS_S)
    parser.add_argument(
        "--read-your-writes", action="store_true", help="also write scored events to Redis"
    )
    parser.add_argument("--linger-ms", type=int, default=DEFAULT_LINGER_MS)
    parser.add_argument("--publish-timeout-s", type=float, default=DEFAULT_PUBLISH_TIMEOUT_S)
    parser.add_argument("--shadow-model-dir", type=Path, help="score a second model alongside")
    args = parser.parse_args(list(argv))
    settings = ServeSettings(
        model_dir=args.model_dir,
        redis_url=args.redis_url,
        bootstrap=args.bootstrap,
        topic=args.topic,
        lateness_s=args.lateness_s,
        read_your_writes=args.read_your_writes,
        linger_ms=args.linger_ms,
        publish_timeout_s=args.publish_timeout_s,
        shadow_model_dir=args.shadow_model_dir,
    )
    return ServeCommand(args.host, args.port, args.workers, settings)


def to_environment(settings: ServeSettings) -> dict[str, str]:
    """Return settings as FINSPEQ_* environment variables, for worker processes to read."""
    return {
        f"{ENV_PREFIX}MODEL_DIR": settings.model_dir.as_posix(),
        f"{ENV_PREFIX}REDIS_URL": settings.redis_url,
        f"{ENV_PREFIX}KAFKA_BOOTSTRAP": settings.bootstrap,
        f"{ENV_PREFIX}TOPIC": settings.topic,
        f"{ENV_PREFIX}ALLOWED_LATENESS_S": str(settings.lateness_s),
        f"{ENV_PREFIX}READ_YOUR_WRITES": TRUE_TEXT if settings.read_your_writes else "0",
        f"{ENV_PREFIX}LINGER_MS": str(settings.linger_ms),
        f"{ENV_PREFIX}PUBLISH_TIMEOUT_S": str(settings.publish_timeout_s),
        f"{ENV_PREFIX}SHADOW_MODEL_DIR": ""
        if settings.shadow_model_dir is None
        else settings.shadow_model_dir.as_posix(),
    }


def from_environment(environment: Mapping[str, str]) -> ServeSettings:
    """Return the settings stored by to_environment; unset values take their defaults.

    Raises:
        KeyError: FINSPEQ_MODEL_DIR is not set.
    """

    def value(name: str, default: str) -> str:
        """Return FINSPEQ_<name>, or default when it is unset."""
        return environment.get(f"{ENV_PREFIX}{name}", default)

    return ServeSettings(
        model_dir=Path(environment[f"{ENV_PREFIX}MODEL_DIR"]),
        redis_url=value("REDIS_URL", DEFAULT_REDIS_URL),
        bootstrap=value("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP),
        topic=value("TOPIC", DEFAULT_TOPIC),
        lateness_s=int(value("ALLOWED_LATENESS_S", str(DEFAULT_LATENESS_S))),
        read_your_writes=value("READ_YOUR_WRITES", "0") == TRUE_TEXT,
        linger_ms=int(value("LINGER_MS", str(DEFAULT_LINGER_MS))),
        publish_timeout_s=float(value("PUBLISH_TIMEOUT_S", str(DEFAULT_PUBLISH_TIMEOUT_S))),
        shadow_model_dir=Path(shadow) if (shadow := value("SHADOW_MODEL_DIR", "")) else None,
    )
