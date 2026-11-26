"""Run a crashing worker command until it exits cleanly, recording recovery state per run.

Everything after `--` is the command. Each run gets `--crash-seed <seed + run>` appended. After
every exit, the drill records each partition's stored position (Redis) and committed group
offset (Kafka). Optionally kills Redis with SIGKILL once, after a delay, and restarts it.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

import redis
from confluent_kafka import Consumer, TopicPartition

from finspeq.adapters.outbound.redis_window import RedisWindowStore

CLEAN_EXIT = 0
CRASH_EXIT = 137
RESTART_DELAY_S = 1.0
REDIS_WAIT_S = 60.0
COMMITTED_TIMEOUT_S = 10.0


@dataclass(slots=True)
class RunRecord:
    """Outcome of one run: exit code, duration, and per-partition recovery state after it."""

    run: int
    exit_code: int
    seconds: float
    stored_offsets: list[int | None]
    committed_offsets: list[int | None]


@dataclass(slots=True)
class RedisKill:
    """When Redis was killed and the stored offsets read just before the kill."""

    after_s: float
    stored_before: list[int | None] = field(default_factory=list)
    stored_after_restart: list[int | None] = field(default_factory=list)


def resolve_program(command: list[str]) -> list[str]:
    """Return command with its program replaced by the absolute path found on PATH.

    On Windows a venv's python.exe starts the base interpreter, and child processes then find
    programs in the base interpreter's directory before PATH; an absolute path avoids that.
    """
    if not command:
        return command
    program = shutil.which(command[0])
    return [program or command[0], *command[1:]]


def stored_offsets(client: redis.Redis, topic: str, partitions: int) -> list[int | None]:
    """Return the last applied offset of each partition in the Redis store, or None."""
    store = RedisWindowStore(client, topic)
    positions = [store.position(partition) for partition in range(partitions)]
    return [None if p is None else p.offset for p in positions]


def committed_offsets(bootstrap: str, group: str, topic: str, partitions: int) -> list[int | None]:
    """Return the committed group offset of each partition (the next offset to read), or None."""
    consumer = Consumer(
        {"bootstrap.servers": bootstrap, "group.id": group, "broker.address.family": "v4"}
    )
    try:
        wanted = [TopicPartition(topic, partition) for partition in range(partitions)]
        result = consumer.committed(wanted, timeout=COMMITTED_TIMEOUT_S)
    finally:
        consumer.close()
    return [tp.offset if tp.offset >= 0 else None for tp in result]


def wait_for_redis(client: redis.Redis) -> None:
    """Block until Redis answers PING, for up to REDIS_WAIT_S seconds."""
    deadline = time.monotonic() + REDIS_WAIT_S
    while time.monotonic() < deadline:
        try:
            if client.ping():
                return
        except redis.RedisError:
            time.sleep(0.5)
    raise SystemExit("Redis did not come back")


def kill_redis_later(kill: RedisKill, client: redis.Redis, args: argparse.Namespace) -> None:
    """After kill.after_s seconds, SIGKILL the compose Redis service, then start it again."""
    time.sleep(kill.after_s)
    kill.stored_before = stored_offsets(client, args.topic, args.partitions)
    subprocess.run(["docker", "compose", "kill", "-s", "SIGKILL", "redis"], check=True)
    subprocess.run(["docker", "compose", "start", "redis"], check=True)
    wait_for_redis(client)
    kill.stored_after_restart = stored_offsets(client, args.topic, args.partitions)


def main() -> int:
    """Run the drill and write its JSON report; return 0 if the command finally exited cleanly."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--bootstrap", default="127.0.0.1:19092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--group", default="finspeq-features")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--seed", type=int, default=1_000)
    parser.add_argument("--max-runs", type=int, default=1_000)
    parser.add_argument("--kill-redis-after-s", type=float, help="SIGKILL Redis once, then restart")
    parser.add_argument(
        "--max-failures", type=int, default=3, help="stop after this many other exits in a row"
    )
    parser.add_argument("command", nargs=argparse.REMAINDER, help="-- command to run")
    args = parser.parse_args()
    command = resolve_program([part for part in args.command if part != "--"])
    if not command:
        parser.error("give the command after --")
    client = redis.Redis.from_url(args.redis_url)
    kill = None if args.kill_redis_after_s is None else RedisKill(args.kill_redis_after_s)
    if kill is not None:
        threading.Thread(target=kill_redis_later, args=(kill, client, args), daemon=True).start()
    runs: list[RunRecord] = []
    started = time.monotonic()
    failures_in_a_row = 0
    for run in range(args.max_runs):
        run_start = time.monotonic()
        exit_code = subprocess.run([*command, "--crash-seed", str(args.seed + run)]).returncode
        wait_for_redis(client)
        runs.append(
            RunRecord(
                run=run,
                exit_code=exit_code,
                seconds=round(time.monotonic() - run_start, 3),
                stored_offsets=stored_offsets(client, args.topic, args.partitions),
                committed_offsets=committed_offsets(
                    args.bootstrap, args.group, args.topic, args.partitions
                ),
            )
        )
        if exit_code == CLEAN_EXIT:
            break
        failures_in_a_row = 0 if exit_code == CRASH_EXIT else failures_in_a_row + 1
        if failures_in_a_row >= args.max_failures:
            print(f"stopping: {failures_in_a_row} runs in a row exited with {exit_code}")
            break
        time.sleep(RESTART_DELAY_S)
    report = summary(runs, time.monotonic() - started, kill)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps({k: v for k, v in report.items() if k != "runs"}, indent=2))
    return 0 if runs and runs[-1].exit_code == CLEAN_EXIT else 1


def summary(runs: list[RunRecord], seconds: float, kill: RedisKill | None) -> dict[str, object]:
    """Return the report: exit-code counts, offset divergence after crashes, and every run.

    Divergence after a run is stored offset + 1 - committed offset: events the store holds that
    the group offset does not cover. Reported only where both values exist.
    """
    exit_codes: dict[str, int] = {}
    divergence: list[int] = []
    for record in runs:
        exit_codes[str(record.exit_code)] = exit_codes.get(str(record.exit_code), 0) + 1
        for stored, committed in zip(record.stored_offsets, record.committed_offsets, strict=True):
            if stored is not None and committed is not None:
                divergence.append(stored + 1 - committed)
    return {
        "runs_total": len(runs),
        "exit_codes": exit_codes,
        "seconds": round(seconds, 1),
        "max_divergence": max(divergence, default=0),
        "runs_with_divergence": sum(1 for d in divergence if d != 0),
        "redis_kill": None if kill is None else asdict(kill),
        "runs": [asdict(record) for record in runs],
    }


if __name__ == "__main__":
    sys.exit(main())
