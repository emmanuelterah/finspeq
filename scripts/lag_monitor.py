"""Sample consumer lag once per interval: broker high watermark minus the Redis stored position.

Lag is measured against the position stored with the state (ADR-003), not the group offset.
Stops after --duration-s, or once lag has been zero for --settle-s after having been positive.
Writes the samples and a summary as JSON. With --metrics-url, each sample also records the
sum of the worker's fq_consumer_lag gauge read from that Prometheus page at the same moment.
"""

from __future__ import annotations

import argparse
import itertools
import json
import time
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path

import redis
from confluent_kafka import Consumer, TopicPartition

from finspeq.adapters.outbound.redis_window import RedisWindowStore

WATERMARK_TIMEOUT_S = 5.0
METRICS_TIMEOUT_S = 2.0
GAUGE_PREFIX = "fq_consumer_lag{"


@dataclass(frozen=True, slots=True)
class Sample:
    """One measurement: seconds since start, per-partition end offsets and applied counts.

    gauge is the sum of fq_consumer_lag over partitions at the same moment, or None when no
    metrics page was read or the gauge was not there yet.
    """

    t: float
    end_offsets: list[int]
    applied: list[int]
    gauge: int | None = None

    @property
    def lag(self) -> int:
        """Return the total number of events in the log not yet applied."""
        return sum(end - done for end, done in zip(self.end_offsets, self.applied, strict=True))


@dataclass(frozen=True, slots=True)
class Summary:
    """Peak lag and the drain: from the first applied event to lag zero.

    Attributes:
        max_lag: Largest total lag seen.
        first_progress_s: Time of the first sample with applied events, or None.
        drained_s: Time of the first sample with lag zero after progress, or None.
        applied: Events applied between those two samples.
        events_per_second: applied / (drained_s - first_progress_s), or 0.0.
        longest_stall_s: Longest run of samples with positive lag and no new applied events.
    """

    max_lag: int
    first_progress_s: float | None
    drained_s: float | None
    applied: int
    events_per_second: float
    longest_stall_s: float


def end_offsets(consumer: Consumer, topic: str, partitions: int) -> list[int]:
    """Return the high watermark (next offset to be written) of each partition."""
    return [
        consumer.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=WATERMARK_TIMEOUT_S
        )[1]
        for partition in range(partitions)
    ]


def applied_counts(store: RedisWindowStore, partitions: int) -> list[int]:
    """Return stored offset + 1 per partition, or 0 where nothing is stored."""
    positions = [store.position(partition) for partition in range(partitions)]
    return [0 if p is None else p.offset + 1 for p in positions]


def gauge_sum(page: str) -> int | None:
    """Return the sum of every fq_consumer_lag sample in a Prometheus text page, or None."""
    values = [
        float(line.rsplit(" ", 1)[1]) for line in page.splitlines() if line.startswith(GAUGE_PREFIX)
    ]
    return round(sum(values)) if values else None


def read_gauge(url: str) -> int | None:
    """Return gauge_sum of the page at url, or None if the page cannot be read."""
    try:
        with urllib.request.urlopen(url, timeout=METRICS_TIMEOUT_S) as response:
            return gauge_sum(response.read().decode("utf-8"))
    except OSError:
        return None


def summarize(samples: list[Sample]) -> Summary:
    """Return the peak lag, the drain window and rate, and the longest stall of samples."""
    max_lag = max((s.lag for s in samples), default=0)
    first = next((s for s in samples if sum(s.applied) > sum(samples[0].applied)), None)
    drained = None
    if first is not None:
        drained = next((s for s in samples if s.t >= first.t and s.lag == 0), None)
    applied = 0 if first is None or drained is None else sum(drained.applied) - sum(first.applied)
    span = 0.0 if first is None or drained is None else drained.t - first.t
    stall = longest = 0.0
    for previous, current in itertools.pairwise(samples):
        if current.lag > 0 and sum(current.applied) == sum(previous.applied):
            stall += current.t - previous.t
            longest = max(longest, stall)
        else:
            stall = 0.0
    return Summary(
        max_lag=max_lag,
        first_progress_s=None if first is None else first.t,
        drained_s=None if drained is None else drained.t,
        applied=applied,
        events_per_second=applied / span if span > 0 else 0.0,
        longest_stall_s=longest,
    )


def main() -> None:
    """Parse arguments, sample until a stop condition, and write the JSON report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--bootstrap", default="127.0.0.1:19092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--interval-s", type=float, default=1.0)
    parser.add_argument("--duration-s", type=float, default=600.0)
    parser.add_argument("--settle-s", type=float, default=5.0)
    parser.add_argument("--metrics-url", help="worker metrics page, e.g. http://127.0.0.1:9101/")
    args = parser.parse_args()
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": "finspeq-lag-monitor",
            "broker.address.family": "v4",
        }
    )
    store = RedisWindowStore(redis.Redis.from_url(args.redis_url), args.topic)
    samples: list[Sample] = []
    start = time.monotonic()
    zero_since: float | None = None
    try:
        while True:
            now = time.monotonic() - start
            sample = Sample(
                round(now, 3),
                end_offsets(consumer, args.topic, args.partitions),
                applied_counts(store, args.partitions),
                read_gauge(args.metrics_url) if args.metrics_url else None,
            )
            samples.append(sample)
            progressed = sum(sample.applied) > sum(samples[0].applied)
            zero_since = (zero_since or now) if progressed and sample.lag == 0 else None
            if now >= args.duration_s or (
                zero_since is not None and now - zero_since >= args.settle_s
            ):
                break
            time.sleep(args.interval_s)
    finally:
        consumer.close()
    report = {"summary": asdict(summarize(samples)), "samples": [asdict(s) for s in samples]}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
