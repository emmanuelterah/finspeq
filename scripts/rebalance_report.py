"""Measure partition handoff pauses from the rebalance events of several worker reports.

Each input is the JSON a `finspeq-stream worker` run prints. A pause starts when a consumer
revokes or loses a partition and ends when any consumer is next assigned that partition.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Change:
    """One rebalance event of one consumer for one partition."""

    at: float
    kind: str
    partition: int


@dataclass(frozen=True, slots=True)
class PauseReport:
    """Handoff pauses in seconds: every pause, and their count, median, and maximum."""

    pauses: list[float]
    count: int
    median_s: float
    max_s: float
    poll_timeouts: int


def changes_of(report: dict[str, object]) -> tuple[list[Change], int]:
    """Return the per-partition changes in a worker report and its number of poll timeouts."""
    changes: list[Change] = []
    timeouts = 0
    consumers = report["consumers"]
    assert isinstance(consumers, list)
    for consumer in consumers:
        for event in consumer["rebalances"]:
            if event["kind"] == "poll-timeout":
                timeouts += 1
            for partition in event["partitions"]:
                changes.append(Change(float(event["at"]), str(event["kind"]), int(partition)))
    return changes, timeouts


def pauses_of(changes: list[Change]) -> list[float]:
    """Return, per revoke or loss, the seconds until the partition is next assigned."""
    ordered = sorted(changes, key=lambda change: change.at)
    pauses = []
    for index, change in enumerate(ordered):
        if change.kind == "assign":
            continue
        following = (
            later.at
            for later in ordered[index + 1 :]
            if later.kind == "assign" and later.partition == change.partition
        )
        assigned = next(following, None)
        if assigned is not None:
            pauses.append(round(assigned - change.at, 3))
    return pauses


def summarize(pauses: list[float], poll_timeouts: int) -> PauseReport:
    """Return the count, median, and maximum of pauses."""
    ordered = sorted(pauses)
    median = ordered[len(ordered) // 2] if ordered else 0.0
    return PauseReport(pauses, len(pauses), median, max(ordered, default=0.0), poll_timeouts)


def main() -> None:
    """Read worker reports, compute the handoff pauses, and write them as JSON."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("reports", type=Path, nargs="+")
    args = parser.parse_args()
    changes: list[Change] = []
    timeouts = 0
    for path in args.reports:
        found, count = changes_of(json.loads(path.read_text(encoding="utf-8")))
        changes += found
        timeouts += count
    report = summarize(pauses_of(changes), timeouts)
    text = json.dumps(asdict(report), indent=2) + "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(text, encoding="utf-8", newline="\n")
    print(text, end="")


if __name__ == "__main__":
    main()
