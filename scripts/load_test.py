"""Open-model load generator for the scoring API, with a staleness check per request.

Two plans:
- rate: transactions in canonical order, one every 1/rate seconds.
- bursts: each card sends its next --burst-size transactions --gap-ms apart; cards start
  --stagger-ms apart.

Latency is measured from each request's scheduled time, so a slow server cannot slow the
generator down (no coordinated omission). For every 200 response, missing = the number of the
card's earlier events in the reference window (warm-up plus earlier sent transactions, in
canonical order) minus the window entries the API reports.
"""

from __future__ import annotations

import argparse
import asyncio
import calendar
import json
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import aiohttp

from finspeq.adapters.inbound.selection import parse_slice
from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.adapters.outbound.parquet import ParquetHistoricalSource
from finspeq.application.messages import Event, EventBatch, EventPosition, TimeRange
from finspeq.application.replay import CardSlice
from finspeq.domain.model import EventTime, Transaction

MILLISECONDS = 1_000.0
HTTP_OK = 200
REQUEST_TIMEOUT_S = 30.0
PERCENTILES = (0.50, 0.95, 0.99)


@dataclass(frozen=True, slots=True)
class Planned:
    """One request: when to send it (seconds after start), the transaction, and the gap group."""

    at_s: float
    tx: Transaction
    expected_entries: int


@dataclass(frozen=True, slots=True)
class Result:
    """Outcome of one request."""

    at_s: float
    status: int
    latency_ms: float
    send_lag_ms: float
    missing: int | None
    steps: dict[str, float]
    error: str | None


def day(text: str) -> EventTime:
    """Return 00:00 UTC of a YYYY-MM-DD day."""
    return EventTime(calendar.timegm(date.fromisoformat(text).timetuple()))


def load(source: Path, start: str, end: str, cards: CardSlice) -> list[Transaction]:
    """Return the slice's transactions in [start, end) in canonical order."""
    items = ParquetHistoricalSource(source).iter_labeled(TimeRange(day(start), day(end)))
    return [item.transaction for item in items if cards.contains(item.transaction.card)]


def expected_entries(warmup: list[Transaction], sent: list[Transaction]) -> list[int]:
    """Return, per sent transaction, its window size after warm-up and earlier sent ones."""
    store = MemoryWindowStore()
    offset = 0
    for tx in warmup:
        store.apply(EventBatch(0, (Event(EventPosition(0, offset), tx),)))
        offset += 1
    counts = {}
    for tx in sorted(sent, key=lambda t: t.order_key):
        counts[tx.tx_id] = len(store.read(tx.card, tx.order_key).entries)
        store.apply(EventBatch(0, (Event(EventPosition(0, offset), tx),)))
        offset += 1
    return [counts[tx.tx_id] for tx in sent]


def rate_plan(
    txs: list[Transaction], rate: float, limit: int
) -> tuple[list[Transaction], list[float]]:
    """Return the first limit transactions and their send times, 1/rate seconds apart."""
    chosen = txs[:limit]
    return chosen, [index / rate for index in range(len(chosen))]


def burst_times(
    bursts: list[list[Transaction]], gap_ms: float, stagger_ms: float
) -> tuple[list[Transaction], list[float]]:
    """Return every burst's transactions and send times in seconds after the start.

    Card i starts i * stagger_ms in; each card's next transaction follows gap_ms later.
    """
    chosen: list[Transaction] = []
    times: list[float] = []
    for index, burst in enumerate(bursts):
        for position, tx in enumerate(burst):
            chosen.append(tx)
            times.append((index * stagger_ms + position * gap_ms) / MILLISECONDS)
    return chosen, times


def burst_plan(txs: list[Transaction], size: int, limit: int) -> list[list[Transaction]]:
    """Return up to limit cards' first size transactions, one list per card, in card order."""
    by_card: dict[str, list[Transaction]] = defaultdict(list)
    for tx in txs:
        if len(by_card[tx.card.value]) < size:
            by_card[tx.card.value].append(tx)
    full = [by_card[card] for card in sorted(by_card) if len(by_card[card]) == size]
    return full[:limit]


def body(tx: Transaction) -> dict[str, object]:
    """Return the POST /score body for tx."""
    return {
        "tx_id": tx.tx_id.value,
        "card": tx.card.value,
        "time": tx.time.epoch_s,
        "amount_cents": tx.amount.cents,
        "merchant": tx.merchant,
        "category": tx.category.value,
        "merchant_point": [tx.merchant_point.lat, tx.merchant_point.lon],
        "home_point": [tx.home_point.lat, tx.home_point.lon],
    }


def steps_of(header: str) -> dict[str, float]:
    """Return the step durations of a Server-Timing header ("read;dur=0.8, ...")."""
    steps = {}
    for part in header.split(","):
        name, _, duration = part.strip().partition(";dur=")
        if name and duration:
            steps[name] = float(duration)
    return steps


async def send(client: aiohttp.ClientSession, planned: Planned, start: float) -> Result:
    """Wait until planned.at_s after start, post the transaction, and return the result."""
    scheduled = start + planned.at_s
    delay = scheduled - time.perf_counter()
    if delay > 0:
        await asyncio.sleep(delay)
    sent = time.perf_counter()
    try:
        async with client.post("/score", json=body(planned.tx)) as response:
            payload = await response.json()
            status = response.status
            timing = response.headers.get("server-timing", "")
    except (aiohttp.ClientError, TimeoutError) as exc:
        done = time.perf_counter()
        latency = (done - scheduled) * MILLISECONDS
        lag = (sent - scheduled) * MILLISECONDS
        return Result(planned.at_s, 0, latency, lag, None, {}, type(exc).__name__)
    done = time.perf_counter()
    missing = None
    error = None
    if status == HTTP_OK:
        missing = planned.expected_entries - int(payload["window_entries"])
    else:
        error = str(payload.get("error", status))
    return Result(
        planned.at_s,
        status,
        (done - scheduled) * MILLISECONDS,
        (sent - scheduled) * MILLISECONDS,
        missing,
        steps_of(timing),
        error,
    )


def ignore_resets(loop: asyncio.AbstractEventLoop, context: dict[str, object]) -> None:
    """Drop connection-reset reports from closing sockets; pass anything else on.

    On Windows the proactor event loop logs a traceback for every socket the server closes
    first; those resets do not affect any request.
    """
    if not isinstance(context.get("exception"), ConnectionResetError):
        loop.default_exception_handler(context)


async def run(
    url: str, plan: list[Planned], connections: int, ready_file: Path | None
) -> tuple[list[Result], float]:
    """Send every planned request on schedule and return the results and the elapsed time.

    When ready_file is given, it is created just before the first request is due, so another
    process can time a fault relative to the start of the load.
    """
    asyncio.get_running_loop().set_exception_handler(ignore_resets)
    connector = aiohttp.TCPConnector(limit=connections)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_S)
    async with aiohttp.ClientSession(url, connector=connector, timeout=timeout) as client:
        start = time.perf_counter() + 0.5
        if ready_file is not None:
            ready_file.write_text("started\n", encoding="utf-8")
        tasks = [asyncio.create_task(send(client, item, start)) for item in plan]
        results = await asyncio.gather(*tasks)
        return list(results), time.perf_counter() - start


def percentiles(values: list[float]) -> dict[str, float]:
    """Return p50, p95, p99, and max of values (nearest rank), rounded to 0.01."""
    if not values:
        return {}
    ordered = sorted(values)
    out = {
        f"p{int(q * 100)}": ordered[max(0, int(q * len(ordered) + 0.5) - 1)] for q in PERCENTILES
    }
    out["max"] = ordered[-1]
    return {key: round(value, 2) for key, value in out.items()}


def summarize(results: list[Result], elapsed_s: float) -> dict[str, object]:
    """Return status counts, latency and step percentiles, staleness, and a per-second log."""
    statuses: dict[str, int] = defaultdict(int)
    errors: dict[str, int] = defaultdict(int)
    for result in results:
        statuses[str(result.status)] += 1
        if result.error is not None:
            errors[result.error] += 1
    ok = [r for r in results if r.status == HTTP_OK]
    steps: dict[str, list[float]] = defaultdict(list)
    for result in ok:
        for name, ms in result.steps.items():
            steps[name].append(ms)
    missing = [r.missing for r in ok if r.missing is not None]
    seconds: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for result in results:
        seconds[int(result.at_s)]["ok" if result.status == HTTP_OK else "failed"] += 1
    return {
        "requests": len(results),
        "elapsed_s": round(elapsed_s, 2),
        "achieved_rps": round(len(results) / elapsed_s, 1) if elapsed_s > 0 else 0.0,
        "statuses": dict(statuses),
        "errors": dict(errors),
        "error_rate": round(1 - len(ok) / len(results), 5) if results else 0.0,
        "latency_ms": percentiles([r.latency_ms for r in ok]),
        "send_lag_ms": percentiles([r.send_lag_ms for r in results]),
        "steps_ms": {name: percentiles(values) for name, values in sorted(steps.items())},
        "missing": {
            "checked": len(missing),
            "stale": sum(1 for m in missing if m > 0),
            "stale_fraction": round(sum(1 for m in missing if m > 0) / len(missing), 5)
            if missing
            else 0.0,
            "max": max(missing, default=0),
            "negative": sum(1 for m in missing if m < 0),
        },
        "per_second": {str(s): dict(counts) for s, counts in sorted(seconds.items())},
    }


def main() -> None:
    """Build the plan, run it against the API, and write the summary as JSON."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--source", type=Path, default=Path("data/processed/events_sorted.parquet"))
    parser.add_argument("--from-date", required=True, help="first day sent, YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, help="day after the last one read")
    parser.add_argument("--warmup-from", help="first warm-up day already in Redis")
    parser.add_argument("--slice", default="all")
    parser.add_argument("--plan", choices=["rate", "bursts"], default="rate")
    parser.add_argument("--rate", type=float, default=100.0)
    parser.add_argument("--limit", type=int, default=3_000, help="requests, or cards for bursts")
    parser.add_argument("--burst-size", type=int, default=10)
    parser.add_argument("--gap-ms", type=float, default=100.0)
    parser.add_argument("--stagger-ms", type=float, default=20.0)
    parser.add_argument("--connections", type=int, default=200)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ready-file", type=Path, help="created when sending starts")
    args = parser.parse_args()
    cards = parse_slice(args.slice)
    txs = load(args.source, args.from_date, args.to_date, cards)
    warmup = (
        []
        if args.warmup_from is None
        else load(args.source, args.warmup_from, args.from_date, cards)
    )
    if args.plan == "rate":
        chosen, times = rate_plan(txs, args.rate, args.limit)
    else:
        bursts = burst_plan(txs, args.burst_size, args.limit)
        chosen, times = burst_times(bursts, args.gap_ms, args.stagger_ms)
    expected = expected_entries(warmup, chosen)
    plan = sorted(
        (Planned(at, tx, count) for at, tx, count in zip(times, chosen, expected, strict=True)),
        key=lambda planned: planned.at_s,
    )
    results, elapsed = asyncio.run(run(args.url, plan, args.connections, args.ready_file))
    report = {
        "arguments": {k: str(v) for k, v in vars(args).items()},
        **summarize(results, elapsed),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n")
    printed = {k: v for k, v in report.items() if k not in ("per_second", "arguments")}
    print(json.dumps(printed, indent=2))


if __name__ == "__main__":
    main()
