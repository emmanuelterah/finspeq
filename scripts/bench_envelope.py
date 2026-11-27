"""Envelope encode/decode speed and size: stdlib JSON (the codec) vs msgspec JSON and MessagePack.

Builds scored envelopes for real transactions, then times, per message: the codec's encode and
decode (stdlib json plus domain validation), and msgspec's typed JSON and MessagePack round
trips of the same envelope. Each figure is the best of --repeats runs.
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable
from pathlib import Path

import msgspec

from finspeq.adapters.codec import decode, encode
from finspeq.adapters.outbound.parquet import ParquetHistoricalSource
from finspeq.application.messages import EventPosition, TimeRange
from finspeq.domain.features import FeatureVector
from finspeq.domain.model import EventTime, Transaction
from finspeq.domain.policy import Decision, Score
from finspeq.domain.scored import DecisionRecord

MICROSECONDS = 1_000_000.0
POSITION = EventPosition(0, 0)


class TransactionFields(msgspec.Struct):
    """The transaction object of the envelope, as msgspec decodes it."""

    tx_id: str
    card: str
    time: int
    amount_cents: int
    merchant: str
    category: str
    merchant_point: tuple[float, float]
    home_point: tuple[float, float]


class ScoredFields(msgspec.Struct):
    """The scored object of the envelope, as msgspec decodes it."""

    feature_schema: str
    features: list[float]
    fraud_probability: float
    decision: str
    model_version: str


class Envelope(msgspec.Struct):
    """The whole envelope, as msgspec decodes it."""

    schema_version: int
    key: str
    transaction: TransactionFields
    scored: ScoredFields | None


def transactions(source: Path, limit: int) -> list[Transaction]:
    """Return the first limit transactions of March 2026."""
    span = TimeRange(EventTime(1_772_323_200), EventTime(1_775_001_600))
    items = []
    for item in ParquetHistoricalSource(source).iter_labeled(span):
        items.append(item.transaction)
        if len(items) == limit:
            break
    return items


def best_us(action: Callable[[], object], count: int, repeats: int) -> float:
    """Return the fastest of repeats runs of action, in microseconds per message."""
    timings = []
    for _ in range(repeats):
        started = time.perf_counter()
        action()
        timings.append(time.perf_counter() - started)
    return min(timings) / count * MICROSECONDS


def main() -> None:
    """Run the benchmark and write the results as JSON."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", type=Path, default=Path("data/processed/events_sorted.parquet"))
    parser.add_argument("--messages", type=int, default=20_000)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    txs = transactions(args.source, args.messages)
    record = DecisionRecord(
        FeatureVector("v2", tuple(float(n) + 0.125 for n in range(11))),
        Score(0.0412),
        Decision.REVIEW,
        "2e632818a205",
    )
    encoded = [encode(tx, record) for tx in txs]
    values = [value for _, value in encoded]
    envelopes = [msgspec.json.decode(value, type=Envelope) for value in values]
    packed = [msgspec.msgpack.encode(envelope) for envelope in envelopes]
    count = len(txs)
    json_decoder = msgspec.json.Decoder(Envelope)
    pack_decoder = msgspec.msgpack.Decoder(Envelope)
    results = {
        "messages": count,
        "bytes_per_message": {
            "json": sum(map(len, values)) / count,
            "msgpack": sum(map(len, packed)) / count,
        },
        "us_per_message": {
            "codec_encode": best_us(
                lambda: [encode(tx, record) for tx in txs], count, args.repeats
            ),
            "codec_decode": best_us(
                lambda: [decode(k, v, POSITION) for k, v in encoded], count, args.repeats
            ),
            "stdlib_json_loads": best_us(
                lambda: [json.loads(v) for v in values], count, args.repeats
            ),
            "msgspec_json_encode": best_us(
                lambda: [msgspec.json.encode(e) for e in envelopes], count, args.repeats
            ),
            "msgspec_json_decode": best_us(
                lambda: [json_decoder.decode(v) for v in values], count, args.repeats
            ),
            "msgspec_msgpack_encode": best_us(
                lambda: [msgspec.msgpack.encode(e) for e in envelopes], count, args.repeats
            ),
            "msgspec_msgpack_decode": best_us(
                lambda: [pack_decoder.decode(p) for p in packed], count, args.repeats
            ),
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(results, indent=2) + "\n"
    args.output.write_text(text, encoding="utf-8", newline="\n")
    print(text, end="")


if __name__ == "__main__":
    main()
