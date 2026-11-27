"""Tests for the load generator's plans, reference window sizes, and summary."""

from __future__ import annotations

import pytest

pytest.importorskip("aiohttp")

from scripts.load_test import (
    Result,
    burst_plan,
    burst_times,
    expected_entries,
    rate_plan,
    steps_of,
    summarize,
)
from tests.factories import BASE_TIME, OTHER_CARD, make_tx


def test_plans_space_requests_by_rate_and_by_gap() -> None:
    """Space rate requests 1/rate apart; start cards stagger apart and send gap apart."""
    txs = [
        make_tx(n, BASE_TIME + n, card=OTHER_CARD if n % 2 else "4000000000000002")
        for n in range(1, 8)
    ]
    chosen, times = rate_plan(txs, rate=4.0, limit=3)
    assert (len(chosen), times) == (3, [0.0, 0.25, 0.5])
    bursts = burst_plan(txs, size=3, limit=5)
    assert [len(b) for b in bursts] == [3, 3]
    _, burst_at = burst_times(bursts, gap_ms=100.0, stagger_ms=10.0)
    assert burst_at == [0.0, 0.1, 0.2, 0.01, 0.11, 0.21]


def test_expected_entries_count_warmup_and_earlier_sent_transactions() -> None:
    """Count the card's warm-up and earlier sent transactions within 24 h."""
    warmup = [make_tx(1, BASE_TIME - 600)]
    sent = [
        make_tx(3, BASE_TIME + 60),
        make_tx(2, BASE_TIME),
        make_tx(4, BASE_TIME, card=OTHER_CARD),
    ]
    assert expected_entries(warmup, sent) == [2, 1, 0]


def test_summary_reports_staleness_and_steps() -> None:
    """Count stale responses, failures, and step percentiles."""
    results = [
        Result(0.1, 200, 10.0, 0.1, 0, {"read": 1.0, "publish": 4.0}, None),
        Result(0.2, 200, 20.0, 0.1, 2, {"read": 2.0, "publish": 6.0}, None),
        Result(1.5, 503, 5000.0, 0.1, None, {}, "publish_failed"),
    ]
    summary = summarize(results, elapsed_s=2.0)
    assert summary["statuses"] == {"200": 2, "503": 1}
    assert summary["errors"] == {"publish_failed": 1}
    assert summary["missing"] == {
        "checked": 2,
        "stale": 1,
        "stale_fraction": 0.5,
        "max": 2,
        "negative": 0,
    }
    assert summary["per_second"] == {"0": {"ok": 2}, "1": {"failed": 1}}
    assert steps_of("read;dur=0.812, publish;dur=3.5") == {"read": 0.812, "publish": 3.5}
