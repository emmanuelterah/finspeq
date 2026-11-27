"""Tests for the lag monitor's summary of samples."""

from __future__ import annotations

import pytest

pytest.importorskip("confluent_kafka")
pytest.importorskip("redis")

from scripts.lag_monitor import Sample, gauge_sum, summarize


def test_summary_finds_drain_rate_and_longest_stall() -> None:
    """Measure the drain from the first progress to lag zero and the longest flat stretch."""
    samples = [
        Sample(0.0, [100, 100], [0, 0]),
        Sample(1.0, [100, 100], [20, 20]),
        Sample(2.0, [100, 100], [20, 20]),
        Sample(3.0, [100, 100], [20, 20]),
        Sample(4.0, [100, 100], [60, 60]),
        Sample(5.0, [100, 100], [100, 100]),
    ]
    summary = summarize(samples)
    assert summary.max_lag == 200
    assert (summary.first_progress_s, summary.drained_s, summary.applied) == (1.0, 5.0, 160)
    assert summary.events_per_second == 40.0
    assert summary.longest_stall_s == 2.0


def test_gauge_sum_adds_every_partition_and_ignores_other_lines() -> None:
    """Sum fq_consumer_lag over partitions; return None when the gauge is absent."""
    page = (
        "# HELP fq_consumer_lag fq_consumer_lag\n"
        'fq_consumer_lag{partition="0"} 12.0\n'
        'fq_consumer_lag{partition="1"} 30.0\n'
        'fq_events_applied_total{partition="0"} 5.0\n'
    )
    assert gauge_sum(page) == 42
    assert gauge_sum("fq_other 1.0\n") is None
