"""Tests for BuildTrainingSet and SplitBoundaries."""

from __future__ import annotations

import pytest

from finspeq.adapters.outbound.memory_history import MemoryHistoricalSource, MemoryTrainingSink
from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.application.build_dataset import BuildPlan, BuildTrainingSet, SplitBoundaries
from finspeq.application.errors import InvalidMessageError, PositionGapError
from finspeq.application.messages import (
    DatasetSummary,
    LabeledTransaction,
    Split,
    TimeRange,
    TrainingRow,
)
from finspeq.domain.features import FEATURE_SET_V1
from finspeq.domain.model import EventTime
from tests.factories import BASE_TIME, OTHER_CARD, make_tx

HOUR = 3_600
DAY = 86_400
BOUNDARIES = SplitBoundaries(EventTime(BASE_TIME + 2 * DAY), EventTime(BASE_TIME + 3 * DAY))
COUNT_1H = FEATURE_SET_V1.names.index("tx_count_1h")
COUNT_24H = FEATURE_SET_V1.names.index("tx_count_24h")


def history() -> list[LabeledTransaction]:
    """Return 4 days of transactions for two cards, every 5 hours, in canonical order."""
    items = []
    for step in range(20):
        time = BASE_TIME + step * 5 * HOUR
        items.append(LabeledTransaction(2 * step, make_tx(2 * step + 1, time), step % 7 == 0))
        other = make_tx(2 * step + 2, time + 60, card=OTHER_CARD, cents=500 + step)
        items.append(LabeledTransaction(2 * step + 1, other, is_fraud=False))
    return items


def build(
    items: list[LabeledTransaction], rows_range: TimeRange | None
) -> tuple[DatasetSummary, tuple[TrainingRow, ...]]:
    """Run BuildTrainingSet over items with memory adapters and return summary and rows."""
    sink = MemoryTrainingSink(FEATURE_SET_V1.version, len(FEATURE_SET_V1.names))
    use_case = BuildTrainingSet(
        source=MemoryHistoricalSource(items),
        store=MemoryWindowStore(),
        sink=sink,
        plan=BuildPlan(feature_set=FEATURE_SET_V1, boundaries=BOUNDARIES),
    )
    summary = use_case.execute(rows_range)
    return summary, sink.rows


def test_split_boundaries_assign_half_open_intervals() -> None:
    """Assign TRAIN before valid_start, VALID up to test_start, TEST from test_start."""
    valid, test = BOUNDARIES.valid_start.epoch_s, BOUNDARIES.test_start.epoch_s
    assert BOUNDARIES.split_of(EventTime(valid - 1)) is Split.TRAIN
    assert BOUNDARIES.split_of(EventTime(valid)) is Split.VALID
    assert BOUNDARIES.split_of(EventTime(test - 1)) is Split.VALID
    assert BOUNDARIES.split_of(EventTime(test)) is Split.TEST


def test_split_boundaries_reject_reversed_order() -> None:
    """Reject a test boundary that is not after the valid boundary."""
    with pytest.raises(InvalidMessageError):
        SplitBoundaries(EventTime(10), EventTime(10))


def test_transaction_is_not_in_its_own_window() -> None:
    """Count zero prior transactions for a card's first event and one for its second."""
    _, rows = build(history(), None)
    first, second = rows[0], rows[2]
    assert first.vector.values[COUNT_24H] == 0.0
    assert second.vector.values[COUNT_24H] == 1.0
    assert second.vector.values[COUNT_1H] == 0.0


def test_every_transaction_gets_one_row_in_seq_order() -> None:
    """Write one row per transaction, in seq order, with the source label."""
    items = history()
    _, rows = build(items, None)
    assert [row.seq for row in rows] == [item.seq for item in items]
    assert [row.is_fraud for row in rows] == [item.is_fraud for item in items]


def test_summary_counts_rows_and_frauds_per_split() -> None:
    """Report row and fraud counts per split that match the rows written."""
    summary, rows = build(history(), None)
    for count in summary.counts:
        in_split = [row for row in rows if row.split is count.split]
        assert count.rows == len(in_split)
        assert count.frauds == sum(row.is_fraud for row in in_split)
    assert summary.ref.rows == len(rows)


def test_chunked_build_with_warmup_equals_single_pass() -> None:
    """Build two adjacent ranges separately and get exactly the single-pass rows."""
    items = history()
    _, full = build(items, None)
    middle = EventTime(BASE_TIME + 2 * DAY + 7 * HOUR)
    end = EventTime(BASE_TIME + 10 * DAY)
    _, first = build(items, TimeRange(EventTime(0), middle))
    _, second = build(items, TimeRange(middle, end))
    assert first + second == full


def test_chunk_without_warmup_differs_from_single_pass() -> None:
    """Get different features when the warm-up period is disabled, so warm-up is required."""
    items = history()
    middle = EventTime(BASE_TIME + 2 * DAY + 7 * HOUR)
    _, full = build(items, None)
    sink = MemoryTrainingSink(FEATURE_SET_V1.version, len(FEATURE_SET_V1.names))
    plan = BuildPlan(feature_set=FEATURE_SET_V1, boundaries=BOUNDARIES, warmup_seconds=0)
    BuildTrainingSet(MemoryHistoricalSource(items), MemoryWindowStore(), sink, plan).execute(
        TimeRange(middle, EventTime(BASE_TIME + 10 * DAY))
    )
    assert sink.rows != full[-len(sink.rows) :]


def test_missing_seq_in_source_is_rejected() -> None:
    """Raise PositionGapError when the source skips a seq value."""
    items = history()
    del items[3]
    with pytest.raises(PositionGapError):
        build(items, None)
