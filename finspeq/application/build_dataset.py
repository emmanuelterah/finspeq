"""BuildTrainingSet: training rows computed with the same feature code as online scoring."""

from __future__ import annotations

from dataclasses import dataclass

from finspeq.application.errors import InvalidMessageError
from finspeq.application.messages import (
    DatasetSummary,
    Event,
    EventBatch,
    EventPosition,
    LabeledTransaction,
    Split,
    SplitCount,
    TimeRange,
    TrainingRow,
)
from finspeq.application.ports import HistoricalSource, TrainingSink, WindowStore
from finspeq.domain.features import FeatureSet
from finspeq.domain.model import EventTime
from finspeq.domain.window import WINDOW_SECONDS

OFFLINE_PARTITION = 0


@dataclass(frozen=True, slots=True)
class SplitBoundaries:
    """Event-time boundaries: TRAIN before valid_start, VALID before test_start, TEST after."""

    valid_start: EventTime
    test_start: EventTime

    def __post_init__(self) -> None:
        """Raise InvalidMessageError unless valid_start is before test_start."""
        if self.valid_start >= self.test_start:
            raise InvalidMessageError(f"valid_start must be before test_start, got {self}")

    def split_of(self, time: EventTime) -> Split:
        """Return the split that contains time."""
        if time < self.valid_start:
            return Split.TRAIN
        if time < self.test_start:
            return Split.VALID
        return Split.TEST


@dataclass(frozen=True, slots=True)
class BuildPlan:
    """Feature set, split boundaries, and warm-up length used to build a dataset."""

    feature_set: FeatureSet
    boundaries: SplitBoundaries
    warmup_seconds: int = WINDOW_SECONDS


class BuildTrainingSet:
    """Writes one training row per historical transaction in a time range."""

    def __init__(
        self,
        source: HistoricalSource,
        store: WindowStore,
        sink: TrainingSink,
        plan: BuildPlan,
    ) -> None:
        """Create the use case.

        Args:
            source: Labelled transactions in canonical order.
            store: Empty window store; receives every transaction read from source.
            sink: Destination of the training rows.
            plan: Feature set, split boundaries, and warm-up length.
        """
        self._source = source
        self._store = store
        self._sink = sink
        self._plan = plan

    def execute(self, rows_range: TimeRange | None) -> DatasetSummary:
        """Write a row for every transaction in rows_range, close the sink, and return counts.

        Transactions in the warm-up period before rows_range.start are applied to the store
        without writing rows. For every transaction the window is read before the transaction
        is applied, so a transaction is never part of its own window.

        Args:
            rows_range: Event times to write rows for; None writes every transaction.

        Raises:
            InvalidSourceDataError: The source yields data out of order or invalid data.
            PositionGapError: The source skipped a seq value inside the read range.
        """
        counts = _SplitCounter()
        for item in self._source.iter_labeled(self._read_range(rows_range)):
            if rows_range is None or rows_range.contains(item.transaction.time):
                self._write_row(item, counts)
            self._apply(item)
        return DatasetSummary(ref=self._sink.close(), counts=counts.as_tuple())

    def _read_range(self, rows_range: TimeRange | None) -> TimeRange | None:
        """Return rows_range extended backwards by the warm-up length, or None for no limit."""
        if rows_range is None:
            return None
        start = max(0, rows_range.start.epoch_s - self._plan.warmup_seconds)
        return TimeRange(start=EventTime(start), end=rows_range.end)

    def _write_row(self, item: LabeledTransaction, counts: _SplitCounter) -> None:
        """Compute item's feature vector from the store and write its training row."""
        tx = item.transaction
        window = self._store.read(tx.card, tx.order_key)
        split = self._plan.boundaries.split_of(tx.time)
        row = TrainingRow(
            seq=item.seq,
            time=tx.time,
            tx_id=tx.tx_id,
            vector=self._plan.feature_set.compute(tx, window),
            is_fraud=item.is_fraud,
            split=split,
        )
        self._sink.write(row)
        counts.add(split, is_fraud=item.is_fraud)

    def _apply(self, item: LabeledTransaction) -> None:
        """Apply item to the store as a one-event batch at offset item.seq."""
        position = EventPosition(OFFLINE_PARTITION, item.seq)
        event = Event(position=position, transaction=item.transaction)
        self._store.apply(EventBatch(partition=OFFLINE_PARTITION, events=(event,)))


class _SplitCounter:
    """Running row and fraud counts per split."""

    def __init__(self) -> None:
        """Create counters at zero for every split."""
        self._rows = dict.fromkeys(Split, 0)
        self._frauds = dict.fromkeys(Split, 0)

    def add(self, split: Split, *, is_fraud: bool) -> None:
        """Count one row in split, and one fraud row if is_fraud."""
        self._rows[split] += 1
        self._frauds[split] += int(is_fraud)

    def as_tuple(self) -> tuple[SplitCount, ...]:
        """Return the counts in Split declaration order."""
        return tuple(SplitCount(s, self._rows[s], self._frauds[s]) for s in Split)
