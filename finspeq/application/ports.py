"""Outbound port interfaces implemented by adapters."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Protocol

from finspeq.application.messages import (
    ApplyResult,
    DatasetRef,
    EventBatch,
    EventPosition,
    LabeledTransaction,
    LedgerRow,
    ModelArtifact,
    ModelRef,
    ScoredSplit,
    Split,
    TimeRange,
    TrainingRow,
    TrainParams,
)
from finspeq.domain.features import FeatureVector
from finspeq.domain.model import CardId, OrderKey, Transaction
from finspeq.domain.policy import Score, Thresholds
from finspeq.domain.scored import ScoredTransaction
from finspeq.domain.window import CardWindow


class WindowReader(Protocol):
    """Read access to per-card event windows."""

    def read(self, card: CardId, until: OrderKey) -> CardWindow:
        """Return the card's entries before `until` and within WINDOW_SECONDS of it.

        Raises:
            StoreUnavailableError: The store did not respond.
        """
        ...


class WindowWriter(Protocol):
    """Atomic write access to per-card event windows and partition positions."""

    def apply(self, batch: EventBatch) -> ApplyResult:
        """Store the batch's events and its last position as one atomic step.

        Events at or below the stored position for the partition are skipped. A transaction id
        already stored for its card keeps its first entry. Each card touched by the batch loses
        entries older than its newest event time minus WINDOW_SECONDS and the allowed lateness.
        The result counts stored events that are earlier than their card's newest event time
        before them, in batch order.

        Raises:
            PositionGapError: The unapplied offsets do not continue from the stored position
                without gaps. Nothing is written.
            StoreUnavailableError: The store did not respond. Nothing is written.
        """
        ...

    def position(self, partition: int) -> EventPosition | None:
        """Return the last applied position for partition, or None if nothing was applied."""
        ...


class WindowStore(WindowReader, WindowWriter, Protocol):
    """A store that implements both window ports."""


class WindowAppender(Protocol):
    """Write access to one card window outside any partition position (read-your-writes)."""

    def append(self, tx: Transaction) -> bool:
        """Store tx's entry for its card and prune that card as WindowWriter.apply does.

        No partition position changes. A transaction id already stored for the card keeps its
        first entry, so a later apply of the same event stores nothing.

        Returns:
            True if the entry was stored; False if the card already held tx's id.

        Raises:
            StoreUnavailableError: The store did not respond. Nothing is written.
        """
        ...


class FraudModel(Protocol):
    """Fraud probability model loaded for inference."""

    @property
    def version(self) -> str:
        """Return the model's version identifier."""
        ...

    @property
    def schema(self) -> str:
        """Return the feature-set version the model was trained on."""
        ...

    def predict(self, vector: FeatureVector) -> Score:
        """Return the fraud probability for vector.

        Raises:
            SchemaMismatchError: vector.schema differs from the model's schema.
        """
        ...


class EventPublisher(Protocol):
    """Writer of transactions to the event log."""

    def publish_raw(self, tx: Transaction) -> EventPosition:
        """Append tx without a decision record and return its position after the broker ack.

        Raises:
            PublishFailedError: The broker did not acknowledge the event.
        """
        ...

    def publish_scored(self, scored: ScoredTransaction) -> EventPosition:
        """Append scored and return its position after the broker ack.

        Raises:
            PublishFailedError: The broker did not acknowledge the event.
        """
        ...


class BulkPublisher(Protocol):
    """Writer of many raw transactions to the event log."""

    def publish_raw_many(self, txs: Sequence[Transaction]) -> tuple[EventPosition, ...]:
        """Append txs in order and return their positions after every broker ack.

        Raises:
            PublishFailedError: The broker did not acknowledge at least one transaction.
        """
        ...


class EventSource(Protocol):
    """Reader of log events, one single-partition batch at a time."""

    def poll(self, max_events: int) -> EventBatch | None:
        """Return up to max_events events from one partition, or None if none are available."""
        ...

    def seek(self, position: EventPosition) -> None:
        """Make the next read of position.partition start at position.offset."""
        ...

    def acknowledge(self, batch: EventBatch) -> None:
        """Record that batch was applied; used for lag reporting, never for recovery."""
        ...


class DecisionLedger(Protocol):
    """Durable record of decisions and of the ledger's partition positions."""

    def record(self, batch: EventBatch) -> ApplyResult:
        """Insert one row per scored event and store the batch position in one transaction.

        Events without a decision record and events at or below the stored position are
        skipped. A transaction id already recorded keeps its first row.

        Raises:
            PositionGapError: The unapplied offsets do not continue from the stored position
                without gaps. Nothing is written.
            StoreUnavailableError: The database did not respond. Nothing is written.
        """
        ...

    def position(self, partition: int) -> EventPosition | None:
        """Return the last recorded position for partition, or None if nothing was recorded."""
        ...


class DecisionReader(Protocol):
    """Read access to every recorded decision, for verification."""

    def rows(self) -> Iterator[LedgerRow]:
        """Yield every recorded decision in tx_id order.

        Raises:
            StoreUnavailableError: The database did not respond.
        """
        ...


class HistoricalSource(Protocol):
    """Reader of labelled historical transactions in canonical order."""

    def iter_labeled(self, time_range: TimeRange | None) -> Iterator[LabeledTransaction]:
        """Yield labelled transactions in (time, tx_id) order, limited to time_range if given.

        Raises:
            InvalidSourceDataError: A record is out of canonical order or violates a domain rule.
        """
        ...


class TrainingSink(Protocol):
    """Writer of one training dataset."""

    def write(self, row: TrainingRow) -> None:
        """Append row to the dataset being written."""
        ...

    def close(self) -> DatasetRef:
        """Finish the dataset, make it visible in one atomic step, and return its reference."""
        ...


class ModelTrainer(Protocol):
    """Trainer of fraud models on a training dataset."""

    def train(self, dataset: DatasetRef, params: TrainParams, output: str) -> ModelArtifact:
        """Fit on the TRAIN split with early stopping on VALID and write the model to output.

        The same dataset, params, and library version produce an identical model file.
        """
        ...


class BatchScorer(Protocol):
    """Scorer of every row of a dataset split."""

    def score(self, dataset: DatasetRef, split: Split) -> ScoredSplit:
        """Return a score and the label of every row of split, in dataset order."""
        ...


class ModelExporter(Protocol):
    """Converter of trained models into inference models with metadata."""

    def export(self, artifact: ModelArtifact, thresholds: Thresholds, root: str) -> ModelRef:
        """Write the inference model and its metadata into a new directory under root.

        The directory appears only when complete. Its name is the inference model's version.
        """
        ...


class Metrics(Protocol):
    """Recorder of operational metrics."""

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        """Record one sample of the histogram `name`. Never raises."""
        ...

    def increment(
        self, name: str, amount: float = 1.0, labels: Mapping[str, str] | None = None
    ) -> None:
        """Add amount to the counter `name`. Never raises."""
        ...


class Clock(Protocol):
    """Source of monotonic time for latency measurement only; never used for features."""

    def now(self) -> float:
        """Return a monotonic time in seconds; only differences between calls are meaningful."""
        ...
