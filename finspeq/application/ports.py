"""Outbound port interfaces implemented by adapters."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from finspeq.application.messages import (
    ApplyResult,
    DatasetRef,
    EventBatch,
    EventPosition,
    TrainingRow,
)
from finspeq.domain.features import FeatureVector
from finspeq.domain.model import CardId, OrderKey, Transaction
from finspeq.domain.policy import Score
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

        Raises:
            PositionGapError: The unapplied offsets do not continue from the stored position
                without gaps. Nothing is written.
            StoreUnavailableError: The store did not respond. Nothing is written.
        """
        ...

    def position(self, partition: int) -> EventPosition | None:
        """Return the last applied position for partition, or None if nothing was applied."""
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


class EventSource(Protocol):
    """Reader of log events, one single-partition batch at a time."""

    def poll(self, max_events: int) -> EventBatch | None:
        """Return up to max_events events from one partition, or None if none are available."""
        ...

    def seek(self, position: EventPosition) -> None:
        """Make the next read of position.partition start at position.offset."""
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


class TrainingSink(Protocol):
    """Writer of one training dataset."""

    def write(self, row: TrainingRow) -> None:
        """Append row to the dataset being written."""
        ...

    def close(self) -> DatasetRef:
        """Finish the dataset, make it visible in one atomic step, and return its reference."""
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
    """Source of wall-clock time for latency measurement only."""

    def now(self) -> float:
        """Return the current wall-clock time in seconds since the Unix epoch."""
        ...
