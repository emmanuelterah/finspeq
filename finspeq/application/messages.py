"""Data passed between use cases and ports."""

from __future__ import annotations

from dataclasses import dataclass

from finspeq.application.errors import InvalidMessageError
from finspeq.domain.features import FeatureVector
from finspeq.domain.model import EventTime, Transaction, TxId
from finspeq.domain.scored import DecisionRecord


@dataclass(frozen=True, slots=True, order=True)
class EventPosition:
    """Location of one event in the log: partition number and offset within it."""

    partition: int
    offset: int

    def __post_init__(self) -> None:
        """Raise InvalidMessageError if partition or offset is negative."""
        if self.partition < 0 or self.offset < 0:
            raise InvalidMessageError(f"position must be non-negative, got {self}")


@dataclass(frozen=True, slots=True)
class Event:
    """One log record: a transaction, its position, and its decision record if scored."""

    position: EventPosition
    transaction: Transaction
    record: DecisionRecord | None = None


@dataclass(frozen=True, slots=True)
class EventBatch:
    """Events read from one partition, in strictly increasing offset order."""

    partition: int
    events: tuple[Event, ...]

    def __post_init__(self) -> None:
        """Raise InvalidMessageError if the batch is empty, mixed, or not in offset order."""
        if not self.events:
            raise InvalidMessageError("batch must contain at least one event")
        previous = -1
        for event in self.events:
            if event.position.partition != self.partition:
                raise InvalidMessageError(f"event from partition {event.position.partition}")
            if event.position.offset <= previous:
                raise InvalidMessageError("offsets must be strictly increasing")
            previous = event.position.offset

    @property
    def last_position(self) -> EventPosition:
        """Return the position of the batch's last event."""
        return self.events[-1].position


@dataclass(frozen=True, slots=True)
class ApplyResult:
    """Outcome of storing one batch: counts and the stored position afterwards."""

    applied: int
    skipped: int
    position: EventPosition | None


@dataclass(frozen=True, slots=True)
class TrainingRow:
    """One labelled example: transaction identity, feature vector, and fraud label."""

    time: EventTime
    tx_id: TxId
    vector: FeatureVector
    is_fraud: bool


@dataclass(frozen=True, slots=True)
class DatasetRef:
    """Location and shape of a completed training dataset."""

    path: str
    rows: int
    schema: str
