"""Data passed between use cases and ports."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

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
class LedgerRow:
    """One recorded decision: the transaction's identity, its decision record, and position."""

    tx_id: TxId
    card: str
    time: EventTime
    amount_cents: int
    record: DecisionRecord
    position: EventPosition


@dataclass(frozen=True, slots=True)
class ApplyResult:
    """Outcome of storing one batch: counts and the stored position afterwards.

    Attributes:
        applied: Events stored.
        skipped: Events not stored: at or below the stored position, or a repeated tx_id.
        position: The stored position after the batch, or None if nothing was ever applied.
        behind: Stored events whose time is earlier than their card's newest event time
            before them (out of order for their card).
    """

    applied: int
    skipped: int
    position: EventPosition | None
    behind: int


class Split(Enum):
    """Dataset partition a training row belongs to."""

    TRAIN = "train"
    VALID = "valid"
    TEST = "test"


@dataclass(frozen=True, slots=True)
class TimeRange:
    """Half-open interval [start, end) of event times."""

    start: EventTime
    end: EventTime

    def __post_init__(self) -> None:
        """Raise InvalidMessageError unless start is before end."""
        if self.start >= self.end:
            raise InvalidMessageError(f"start must be before end, got {self}")

    def contains(self, time: EventTime) -> bool:
        """Return True if start <= time < end."""
        return self.start <= time < self.end


@dataclass(frozen=True, slots=True)
class LabeledTransaction:
    """A historical transaction, its position in canonical order, and its fraud label."""

    seq: int
    transaction: Transaction
    is_fraud: bool


@dataclass(frozen=True, slots=True)
class TrainingRow:
    """One labelled example: canonical position, identity, feature vector, label, and split."""

    seq: int
    time: EventTime
    tx_id: TxId
    vector: FeatureVector
    is_fraud: bool
    split: Split


@dataclass(frozen=True, slots=True)
class DatasetRef:
    """Location and shape of a completed training dataset or dataset part."""

    path: str
    rows: int
    schema: str


@dataclass(frozen=True, slots=True)
class SplitCount:
    """Number of rows and fraud rows written for one split."""

    split: Split
    rows: int
    frauds: int


@dataclass(frozen=True, slots=True)
class DatasetSummary:
    """Result of building a dataset part: its reference and per-split counts."""

    ref: DatasetRef
    counts: tuple[SplitCount, ...]


class Imbalance(Enum):
    """Weighting of fraud rows during training relative to legitimate rows."""

    NONE = "none"
    SQRT = "sqrt"
    BALANCED = "balanced"


@dataclass(frozen=True, slots=True)
class TrainParams:
    """Gradient-boosting hyperparameters, imbalance strategy, seed, and thread count."""

    imbalance: Imbalance = Imbalance.NONE
    learning_rate: float = 0.05
    num_leaves: int = 63
    min_data_in_leaf: int = 200
    feature_fraction: float = 0.9
    bagging_fraction: float = 0.8
    bagging_freq: int = 1
    lambda_l2: float = 1.0
    max_rounds: int = 2_000
    early_stopping_rounds: int = 100
    seed: int = 42
    num_threads: int = 4


@dataclass(frozen=True, slots=True)
class ModelArtifact:
    """A trained model file and the facts needed to export and audit it."""

    path: str
    version: str
    schema: str
    feature_names: tuple[str, ...]
    best_iteration: int
    positive_weight: float
    dataset_manifest_sha256: str


@dataclass(frozen=True, slots=True)
class ModelRef:
    """Location, version, and feature schema of an exported inference model."""

    directory: str
    version: str
    schema: str


@dataclass(frozen=True, slots=True)
class ScoredSplit:
    """Scores and labels of every row of one dataset split, in dataset order."""

    split: Split
    scores: tuple[float, ...]
    labels: tuple[bool, ...]

    def __post_init__(self) -> None:
        """Raise InvalidMessageError if scores and labels differ in length."""
        if len(self.scores) != len(self.labels):
            raise InvalidMessageError(f"{len(self.scores)} scores but {len(self.labels)} labels")
