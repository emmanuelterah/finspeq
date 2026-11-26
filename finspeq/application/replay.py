"""Deterministic card slices and partitioned event batches built from historical data."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from finspeq.application.errors import InvalidMessageError
from finspeq.application.messages import (
    Event,
    EventBatch,
    EventPosition,
    LabeledTransaction,
)
from finspeq.domain.model import CardId

HASH_PREFIX_HEX_CHARS = 16


def card_bucket(card: CardId, buckets: int) -> int:
    """Return int(first 16 hex chars of SHA-256(card)) mod buckets; stable across processes."""
    digest = hashlib.sha256(card.value.encode("ascii")).hexdigest()
    return int(digest[:HASH_PREFIX_HEX_CHARS], 16) % buckets


@dataclass(frozen=True, slots=True)
class CardSlice:
    """The cards whose bucket among `modulus` buckets equals `remainder`."""

    modulus: int
    remainder: int

    def __post_init__(self) -> None:
        """Raise InvalidMessageError unless 0 <= remainder < modulus."""
        if not 0 <= self.remainder < self.modulus:
            raise InvalidMessageError(f"need 0 <= remainder < modulus, got {self}")

    def contains(self, card: CardId) -> bool:
        """Return True if card belongs to the slice."""
        return card_bucket(card, self.modulus) == self.remainder


ALL_CARDS = CardSlice(modulus=1, remainder=0)


def partitioned_batches(
    items: Iterable[LabeledTransaction], partitions: int, batch_size: int
) -> Iterator[EventBatch]:
    """Yield batches of items assigned to partitions by card_bucket, with per-partition offsets.

    Offsets start at 0 in each partition and increase by one per event. A batch holds up to
    batch_size consecutive events of one partition; a partial batch is yielded when another
    partition's batch fills, and every partial batch is yielded at the end, in partition order.
    """
    pending: dict[int, list[Event]] = {}
    next_offset = [0] * partitions
    for item in items:
        partition = card_bucket(item.transaction.card, partitions)
        position = EventPosition(partition, next_offset[partition])
        next_offset[partition] += 1
        events = pending.setdefault(partition, [])
        events.append(Event(position=position, transaction=item.transaction))
        if len(events) == batch_size:
            yield EventBatch(partition=partition, events=tuple(events))
            events.clear()
    for partition in sorted(pending):
        if pending[partition]:
            yield EventBatch(partition=partition, events=tuple(pending[partition]))
