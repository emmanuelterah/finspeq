"""Tests for card slices and partitioned batches."""

from __future__ import annotations

import pytest

from finspeq.application.errors import InvalidMessageError
from finspeq.application.messages import LabeledTransaction
from finspeq.application.replay import CardSlice, card_bucket, partitioned_batches
from finspeq.domain.model import CardId
from tests.factories import BASE_TIME, make_tx

CARDS = [f"40000000000000{n:02d}" for n in range(20)]


def items() -> list[LabeledTransaction]:
    """Return 60 transactions over 20 cards, three per card, in canonical order."""
    result = []
    for step in range(60):
        tx = make_tx(step + 1, BASE_TIME + step, card=CARDS[step % 20])
        result.append(LabeledTransaction(step, tx, is_fraud=False))
    return result


def test_card_bucket_is_stable_and_in_range() -> None:
    """Return the same bucket on every call, within [0, buckets)."""
    card = CardId(CARDS[0])
    assert card_bucket(card, 7) == card_bucket(card, 7)
    assert {card_bucket(CardId(c), 4) for c in CARDS} <= {0, 1, 2, 3}


def test_card_slices_partition_the_cards() -> None:
    """Put every card in exactly one of the slices of a modulus."""
    slices = [CardSlice(3, r) for r in range(3)]
    for card in CARDS:
        assert sum(s.contains(CardId(card)) for s in slices) == 1


def test_invalid_slice_is_rejected() -> None:
    """Reject a remainder outside [0, modulus)."""
    with pytest.raises(InvalidMessageError):
        CardSlice(3, 3)


def test_partitioned_batches_keep_card_order_and_consecutive_offsets() -> None:
    """Give each partition offsets 0, 1, 2, ... and keep each card on one partition in order."""
    batches = list(partitioned_batches(items(), partitions=3, batch_size=4))
    offsets: dict[int, list[int]] = {}
    card_partition: dict[str, int] = {}
    for batch in batches:
        assert len(batch.events) <= 4
        for event in batch.events:
            offsets.setdefault(batch.partition, []).append(event.position.offset)
            card = event.transaction.card.value
            assert card_partition.setdefault(card, batch.partition) == batch.partition
    for values in offsets.values():
        assert values == list(range(len(values)))
    assert sum(len(v) for v in offsets.values()) == 60
