"""Tests for the transactional relay's transform and copy counting."""

from __future__ import annotations

import json
from typing import cast

import pytest

pytest.importorskip("confluent_kafka")

from confluent_kafka import Message

from experiments.transactional_relay import count_copies, transform


class FakeMessage:
    """Consumed message with a partition, offset, key, and value."""

    def partition(self) -> int:
        """Return 2."""
        return 2

    def offset(self) -> int:
        """Return 41."""
        return 41

    def key(self) -> bytes:
        """Return the card key."""
        return b"4000000000000002"

    def value(self) -> bytes:
        """Return a ten-byte value."""
        return b"0123456789"


def test_transform_names_the_input_position() -> None:
    """Keep the key and write the source partition and offset into the value."""
    key, value = transform(cast(Message, FakeMessage()))
    assert key == b"4000000000000002"
    assert json.loads(value) == {"source_partition": 2, "source_offset": 41, "bytes": 10}


def test_copy_count_finds_missing_and_duplicated_positions() -> None:
    """Count one missing input and one input copied three times."""
    positions = [(0, 0), (0, 1), (0, 1), (0, 1), (1, 0)]
    result = count_copies(positions, {0: 3, 1: 1}, "read_committed")
    assert (result.records, result.inputs, result.missing) == (5, 4, 1)
    assert (result.duplicated, result.extra_copies) == (1, 2)
