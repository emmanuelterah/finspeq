"""Tests for application message invariants."""

from __future__ import annotations

import pytest

from finspeq.application.errors import InvalidMessageError
from finspeq.application.messages import Event, EventBatch, EventPosition, TimeRange
from finspeq.domain.model import EventTime
from tests.factories import batch, make_tx


def test_negative_position_is_rejected() -> None:
    """Reject a negative offset."""
    with pytest.raises(InvalidMessageError):
        EventPosition(0, -1)


def test_empty_batch_is_rejected() -> None:
    """Reject a batch without events."""
    with pytest.raises(InvalidMessageError):
        EventBatch(partition=0, events=())


def test_batch_with_decreasing_offsets_is_rejected() -> None:
    """Reject offsets that are not strictly increasing."""
    with pytest.raises(InvalidMessageError):
        batch((2, make_tx(1)), (1, make_tx(2)))


def test_batch_with_event_from_other_partition_is_rejected() -> None:
    """Reject an event whose partition differs from the batch partition."""
    event = Event(position=EventPosition(1, 0), transaction=make_tx(1))
    with pytest.raises(InvalidMessageError):
        EventBatch(partition=0, events=(event,))


def test_last_position_is_the_final_event_position() -> None:
    """Return the position of the last event."""
    assert batch((3, make_tx(1)), (4, make_tx(2))).last_position == EventPosition(0, 4)


def test_time_range_is_half_open() -> None:
    """Contain the start and exclude the end."""
    time_range = TimeRange(EventTime(10), EventTime(20))
    assert time_range.contains(EventTime(10))
    assert time_range.contains(EventTime(19))
    assert not time_range.contains(EventTime(20))


def test_empty_time_range_is_rejected() -> None:
    """Reject a range whose start is not before its end."""
    with pytest.raises(InvalidMessageError):
        TimeRange(EventTime(20), EventTime(20))
