"""The position rule shared by every store that records partition positions."""

from __future__ import annotations

from finspeq.application.errors import PositionGapError
from finspeq.application.messages import Event, EventBatch


def unapplied_events(batch: EventBatch, stored: int | None) -> tuple[Event, ...]:
    """Return the batch events after the stored offset, checking that they are consecutive.

    Args:
        batch: Batch to filter.
        stored: Last applied offset for the batch's partition, or None if none.

    Raises:
        PositionGapError: An unapplied offset is not the previous offset plus one, or the first
            one is not stored plus one.
    """
    pending = tuple(e for e in batch.events if stored is None or e.position.offset > stored)
    expected = None if stored is None else stored + 1
    for event in pending:
        if expected is not None and event.position.offset != expected:
            raise PositionGapError(
                f"partition {batch.partition}: expected offset {expected}, "
                f"got {event.position.offset}"
            )
        expected = event.position.offset + 1
    return pending
