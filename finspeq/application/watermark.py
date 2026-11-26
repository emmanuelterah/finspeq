"""Event-time watermark of one ordered stream (one partition)."""

from __future__ import annotations

from finspeq.domain.model import EventTime


class Watermark:
    """Tracks the newest event time seen and classifies events that arrive behind it.

    The watermark is the newest event time minus allowed_lateness_s. An event whose time is
    below the watermark is late.
    """

    def __init__(self, allowed_lateness_s: int) -> None:
        """Create a watermark that has seen no events."""
        self._allowed_lateness_s = allowed_lateness_s
        self._newest: int | None = None

    def observe(self, time: EventTime) -> int:
        """Record time and return how many seconds it is behind the newest time seen before it.

        Returns 0 for the first event and for events at or after the newest time.
        """
        behind = 0 if self._newest is None else max(0, self._newest - time.epoch_s)
        self._newest = time.epoch_s if self._newest is None else max(self._newest, time.epoch_s)
        return behind

    def is_late(self, behind_s: int) -> bool:
        """Return True if an event behind_s seconds behind the newest time is below the mark."""
        return behind_s > self._allowed_lateness_s
