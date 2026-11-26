"""In-process implementation of WindowReader and WindowWriter."""

from __future__ import annotations

from bisect import bisect_left, insort

from finspeq.application.errors import PositionGapError
from finspeq.application.messages import ApplyResult, Event, EventBatch, EventPosition
from finspeq.application.ports import WindowReader, WindowWriter
from finspeq.domain.model import CardId, OrderKey, Transaction
from finspeq.domain.window import WINDOW_SECONDS, CardWindow, WindowEntry


class MemoryWindowStore(WindowReader, WindowWriter):
    """Card windows and partition positions held in process memory."""

    def __init__(self, allowed_lateness_s: int = 0) -> None:
        """Create an empty store.

        Args:
            allowed_lateness_s: Seconds an entry is kept beyond WINDOW_SECONDS behind its
                card's newest event time.
        """
        self._lateness = allowed_lateness_s
        self._entries: dict[CardId, list[WindowEntry]] = {}
        self._newest: dict[CardId, int] = {}
        self._positions: dict[int, int] = {}

    def read(self, card: CardId, until: OrderKey) -> CardWindow:
        """Return the card's entries before `until` and within WINDOW_SECONDS of it."""
        entries = self._entries.get(card, [])
        start = bisect_left(entries, until.time.epoch_s - WINDOW_SECONDS, key=_entry_epoch)
        stop = bisect_left(entries, until, key=_entry_key)
        return CardWindow(card=card, until=until, entries=tuple(entries[start:stop]))

    def apply(self, batch: EventBatch) -> ApplyResult:
        """Store the batch's events and its last position as one atomic step.

        Checks offsets first, builds the new card lists on copies, then replaces the stored
        lists and position together. Semantics match WindowWriter.apply.

        Raises:
            PositionGapError: The unapplied offsets do not continue from the stored position.
        """
        pending = _unapplied(batch, self._positions.get(batch.partition))
        if not pending:
            return ApplyResult(0, len(batch.events), self.position(batch.partition))
        staging = _Staging(self._entries, self._newest)
        applied = sum(1 for event in pending if staging.add(event.transaction))
        staging.prune(WINDOW_SECONDS + self._lateness)
        self._entries.update(staging.entries)
        self._newest.update(staging.newest)
        self._positions[batch.partition] = pending[-1].position.offset
        return ApplyResult(applied, len(batch.events) - applied, pending[-1].position)

    def position(self, partition: int) -> EventPosition | None:
        """Return the last applied position for partition, or None if nothing was applied."""
        offset = self._positions.get(partition)
        return None if offset is None else EventPosition(partition, offset)


class _Staging:
    """Copies of the card lists touched by one batch, modified before being committed."""

    def __init__(self, entries: dict[CardId, list[WindowEntry]], newest: dict[CardId, int]) -> None:
        """Create empty staging over the committed entries and newest-time maps.

        Args:
            entries: Committed card lists; read only.
            newest: Committed newest event time per card in epoch seconds; read only.
        """
        self._committed_entries = entries
        self._committed_newest = newest
        self.entries: dict[CardId, list[WindowEntry]] = {}
        self.newest: dict[CardId, int] = {}

    def add(self, tx: Transaction) -> bool:
        """Insert tx's entry into its card's staged list in order key order.

        Returns:
            True if the entry was inserted; False if the card already holds tx's id.
        """
        entries = self._card_entries(tx.card)
        previous = self.newest.get(tx.card, self._committed_newest.get(tx.card, tx.time.epoch_s))
        self.newest[tx.card] = max(previous, tx.time.epoch_s)
        if any(entry.tx_id == tx.tx_id for entry in entries):
            return False
        insort(entries, WindowEntry.from_transaction(tx), key=_entry_key)
        return True

    def prune(self, keep_seconds: int) -> None:
        """Remove staged entries older than their card's newest time minus keep_seconds."""
        for card, entries in self.entries.items():
            boundary = self.newest[card] - keep_seconds
            del entries[: bisect_left(entries, boundary, key=_entry_epoch)]

    def _card_entries(self, card: CardId) -> list[WindowEntry]:
        """Return the staged list for card, copying the committed list on first access."""
        staged = self.entries.get(card)
        if staged is None:
            staged = list(self._committed_entries.get(card, ()))
            self.entries[card] = staged
        return staged


def _unapplied(batch: EventBatch, stored: int | None) -> tuple[Event, ...]:
    """Return the batch events after the stored offset, checking they are consecutive.

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


def _entry_epoch(entry: WindowEntry) -> int:
    """Return the entry's event time in epoch seconds."""
    return entry.time.epoch_s


def _entry_key(entry: WindowEntry) -> OrderKey:
    """Return the entry's canonical ordering key."""
    return entry.order_key
