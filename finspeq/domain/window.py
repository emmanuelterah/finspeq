"""Per-card event window: a card's transactions in the 24 hours before an event."""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass

from finspeq.domain.errors import InvalidWindowError
from finspeq.domain.model import CardId, EventTime, Money, OrderKey, Transaction, TxId

WINDOW_SECONDS = 86_400


@dataclass(frozen=True, slots=True)
class WindowEntry:
    """Stored facts about one past transaction of a card."""

    tx_id: TxId
    time: EventTime
    amount: Money
    merchant: str

    @classmethod
    def from_transaction(cls, tx: Transaction) -> WindowEntry:
        """Return the entry that stores tx's id, time, amount, and merchant."""
        return cls(tx_id=tx.tx_id, time=tx.time, amount=tx.amount, merchant=tx.merchant)

    @property
    def order_key(self) -> OrderKey:
        """Return the entry's canonical ordering key."""
        return OrderKey(time=self.time, tx_id=self.tx_id)


@dataclass(frozen=True, slots=True)
class CardWindow:
    """A card's entries before `until` and within WINDOW_SECONDS of it, in OrderKey order."""

    card: CardId
    until: OrderKey
    entries: tuple[WindowEntry, ...]

    def __post_init__(self) -> None:
        """Raise InvalidWindowError unless entries are strictly ordered and inside the window.

        An entry is inside the window when its order key is less than `until` and its time is
        at least `until.time.epoch_s - WINDOW_SECONDS`.
        """
        lower = self.until.time.epoch_s - WINDOW_SECONDS
        previous: OrderKey | None = None
        for entry in self.entries:
            key = entry.order_key
            if previous is not None and key <= previous:
                raise InvalidWindowError("entries must be strictly increasing by order key")
            if key >= self.until or entry.time.epoch_s < lower:
                raise InvalidWindowError(f"entry {entry.tx_id.value} is outside the window")
            previous = key

    def recent(self, seconds: int) -> tuple[WindowEntry, ...]:
        """Return the entries with time at least `until.time.epoch_s - seconds`, in order.

        Args:
            seconds: Look-back length in seconds. Values above WINDOW_SECONDS return every entry.
        """
        lower = self.until.time.epoch_s - seconds
        start = bisect_left(self.entries, lower, key=_entry_epoch)
        return self.entries[start:]

    def last(self) -> WindowEntry | None:
        """Return the most recent entry, or None if the window is empty."""
        return self.entries[-1] if self.entries else None


def _entry_epoch(entry: WindowEntry) -> int:
    """Return the entry's event time in epoch seconds."""
    return entry.time.epoch_s
