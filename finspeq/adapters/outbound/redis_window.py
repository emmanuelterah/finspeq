"""Redis implementation of WindowReader and WindowWriter, using two Lua scripts."""

from __future__ import annotations

from importlib.resources import files

import redis

from finspeq.application.errors import (
    InvalidMessageError,
    PortError,
    PositionGapError,
    StoreUnavailableError,
)
from finspeq.application.messages import ApplyResult, EventBatch, EventPosition
from finspeq.application.ports import WindowReader, WindowWriter
from finspeq.domain.model import CardId, EventTime, Money, OrderKey, TxId
from finspeq.domain.window import WINDOW_SECONDS, CardWindow, WindowEntry

DEFAULT_PREFIX = "fq:"
NO_POSITION = -1
VALUE_SEPARATOR = "|"


class RedisWindowStore(WindowReader, WindowWriter):
    """Card windows in a sorted set plus a hash per card, and one position key per partition.

    Requires a standalone Redis server: one batch touches keys of many cards.
    """

    def __init__(
        self,
        client: redis.Redis,
        topic: str,
        allowed_lateness_s: int = 0,
        prefix: str = DEFAULT_PREFIX,
    ) -> None:
        """Create a store over client and register its Lua scripts.

        Args:
            client: Redis client created with decode_responses=False.
            topic: Topic name used in position keys.
            allowed_lateness_s: Seconds an entry is kept beyond WINDOW_SECONDS behind its
                card's newest event time.
            prefix: Prefix of every key the store writes.
        """
        self._client = client
        self._topic = topic
        self._keep_seconds = WINDOW_SECONDS + allowed_lateness_s
        self._prefix = prefix
        self._apply_script = client.register_script(_lua("apply_batch.lua"))
        self._read_script = client.register_script(_lua("read_window.lua"))

    def read(self, card: CardId, until: OrderKey) -> CardWindow:
        """Return the card's entries before `until` and within WINDOW_SECONDS of it.

        Runs one script call, so the sorted set and the hash are read as one snapshot.

        Raises:
            StoreUnavailableError: Redis did not respond.
        """
        lower = until.time.epoch_s - WINDOW_SECONDS
        keys = [self._card_key(card, "ev"), self._card_key(card, "tx")]
        args: list[str | int] = [lower, until.time.epoch_s, until.tx_id.value]
        try:
            flat = self._read_script(keys=keys, args=args)
        except (redis.ConnectionError, redis.TimeoutError) as exc:
            raise StoreUnavailableError(str(exc)) from exc
        entries = tuple(
            _entry(_text(flat[index]), _text(flat[index + 1])) for index in range(0, len(flat), 2)
        )
        return CardWindow(card=card, until=until, entries=entries)

    def apply(self, batch: EventBatch) -> ApplyResult:
        """Store the batch's events and its last position in one script call.

        Semantics match WindowWriter.apply.

        Raises:
            PositionGapError: The unapplied offsets do not continue from the stored position.
            InvalidMessageError: The script rejected the argument list.
            PortError: The script failed for another reason. Nothing is written.
            StoreUnavailableError: Redis did not respond. Nothing is written.
        """
        args: list[str | int] = [self._prefix, self._keep_seconds, len(batch.events)]
        for event in batch.events:
            tx = event.transaction
            args += [
                event.position.offset,
                tx.card.value,
                tx.tx_id.value,
                tx.time.epoch_s,
                tx.amount.cents,
                tx.merchant,
            ]
        try:
            applied, skipped, last = self._apply_script(
                keys=[self._position_key(batch.partition)], args=args
            )
        except redis.ResponseError as exc:
            message = str(exc)
            if message.startswith("GAP"):
                raise PositionGapError(f"partition {batch.partition}: {message}") from exc
            if message.startswith("INVALID"):
                raise InvalidMessageError(message) from exc
            raise PortError(f"apply_batch.lua failed: {message}") from exc
        except (redis.ConnectionError, redis.TimeoutError) as exc:
            raise StoreUnavailableError(str(exc)) from exc
        position = None if last == NO_POSITION else EventPosition(batch.partition, int(last))
        return ApplyResult(applied=int(applied), skipped=int(skipped), position=position)

    def position(self, partition: int) -> EventPosition | None:
        """Return the last applied position for partition, or None if nothing was applied.

        Raises:
            StoreUnavailableError: Redis did not respond.
        """
        try:
            value = self._client.get(self._position_key(partition))
        except (redis.ConnectionError, redis.TimeoutError) as exc:
            raise StoreUnavailableError(str(exc)) from exc
        return None if value is None else EventPosition(partition, int(value))

    def _card_key(self, card: CardId, suffix: str) -> str:
        """Return the key of one of card's structures: "ev", "tx", or "newest"."""
        return f"{self._prefix}card:{{{card.value}}}:{suffix}"

    def _position_key(self, partition: int) -> str:
        """Return the key that stores the last applied offset of partition."""
        return f"{self._prefix}pos:{self._topic}:{partition}"


def _lua(name: str) -> str:
    """Return the source of a Lua script shipped in the lua/ directory of this package."""
    return files("finspeq.adapters.outbound").joinpath("lua", name).read_text(encoding="utf-8")


def _text(value: bytes | str) -> str:
    """Return value decoded as UTF-8 if it is bytes."""
    return value.decode("utf-8") if isinstance(value, bytes) else value


def _entry(tx_id: str, value: str) -> WindowEntry:
    """Return the window entry stored as `time|cents|merchant` for tx_id."""
    time, cents, merchant = value.split(VALUE_SEPARATOR, 2)
    return WindowEntry(
        tx_id=TxId(tx_id), time=EventTime(int(time)), amount=Money(int(cents)), merchant=merchant
    )
