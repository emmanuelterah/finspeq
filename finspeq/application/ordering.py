"""Enforcement of canonical event order for historical sources."""

from __future__ import annotations

from finspeq.application.errors import InvalidSourceDataError
from finspeq.application.messages import LabeledTransaction
from finspeq.domain.model import OrderKey


class CanonicalOrderGuard:
    """Checks that successive labelled transactions strictly increase in OrderKey and seq."""

    def __init__(self) -> None:
        """Create a guard that has seen no transactions."""
        self._previous_key: OrderKey | None = None
        self._previous_seq: int | None = None

    def check(self, item: LabeledTransaction) -> LabeledTransaction:
        """Return item if it follows the previous one in canonical order.

        Raises:
            InvalidSourceDataError: item's order key or seq is not above the previous item's.
        """
        key = item.transaction.order_key
        if self._previous_key is not None and key <= self._previous_key:
            raise InvalidSourceDataError(f"seq {item.seq}: order key is not increasing")
        if self._previous_seq is not None and item.seq <= self._previous_seq:
            raise InvalidSourceDataError(f"seq {item.seq}: seq is not increasing")
        self._previous_key = key
        self._previous_seq = item.seq
        return item
