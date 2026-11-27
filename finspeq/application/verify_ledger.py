"""VerifyLedger: compare the decision ledger with the scored events in the log."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from finspeq.application.messages import Event
from finspeq.application.ports import DecisionLedger, DecisionReader
from finspeq.domain.model import TxId


@dataclass(frozen=True, slots=True)
class LedgerReport:
    """Ledger rows against the log's scored events.

    Attributes:
        log_events: Events read from the log.
        scored_transactions: Distinct transaction ids with a decision record in the log.
        rows: Rows in the ledger.
        missing: Scored transactions in the log with no row.
        extra: Rows whose transaction has no scored event in the log.
        wrong_position: Rows not at the first log position of their transaction.
        positions_behind: Partitions whose stored position is before the partition's last event.
    """

    log_events: int
    scored_transactions: int
    rows: int
    missing: int
    extra: int
    wrong_position: int
    positions_behind: int

    @property
    def passed(self) -> bool:
        """Return True if every scored transaction has exactly its first-position row."""
        checks = (self.missing, self.extra, self.wrong_position, self.positions_behind)
        return all(count == 0 for count in checks)


class VerifyLedger:
    """Checks that the ledger holds one row per scored transaction, at its first position."""

    def __init__(self, ledger: DecisionLedger, reader: DecisionReader) -> None:
        """Create the use case over the ledger's positions and rows."""
        self._ledger = ledger
        self._reader = reader

    def execute(self, events: Iterable[Event]) -> LedgerReport:
        """Read events (the whole log, in offset order per partition) and compare.

        A transaction logged twice is expected at the position of its first scored event.
        """
        first_position: dict[TxId, tuple[int, int]] = {}
        last_offset: dict[int, int] = {}
        count = 0
        for event in events:
            count += 1
            position = event.position
            last_offset[position.partition] = position.offset
            if event.record is not None:
                key = (position.partition, position.offset)
                first_position.setdefault(event.transaction.tx_id, key)
        rows = {row.tx_id: row for row in self._reader.rows()}
        missing = sum(1 for tx_id in first_position if tx_id not in rows)
        extra = sum(1 for tx_id in rows if tx_id not in first_position)
        wrong = sum(
            1
            for tx_id, row in rows.items()
            if tx_id in first_position
            and (row.position.partition, row.position.offset) != first_position[tx_id]
        )
        behind = 0
        for partition, offset in last_offset.items():
            stored = self._ledger.position(partition)
            if stored is None or stored.offset < offset:
                behind += 1
        return LedgerReport(count, len(first_position), len(rows), missing, extra, wrong, behind)
