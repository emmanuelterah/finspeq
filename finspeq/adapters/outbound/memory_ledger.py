"""In-process implementation of DecisionLedger and DecisionReader."""

from __future__ import annotations

from collections.abc import Iterator

from finspeq.application.messages import ApplyResult, EventBatch, EventPosition, LedgerRow
from finspeq.application.ports import DecisionLedger, DecisionReader
from finspeq.application.positions import unapplied_events
from finspeq.domain.model import TxId


class MemoryLedger(DecisionLedger, DecisionReader):
    """Decision rows and partition positions held in process memory."""

    def __init__(self) -> None:
        """Create an empty ledger."""
        self._rows: dict[TxId, LedgerRow] = {}
        self._positions: dict[int, int] = {}

    def record(self, batch: EventBatch) -> ApplyResult:
        """Store one row per new scored event and the batch position as one step.

        Semantics match DecisionLedger.record.

        Raises:
            PositionGapError: The unapplied offsets do not continue from the stored position.
        """
        pending = unapplied_events(batch, self._positions.get(batch.partition))
        if not pending:
            return ApplyResult(0, len(batch.events), self.position(batch.partition), 0)
        rows: dict[TxId, LedgerRow] = {}
        for event in pending:
            tx, record = event.transaction, event.record
            if record is None or tx.tx_id in self._rows or tx.tx_id in rows:
                continue
            rows[tx.tx_id] = LedgerRow(
                tx.tx_id, tx.card.value, tx.time, tx.amount.cents, record, event.position
            )
        self._rows.update(rows)
        self._positions[batch.partition] = pending[-1].position.offset
        return ApplyResult(len(rows), len(batch.events) - len(rows), pending[-1].position, 0)

    def position(self, partition: int) -> EventPosition | None:
        """Return the last recorded position for partition, or None if nothing was recorded."""
        offset = self._positions.get(partition)
        return None if offset is None else EventPosition(partition, offset)

    def rows(self) -> Iterator[LedgerRow]:
        """Yield every recorded decision in tx_id order."""
        for tx_id in sorted(self._rows):
            yield self._rows[tx_id]
