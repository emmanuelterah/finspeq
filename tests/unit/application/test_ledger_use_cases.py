"""Tests for RecordDecisions and VerifyLedger with the memory ledger."""

from __future__ import annotations

from finspeq.adapters.outbound.memory_ledger import MemoryLedger
from finspeq.adapters.outbound.null_metrics import NullMetrics
from finspeq.application.messages import Event, EventPosition
from finspeq.application.record_decisions import RecordDecisions
from finspeq.application.verify_ledger import VerifyLedger
from tests.contract.test_decision_ledger import record_for, scored_batch
from tests.factories import make_tx
from tests.unit.application.test_score import TickClock


def log() -> list[Event]:
    """Return a log of four events: two scored, one raw, and a repeat of the first scored."""
    first, second, raw = make_tx(1), make_tx(2), make_tx(3)
    return [
        Event(EventPosition(0, 0), first, record_for(first)),
        Event(EventPosition(0, 1), raw, None),
        Event(EventPosition(0, 2), second, record_for(second)),
        Event(EventPosition(0, 3), first, record_for(first)),
    ]


def test_recorded_log_passes_verification() -> None:
    """Record the whole log and find every scored transaction at its first position."""
    ledger = MemoryLedger()
    use_case = RecordDecisions(ledger, NullMetrics(), TickClock())
    events = log()
    result = use_case.execute(scored_batch((0, make_tx(1), True)))
    assert result.applied == 1
    rest = scored_batch((1, make_tx(3), False), (2, make_tx(2), True), (3, make_tx(1), True))
    use_case.execute(rest)
    report = VerifyLedger(ledger, ledger).execute(events)
    assert (report.log_events, report.scored_transactions, report.rows) == (4, 2, 2)
    assert report.passed


def test_missing_rows_and_lagging_positions_fail() -> None:
    """Report a scored transaction without a row and a partition whose position lags."""
    ledger = MemoryLedger()
    ledger.record(scored_batch((0, make_tx(1), True), (1, make_tx(3), False)))
    report = VerifyLedger(ledger, ledger).execute(log())
    assert (report.missing, report.extra, report.positions_behind) == (1, 0, 1)
    assert not report.passed
