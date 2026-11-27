"""Tests for the shadow comparison."""

from __future__ import annotations

import pytest

from finspeq.application.messages import EventPosition, LedgerRow
from finspeq.application.shadow import CompareShadow
from finspeq.domain.features import FeatureVector
from finspeq.domain.model import EventTime, TxId
from finspeq.domain.policy import Decision, Score
from finspeq.domain.scored import DecisionRecord, ShadowDecision
from tests.factories import tx_id

A, R, B = Decision.APPROVE, Decision.REVIEW, Decision.BLOCK


def row(n: int, live: Decision, shadow: Decision | None) -> LedgerRow:
    """Return ledger row n with the live and shadow decisions given."""
    verdict = None if shadow is None else ShadowDecision(Score(0.5), shadow, "shadow")
    record = DecisionRecord(FeatureVector("v2", (1.0,)), Score(0.5), live, "live", verdict)
    return LedgerRow(tx_id(n), "4000000000000002", EventTime(n), 100, record, EventPosition(0, n))


def test_shadow_report_compares_decisions_and_rates() -> None:
    """Count decision pairs, per-model rates, and frauds caught by only one model."""
    rows = [row(1, A, R), row(2, R, R), row(3, B, A), row(4, A, A), row(5, A, None)]
    labels: dict[TxId, bool] = {tx_id(n): n in (1, 3) for n in range(1, 6)}
    report = CompareShadow().execute(rows, labels)
    assert (report.rows, report.frauds) == (4, 2)
    assert report.agreement == {
        "APPROVE->APPROVE": 1,
        "APPROVE->REVIEW": 1,
        "BLOCK->APPROVE": 1,
        "REVIEW->REVIEW": 1,
    }
    assert (report.live.flag_recall, report.shadow.flag_recall) == (0.5, 0.5)
    assert report.live.block_recall == 0.5
    assert report.shadow.flag_fpr == pytest.approx(0.5)
    assert (report.frauds_only_shadow_flags, report.frauds_only_live_flags) == (1, 1)
    assert (report.live.model_version, report.shadow.model_version) == ("live", "shadow")
