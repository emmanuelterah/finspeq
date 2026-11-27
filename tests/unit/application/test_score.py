"""Tests for the ScoreTransaction use case."""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.application.errors import PublishFailedError
from finspeq.application.messages import EventPosition
from finspeq.application.score import ScoreTransaction, ScoringDeps
from finspeq.domain.features import FEATURE_SET_V2, FeatureVector
from finspeq.domain.model import Transaction
from finspeq.domain.policy import Decision, DecisionPolicy, Score, Thresholds
from finspeq.domain.scored import ScoredTransaction
from tests.factories import BASE_TIME, batch, make_tx


class CountModel:
    """FraudModel whose score is 0.2 per transaction in the last 24 h, capped at 1."""

    version = "m-test"
    schema = "v2"

    def predict(self, vector: FeatureVector) -> Score:
        """Return min(1, 0.2 * tx_count_24h)."""
        return Score(min(1.0, 0.2 * vector.values[2]))


class ListPublisher:
    """EventPublisher that records scored events, or fails every call when fail is True."""

    def __init__(self, fail: bool = False) -> None:
        """Create a publisher with no events."""
        self.fail = fail
        self.scored: list[ScoredTransaction] = []

    def publish_raw(self, tx: Transaction) -> EventPosition:
        """Fail: the hot path publishes scored events only."""
        raise AssertionError(f"unexpected raw publish of {tx.tx_id}")

    def publish_scored(self, scored: ScoredTransaction) -> EventPosition:
        """Record scored and return the next offset, or raise PublishFailedError."""
        if self.fail:
            raise PublishFailedError("broker down")
        self.scored.append(scored)
        return EventPosition(0, len(self.scored) - 1)


class RecordingMetrics:
    """Metrics port that keeps every histogram sample and counter by name and labels."""

    def __init__(self) -> None:
        """Create empty records."""
        self.samples: list[tuple[str, str]] = []
        self.counters: dict[str, float] = {}

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        """Record the name and step label of a sample; check the value is non-negative."""
        assert value >= 0
        self.samples.append((name, (labels or {}).get("step", "")))

    def increment(
        self, name: str, amount: float = 1.0, labels: Mapping[str, str] | None = None
    ) -> None:
        """Add amount to the counter named name and decision label."""
        key = f"{name}:{(labels or {}).get('decision', '')}"
        self.counters[key] = self.counters.get(key, 0.0) + amount


class TickClock:
    """Clock that advances 1 ms per call."""

    def __init__(self) -> None:
        """Start at zero."""
        self._now = 0.0

    def now(self) -> float:
        """Return the time and advance it by 1 ms."""
        self._now += 0.001
        return self._now


def scorer(
    store: MemoryWindowStore, publisher: ListPublisher, append: bool
) -> tuple[ScoreTransaction, RecordingMetrics]:
    """Return a ScoreTransaction over store and publisher, and its metrics."""
    deps = ScoringDeps(
        store, FEATURE_SET_V2, CountModel(), DecisionPolicy(Thresholds(0.3, 0.5)), publisher
    )
    metrics = RecordingMetrics()
    use_case = ScoreTransaction(deps, metrics, TickClock(), store if append else None)
    return use_case, metrics


def test_scores_from_the_window_and_logs_the_decision() -> None:
    """Use the stored window, publish the decision record, and time every step."""
    store = MemoryWindowStore()
    store.apply(batch((0, make_tx(1, BASE_TIME - 60)), (1, make_tx(2, BASE_TIME - 30))))
    publisher = ListPublisher()
    use_case, metrics = scorer(store, publisher, append=False)
    outcome = use_case.execute(make_tx(3, BASE_TIME))
    assert outcome.window_entries == 2
    assert outcome.scored.record.decision is Decision.REVIEW
    assert outcome.scored.record.model_version == "m-test"
    assert publisher.scored == [outcome.scored]
    assert outcome.position == EventPosition(0, 0)
    steps = [step for name, step in metrics.samples if name == "fq_step_ms"]
    assert steps == ["read", "features", "predict", "publish"]
    assert [name for name, _ in outcome.step_ms] == steps
    assert all(abs(ms - 1.0) < 1e-6 for _, ms in outcome.step_ms)
    assert metrics.counters == {"fq_decisions_total:REVIEW": 1.0}


def test_read_your_writes_appends_after_the_log_ack() -> None:
    """With an appender, the next transaction of the card sees the previous one at once."""
    store = MemoryWindowStore()
    use_case, metrics = scorer(store, ListPublisher(), append=True)
    first = use_case.execute(make_tx(1, BASE_TIME))
    second = use_case.execute(make_tx(2, BASE_TIME + 1))
    assert (first.window_entries, second.window_entries) == (0, 1)
    assert ("fq_step_ms", "append") in metrics.samples
    assert store.position(0) is None


def test_failed_publish_returns_nothing_and_appends_nothing() -> None:
    """Raise PublishFailedError and leave the window store unchanged."""
    store = MemoryWindowStore()
    use_case, _ = scorer(store, ListPublisher(fail=True), append=True)
    with pytest.raises(PublishFailedError):
        use_case.execute(make_tx(1, BASE_TIME))
    probe = make_tx(9, BASE_TIME + 10)
    assert store.read(probe.card, probe.order_key).entries == ()
