"""ScoreTransaction: the hot path from a transaction to a logged decision."""

from __future__ import annotations

from dataclasses import dataclass

from finspeq.application.messages import EventPosition
from finspeq.application.ports import (
    Clock,
    EventPublisher,
    FraudModel,
    Metrics,
    WindowAppender,
    WindowReader,
)
from finspeq.domain.features import FeatureSet
from finspeq.domain.model import Transaction
from finspeq.domain.policy import DecisionPolicy
from finspeq.domain.scored import DecisionRecord, ScoredTransaction

MILLISECONDS_PER_SECOND = 1_000.0


@dataclass(frozen=True, slots=True)
class ScoringDeps:
    """What scoring reads and writes: windows, features, model, thresholds, and the log."""

    reader: WindowReader
    feature_set: FeatureSet
    model: FraudModel
    policy: DecisionPolicy
    publisher: EventPublisher


@dataclass(frozen=True, slots=True)
class ScoreOutcome:
    """A scored transaction, where its event was logged, and how the call went.

    Attributes:
        scored: The transaction and its decision record.
        position: Where the scored event was logged.
        window_entries: Entries in the window the features were computed from.
        step_ms: Milliseconds per step, in order: read, features, predict, publish, and
            append when read-your-writes is on.
    """

    scored: ScoredTransaction
    position: EventPosition
    window_entries: int
    step_ms: tuple[tuple[str, float], ...]


class ScoreTransaction:
    """Reads a card's window, scores the transaction, and logs the decision before returning.

    No decision is returned unless its event is in the log. With an appender, the event is
    also written to the window store after the log acknowledges it (read-your-writes).
    """

    def __init__(
        self,
        deps: ScoringDeps,
        metrics: Metrics,
        clock: Clock,
        appender: WindowAppender | None = None,
    ) -> None:
        """Create the use case.

        Args:
            deps: Window reader, feature set, model, policy, and publisher.
            metrics: Receives fq_step_ms (label step: read, features, predict, publish,
                append), fq_window_entries, and fq_decisions_total (label decision).
            clock: Times each step.
            appender: Writes the scored event to the window store after the log ack; None
                leaves the store to the feature worker alone.
        """
        self._deps = deps
        self._metrics = metrics
        self._clock = clock
        self._appender = appender

    def execute(self, tx: Transaction) -> ScoreOutcome:
        """Score tx, publish it with its decision record, and return the outcome.

        Raises:
            StoreUnavailableError: The window store did not respond.
            SchemaMismatchError: The model expects another feature set.
            PublishFailedError: The broker did not acknowledge the event; nothing is returned
                and nothing is appended.
        """
        deps = self._deps
        steps: list[tuple[str, float]] = []
        start = self._clock.now()
        window = deps.reader.read(tx.card, tx.order_key)
        read_done = self._step("read", start, steps)
        vector = deps.feature_set.compute(tx, window)
        features_done = self._step("features", read_done, steps)
        score = deps.model.predict(vector)
        decision = deps.policy.decide(score)
        predict_done = self._step("predict", features_done, steps)
        record = DecisionRecord(vector, score, decision, deps.model.version)
        scored = ScoredTransaction(tx, record)
        position = deps.publisher.publish_scored(scored)
        publish_done = self._step("publish", predict_done, steps)
        if self._appender is not None:
            self._appender.append(tx)
            self._step("append", publish_done, steps)
        self._metrics.observe("fq_window_entries", float(len(window.entries)))
        self._metrics.increment("fq_decisions_total", 1.0, {"decision": decision.value})
        return ScoreOutcome(scored, position, len(window.entries), tuple(steps))

    def _step(self, name: str, started: float, steps: list[tuple[str, float]]) -> float:
        """Record the milliseconds since started as step `name` in metrics and steps.

        Returns:
            The current time, the start of the next step.
        """
        now = self._clock.now()
        elapsed_ms = (now - started) * MILLISECONDS_PER_SECOND
        self._metrics.observe("fq_step_ms", elapsed_ms, {"step": name})
        steps.append((name, elapsed_ms))
        return now
