"""LatenessSweep: feature and decision error of out-of-order arrival per allowed lateness."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from finspeq.application.messages import Event, EventBatch, EventPosition, LabeledTransaction
from finspeq.application.ports import FraudModel, WindowStore
from finspeq.application.watermark import Watermark
from finspeq.domain.features import FeatureSet, FeatureVector
from finspeq.domain.model import CardId, TxId
from finspeq.domain.policy import Decision, DecisionPolicy
from finspeq.domain.window import WINDOW_SECONDS

FLAGGED = frozenset({Decision.REVIEW, Decision.BLOCK})


@dataclass(frozen=True, slots=True)
class Scoring:
    """A model and the policy that turns its scores into decisions."""

    model: FraudModel
    policy: DecisionPolicy

    def decide(self, vector: FeatureVector) -> Decision:
        """Return the policy's decision for the model's score of vector."""
        return self.policy.decide(self.model.predict(vector))


@dataclass(frozen=True, slots=True)
class DecisionRates:
    """Recall and false-positive rate of REVIEW-or-BLOCK and of BLOCK decisions."""

    flag_recall: float
    flag_fpr: float
    block_recall: float
    block_fpr: float


@dataclass(frozen=True, slots=True)
class LatenessPoint:
    """Outcome of processing the arrival order with one allowed lateness.

    Attributes:
        allowed_lateness_s: Seconds the store keeps entries beyond WINDOW_SECONDS.
        retention_factor: (WINDOW_SECONDS + allowed_lateness_s) / WINDOW_SECONDS.
        late: Scored events more than allowed_lateness_s behind the newest event time seen in
            the stream (the partition watermark).
        card_late: Scored events more than allowed_lateness_s behind the newest event time of
            their own card, the time the store prunes against.
        mismatched: Scored events whose feature vector differs from the in-order vector.
        mismatched_late: Mismatched events that were also late.
        mismatched_card_late: Mismatched events that were also card-late.
        feature_mismatches: Per feature name, the number of events whose value differs.
        decision_changes: Events whose decision differs from the in-order decision; 0 without
            a model.
        rates: Decision rates against the labels, or None without a model.
    """

    allowed_lateness_s: int
    retention_factor: float
    late: int
    card_late: int
    mismatched: int
    mismatched_late: int
    mismatched_card_late: int
    feature_mismatches: tuple[tuple[str, int], ...]
    decision_changes: int
    rates: DecisionRates | None


@dataclass(frozen=True, slots=True)
class LatenessReport:
    """Arrival statistics, the in-order reference rates, and one point per allowed lateness.

    Attributes:
        transactions: Distinct transactions in the input.
        arrivals: Arrivals including duplicates.
        duplicates: Arrivals of a transaction id that already arrived; not scored.
        out_of_order: Scored events behind the newest event time seen before them.
        max_behind_s: Largest distance behind the newest event time, in seconds.
        card_out_of_order: Scored events behind their own card's newest event time.
        card_max_behind_s: Largest distance behind a card's newest event time, in seconds.
        reference_rates: Decision rates of the in-order run, or None without a model.
        points: One entry per allowed lateness, in the requested order.
    """

    transactions: int
    arrivals: int
    duplicates: int
    out_of_order: int
    max_behind_s: int
    card_out_of_order: int
    card_max_behind_s: int
    reference_rates: DecisionRates | None
    points: tuple[LatenessPoint, ...]


class LatenessSweep:
    """Scores each transaction when it arrives and compares the result with in-order processing.

    Each arrival's window is read before the arrival is stored, the same way a scorer sees the
    store. The in-order reference uses a store with allowed lateness 0 fed in canonical order.
    """

    def __init__(
        self,
        store_for: Callable[[int], WindowStore],
        feature_set: FeatureSet,
        scoring: Scoring | None,
    ) -> None:
        """Create the use case.

        Args:
            store_for: Returns a new, empty store for an allowed lateness in seconds.
            feature_set: Features computed for every scored event.
            scoring: Model and policy used for decisions, or None to compare features only.
        """
        self._store_for = store_for
        self._feature_set = feature_set
        self._scoring = scoring

    def execute(
        self,
        items: Sequence[LabeledTransaction],
        arrivals: Sequence[LabeledTransaction],
        lateness_values: Sequence[int],
    ) -> LatenessReport:
        """Run items as the in-order reference, then arrivals once per allowed lateness.

        Args:
            items: Every transaction once, in canonical order.
            arrivals: The same transactions in arrival order, possibly with duplicates.
            lateness_values: Allowed lateness values to run, in seconds.

        A duplicate arrival is stored but not scored.
        """
        reference = self._vectors(items, self._store_for(0))
        labels = {item.transaction.tx_id: item.is_fraud for item in items}
        reference_decisions = self._decisions(reference)
        points = tuple(
            self._point(arrivals, lateness, reference, reference_decisions, labels)
            for lateness in lateness_values
        )
        stream = _Lateness(arrivals, 0)
        return LatenessReport(
            transactions=len(items),
            arrivals=len(arrivals),
            duplicates=len(arrivals) - len(items),
            out_of_order=len(stream.late),
            max_behind_s=stream.max_behind_s,
            card_out_of_order=len(stream.card_late),
            card_max_behind_s=stream.card_max_behind_s,
            reference_rates=_rates(reference_decisions, labels),
            points=points,
        )

    def _vectors(
        self, items: Sequence[LabeledTransaction], store: WindowStore
    ) -> dict[TxId, FeatureVector]:
        """Return the feature vector of each first arrival in items, read before it is stored."""
        vectors: dict[TxId, FeatureVector] = {}
        for offset, item in enumerate(items):
            tx = item.transaction
            if tx.tx_id not in vectors:
                window = store.read(tx.card, tx.order_key)
                vectors[tx.tx_id] = self._feature_set.compute(tx, window)
            event = Event(position=EventPosition(0, offset), transaction=tx)
            store.apply(EventBatch(partition=0, events=(event,)))
        return vectors

    def _decisions(self, vectors: dict[TxId, FeatureVector]) -> dict[TxId, Decision]:
        """Return the decision for each vector, or an empty map without a model."""
        if self._scoring is None:
            return {}
        return {tx_id: self._scoring.decide(vector) for tx_id, vector in vectors.items()}

    def _point(
        self,
        arrivals: Sequence[LabeledTransaction],
        lateness: int,
        reference: dict[TxId, FeatureVector],
        reference_decisions: dict[TxId, Decision],
        labels: dict[TxId, bool],
    ) -> LatenessPoint:
        """Process arrivals with a store for lateness and compare each vector with reference."""
        vectors = self._vectors(arrivals, self._store_for(lateness))
        lateness_sets = _Lateness(arrivals, lateness)
        names = self._feature_set.names
        per_feature = [0] * len(names)
        mismatched = mismatched_late = mismatched_card_late = 0
        for tx_id, vector in vectors.items():
            expected = reference[tx_id].values
            differing = [i for i, value in enumerate(vector.values) if value != expected[i]]
            for index in differing:
                per_feature[index] += 1
            if differing:
                mismatched += 1
                mismatched_late += tx_id in lateness_sets.late
                mismatched_card_late += tx_id in lateness_sets.card_late
        decisions = self._decisions(vectors)
        changes = sum(decisions[tx_id] != reference_decisions[tx_id] for tx_id in decisions)
        return LatenessPoint(
            allowed_lateness_s=lateness,
            retention_factor=(WINDOW_SECONDS + lateness) / WINDOW_SECONDS,
            late=len(lateness_sets.late),
            card_late=len(lateness_sets.card_late),
            mismatched=mismatched,
            mismatched_late=mismatched_late,
            mismatched_card_late=mismatched_card_late,
            feature_mismatches=tuple(zip(names, per_feature, strict=True)),
            decision_changes=changes,
            rates=_rates(decisions, labels),
        )


class _Lateness:
    """First arrivals beyond an allowed lateness, measured against the stream and per card.

    Attributes:
        late: Ids more than the lateness behind the newest event time of the stream.
        card_late: Ids more than the lateness behind the newest event time of their card.
        max_behind_s: Largest distance behind the stream's newest event time.
        card_max_behind_s: Largest distance behind a card's newest event time.
    """

    def __init__(self, arrivals: Sequence[LabeledTransaction], lateness: int) -> None:
        """Walk arrivals once and fill the attributes; repeated ids are measured, not counted."""
        stream = Watermark(lateness)
        cards: dict[CardId, Watermark] = {}
        seen: set[TxId] = set()
        self.late: set[TxId] = set()
        self.card_late: set[TxId] = set()
        self.max_behind_s = 0
        self.card_max_behind_s = 0
        for item in arrivals:
            tx = item.transaction
            behind = stream.observe(tx.time)
            card = cards.setdefault(tx.card, Watermark(lateness))
            card_behind = card.observe(tx.time)
            self.max_behind_s = max(self.max_behind_s, behind)
            self.card_max_behind_s = max(self.card_max_behind_s, card_behind)
            if tx.tx_id not in seen:
                if stream.is_late(behind):
                    self.late.add(tx.tx_id)
                if card.is_late(card_behind):
                    self.card_late.add(tx.tx_id)
            seen.add(tx.tx_id)


def _rates(decisions: dict[TxId, Decision], labels: dict[TxId, bool]) -> DecisionRates | None:
    """Return flag and block recall and FPR of decisions against labels, or None if empty."""
    if not decisions:
        return None
    frauds = sum(labels[tx_id] for tx_id in decisions)
    legits = len(decisions) - frauds
    counts = {"flag_tp": 0, "flag_fp": 0, "block_tp": 0, "block_fp": 0}
    for tx_id, decision in decisions.items():
        outcome = "tp" if labels[tx_id] else "fp"
        counts[f"flag_{outcome}"] += decision in FLAGGED
        counts[f"block_{outcome}"] += decision is Decision.BLOCK
    return DecisionRates(
        flag_recall=_ratio(counts["flag_tp"], frauds),
        flag_fpr=_ratio(counts["flag_fp"], legits),
        block_recall=_ratio(counts["block_tp"], frauds),
        block_fpr=_ratio(counts["block_fp"], legits),
    )


def _ratio(part: int, whole: int) -> float:
    """Return part / whole, or 0.0 when whole is 0."""
    return part / whole if whole else 0.0
