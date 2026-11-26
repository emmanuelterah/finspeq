"""VerifyReplay: check that a candidate window store behaves exactly like a reference store."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from finspeq.application.messages import EventBatch, LabeledTransaction
from finspeq.application.ports import WindowStore
from finspeq.application.replay import partitioned_batches
from finspeq.domain.model import CardId, EventTime, OrderKey, TxId

LOWEST_TX_ID = TxId("0" * 32)


@dataclass(frozen=True, slots=True)
class ReplayScenario:
    """Deterministic delivery faults: every `redeliver_every`-th batch is applied twice."""

    redeliver_every: int = 0


@dataclass(frozen=True, slots=True)
class ReplayReport:
    """Counts of what was compared and of every disagreement found."""

    batches: int
    deliveries: int
    events: int
    window_reads: int
    window_mismatches: int
    result_mismatches: int
    position_mismatches: int
    first_mismatch: str | None

    @property
    def passed(self) -> bool:
        """Return True if no comparison disagreed."""
        return self.first_mismatch is None


class VerifyReplay:
    """Applies the same batches to two stores and compares windows, results, and positions."""

    def __init__(self, reference: WindowStore, candidate: WindowStore) -> None:
        """Create the use case for an empty reference store and an empty candidate store."""
        self._reference = reference
        self._candidate = candidate

    def execute(self, batches: Iterable[EventBatch], scenario: ReplayScenario) -> ReplayReport:
        """Deliver every batch to both stores and return the comparison report.

        Before each delivery, reads both stores' windows at every event's order key; equal
        windows give equal feature vectors because both paths use the same FeatureSet. After
        each delivery, compares the apply results and the partition positions.
        """
        tally = _Tally()
        for index, batch in enumerate(batches, start=1):
            tally.batches += 1
            redeliver = scenario.redeliver_every > 0 and index % scenario.redeliver_every == 0
            for _ in range(2 if redeliver else 1):
                self._deliver(batch, tally)
        return tally.report()

    def _deliver(self, batch: EventBatch, tally: _Tally) -> None:
        """Compare windows before batch, apply it to both stores, and compare the outcome."""
        tally.deliveries += 1
        for event in batch.events:
            tx = event.transaction
            tally.window_reads += 1
            expected = self._reference.read(tx.card, tx.order_key)
            actual = self._candidate.read(tx.card, tx.order_key)
            if expected != actual:
                tally.window_mismatches += 1
                tally.note(f"window of {tx.tx_id.value} at offset {event.position.offset}")
        tally.events += len(batch.events)
        expected_result = self._reference.apply(batch)
        actual_result = self._candidate.apply(batch)
        if expected_result != actual_result:
            tally.result_mismatches += 1
            tally.note(f"apply result {actual_result} != {expected_result}")
        if self._reference.position(batch.partition) != self._candidate.position(batch.partition):
            tally.position_mismatches += 1
            tally.note(f"position of partition {batch.partition}")


class _Tally:
    """Mutable counters of one replay run."""

    def __init__(self) -> None:
        """Create counters at zero and no recorded mismatch."""
        self.batches = 0
        self.deliveries = 0
        self.events = 0
        self.window_reads = 0
        self.window_mismatches = 0
        self.result_mismatches = 0
        self.position_mismatches = 0
        self.first_mismatch: str | None = None

    def note(self, description: str) -> None:
        """Record description if it is the first mismatch."""
        if self.first_mismatch is None:
            self.first_mismatch = description

    def report(self) -> ReplayReport:
        """Return the counters as a ReplayReport."""
        return ReplayReport(
            batches=self.batches,
            deliveries=self.deliveries,
            events=self.events,
            window_reads=self.window_reads,
            window_mismatches=self.window_mismatches,
            result_mismatches=self.result_mismatches,
            position_mismatches=self.position_mismatches,
            first_mismatch=self.first_mismatch,
        )


@dataclass(frozen=True, slots=True)
class StateReport:
    """Comparison of a candidate store's final state with a reference replay."""

    events: int
    cards: int
    window_mismatches: int
    candidate_applied_offsets: int
    first_mismatch: str | None

    @property
    def passed(self) -> bool:
        """Return True if every window matched and the offsets cover every event."""
        return self.first_mismatch is None and self.candidate_applied_offsets == self.events


class VerifyFinalState:
    """Compares a store filled by a real worker with an in-process reference replay."""

    def __init__(self, reference: WindowStore, candidate: WindowStore, partitions: int) -> None:
        """Create the use case.

        Args:
            reference: Empty store that receives the replay.
            candidate: Store already filled by the worker from a fresh topic.
            partitions: Number of topic partitions; positions 0..partitions-1 are summed.
        """
        self._reference = reference
        self._candidate = candidate
        self._partitions = partitions

    def execute(self, items: Iterable[LabeledTransaction], batch_size: int) -> StateReport:
        """Replay items into the reference, then compare every card's final window.

        Each card is probed just after its last event time. candidate_applied_offsets is the
        sum of (stored offset + 1) over the candidate's partitions, which equals the number of
        events when the topic held only these events.
        """
        last_time: dict[CardId, int] = {}
        events = 0
        replay = (self._track(item, last_time) for item in items)
        for batch in partitioned_batches(replay, partitions=1, batch_size=batch_size):
            self._reference.apply(batch)
            events += len(batch.events)
        mismatches = 0
        first: str | None = None
        for card in sorted(last_time, key=lambda c: c.value):
            probe = OrderKey(EventTime(last_time[card] + 1), LOWEST_TX_ID)
            if self._reference.read(card, probe) != self._candidate.read(card, probe):
                mismatches += 1
                first = first or f"window of card ending {card.value[-4:]}"
        applied = 0
        for partition in range(self._partitions):
            position = self._candidate.position(partition)
            applied += 0 if position is None else position.offset + 1
        return StateReport(events, len(last_time), mismatches, applied, first)

    @staticmethod
    def _track(item: LabeledTransaction, last_time: dict[CardId, int]) -> LabeledTransaction:
        """Record item's event time as its card's latest and return item unchanged."""
        tx = item.transaction
        last_time[tx.card] = max(last_time.get(tx.card, 0), tx.time.epoch_s)
        return item
