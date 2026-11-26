"""Tests for ReplayHistory pacing and VerifyFinalState."""

from __future__ import annotations

from collections.abc import Sequence

from finspeq.adapters.outbound.memory_history import MemoryHistoricalSource
from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.application.disorder import NO_DISORDER, Disorder, arrival_order
from finspeq.application.messages import EventPosition, LabeledTransaction
from finspeq.application.replay import ALL_CARDS, CardSlice, card_bucket, partitioned_batches
from finspeq.application.simulate import ReplayHistory, SimulationPlan
from finspeq.application.verify import VerifyFinalState
from finspeq.domain.model import CardId, Transaction
from tests.factories import BASE_TIME, OTHER_CARD, make_tx


class ListPublisher:
    """BulkPublisher that records published chunks and returns sequential positions."""

    def __init__(self) -> None:
        """Create a publisher with no chunks."""
        self.chunks: list[list[Transaction]] = []

    def publish_raw_many(self, txs: Sequence[Transaction]) -> tuple[EventPosition, ...]:
        """Record txs and return positions continuing from the previous chunk."""
        start = sum(len(c) for c in self.chunks)
        self.chunks.append(list(txs))
        return tuple(EventPosition(0, start + i) for i in range(len(txs)))


class ManualClock:
    """Clock that only moves when sleep is called."""

    def __init__(self) -> None:
        """Start at time zero with no sleeps."""
        self.time = 0.0
        self.sleeps: list[float] = []

    def now(self) -> float:
        """Return the current time."""
        return self.time

    def sleep(self, seconds: float) -> None:
        """Record seconds and advance the time by them."""
        self.sleeps.append(seconds)
        self.time += seconds


def history(count: int) -> list[LabeledTransaction]:
    """Return count transactions alternating between two cards."""
    return [
        LabeledTransaction(
            i,
            make_tx(i + 1, BASE_TIME + i, card=OTHER_CARD if i % 2 else "4000000000000002"),
            is_fraud=False,
        )
        for i in range(count)
    ]


def test_replay_paces_chunks_to_the_target_rate() -> None:
    """Sleep 2 s after each 10-transaction chunk at 5 transactions per second."""
    clock, publisher = ManualClock(), ListPublisher()
    use_case = ReplayHistory(MemoryHistoricalSource(history(30)), publisher, clock, clock.sleep)
    report = use_case.execute(SimulationPlan(None, ALL_CARDS, None, 5.0, 10, NO_DISORDER))
    assert [len(c) for c in publisher.chunks] == [10, 10, 10]
    assert clock.sleeps == [2.0, 2.0, 2.0]
    assert (report.events, report.chunks, report.events_per_second) == (30, 3, 5.0)


def test_replay_applies_slice_and_limit() -> None:
    """Publish only the slice's cards and stop at the limit."""
    first, other = CardId("4000000000000002"), CardId(OTHER_CARD)
    modulus = next(m for m in range(2, 50) if card_bucket(first, m) != card_bucket(other, m))
    cards = CardSlice(modulus, card_bucket(other, modulus))
    wanted = [i for i in history(40) if cards.contains(i.transaction.card)]
    assert 7 < len(wanted) < 40
    publisher = ListPublisher()
    clock = ManualClock()
    use_case = ReplayHistory(MemoryHistoricalSource(history(40)), publisher, clock, clock.sleep)
    report = use_case.execute(SimulationPlan(None, cards, 7, 0.0, 3, NO_DISORDER))
    published = [tx for chunk in publisher.chunks for tx in chunk]
    assert published == [i.transaction for i in wanted][:7]
    assert report.events == 7
    assert clock.sleeps == []


def test_final_state_matches_a_store_filled_the_same_way() -> None:
    """Pass when the candidate received every event on two partitions."""
    items = history(50)
    candidate = MemoryWindowStore()
    for batch in partitioned_batches(items, partitions=2, batch_size=8):
        candidate.apply(batch)
    report = VerifyFinalState(MemoryWindowStore(), candidate, partitions=2).execute(items, 10)
    assert report.passed
    assert (report.events, report.cards, report.candidate_applied_offsets) == (50, 2, 50)


def test_final_state_detects_missing_events() -> None:
    """Fail when the candidate missed the last events of one card."""
    items = history(50)
    candidate = MemoryWindowStore()
    for batch in partitioned_batches(items[:40], partitions=1, batch_size=8):
        candidate.apply(batch)
    report = VerifyFinalState(MemoryWindowStore(), candidate, partitions=1).execute(items, 10)
    assert not report.passed
    assert report.window_mismatches == 2


def test_replay_publishes_the_seeded_arrival_order() -> None:
    """With disorder, the published transactions are the arrival order, duplicates included."""
    disorder = Disorder(0.5, 30.0, 300, 0.2, seed=3)
    items = history(60)
    publisher, clock = ListPublisher(), ManualClock()
    use_case = ReplayHistory(MemoryHistoricalSource(items), publisher, clock, clock.sleep)
    report = use_case.execute(SimulationPlan(None, ALL_CARDS, None, 0.0, 7, disorder))
    published = [tx for chunk in publisher.chunks for tx in chunk]
    assert published == [i.transaction for i in arrival_order(items, disorder)]
    assert report.events == len(published) > len(items)


def test_final_state_is_independent_of_batching_under_disorder() -> None:
    """A store filled with the arrival order in other batch sizes matches the reference.

    Events are 30 minutes apart over about four days, so late events and pruning both occur.
    """
    disorder = Disorder(0.5, 3_600.0, 14_400, 0.2, seed=5)
    items = [
        LabeledTransaction(i, make_tx(i + 1, BASE_TIME + i * 1_800, card=card), is_fraud=False)
        for i, card in enumerate(["4000000000000002", OTHER_CARD] * 100)
    ]
    arrivals = list(arrival_order(items, disorder))
    assert arrivals != items
    candidate = MemoryWindowStore(allowed_lateness_s=60)
    for batch in partitioned_batches(arrivals, partitions=1, batch_size=13):
        candidate.apply(batch)
    reference = MemoryWindowStore(allowed_lateness_s=60)
    report = VerifyFinalState(reference, candidate, partitions=1).execute(arrivals, 4)
    assert report.passed
    assert report.events == len(arrivals)
