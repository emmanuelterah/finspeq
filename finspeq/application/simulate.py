"""ReplayHistory: publish historical transactions to the event log at a target rate."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass

from finspeq.application.disorder import Disorder, arrival_order
from finspeq.application.messages import LabeledTransaction, TimeRange
from finspeq.application.ports import BulkPublisher, Clock, HistoricalSource
from finspeq.application.replay import CardSlice
from finspeq.domain.model import Transaction


@dataclass(frozen=True, slots=True)
class SimulationPlan:
    """What to publish and how fast.

    Attributes:
        time_range: Event times to publish; None publishes everything.
        cards: Cards to publish.
        limit: Maximum number of distinct transactions; None for no limit.
        rate: Target arrivals per second; 0 publishes as fast as possible.
        chunk_size: Arrivals per publish call.
        disorder: Delays and duplicates applied after the card and limit filters.
    """

    time_range: TimeRange | None
    cards: CardSlice
    limit: int | None
    rate: float
    chunk_size: int
    disorder: Disorder


@dataclass(frozen=True, slots=True)
class SimulationReport:
    """Number of arrivals published (duplicates included), elapsed seconds, and achieved rate."""

    events: int
    chunks: int
    elapsed_s: float
    events_per_second: float


class ReplayHistory:
    """Publishes historical transactions in arrival order, in chunks, at a target rate."""

    def __init__(
        self,
        source: HistoricalSource,
        publisher: BulkPublisher,
        clock: Clock,
        sleep: Callable[[float], None],
    ) -> None:
        """Create the use case.

        Args:
            source: Historical transactions in canonical order.
            publisher: Event log writer; each chunk is acknowledged before the next.
            clock: Measures elapsed time for pacing.
            sleep: Blocks for the given number of seconds.
        """
        self._source = source
        self._publisher = publisher
        self._clock = clock
        self._sleep = sleep

    def execute(self, plan: SimulationPlan) -> SimulationReport:
        """Publish the plan's arrivals and return counts and the achieved rate.

        After each chunk, sleeps until `published / rate` seconds have passed since the start.
        """
        start = self._clock.now()
        chunk: list[Transaction] = []
        published = chunks = 0
        for item in arrival_order(self._selected(plan), plan.disorder):
            chunk.append(item.transaction)
            if len(chunk) == plan.chunk_size:
                published, chunks = self._publish(chunk, published, chunks, plan, start)
        if chunk:
            published, chunks = self._publish(chunk, published, chunks, plan, start)
        elapsed = self._clock.now() - start
        rate = published / elapsed if elapsed > 0 else 0.0
        return SimulationReport(published, chunks, elapsed, rate)

    def _selected(self, plan: SimulationPlan) -> Iterator[LabeledTransaction]:
        """Yield the source's transactions in plan's time range and slice, up to plan.limit."""
        count = 0
        for item in self._source.iter_labeled(plan.time_range):
            if plan.limit is not None and count >= plan.limit:
                return
            if plan.cards.contains(item.transaction.card):
                count += 1
                yield item

    def _publish(
        self,
        chunk: list[Transaction],
        published: int,
        chunks: int,
        plan: SimulationPlan,
        start: float,
    ) -> tuple[int, int]:
        """Publish chunk, clear it, sleep to keep the target rate, and return updated counts."""
        self._publisher.publish_raw_many(chunk)
        published += len(chunk)
        chunk.clear()
        if plan.rate > 0:
            wait = published / plan.rate - (self._clock.now() - start)
            if wait > 0:
                self._sleep(wait)
        return published, chunks + 1
