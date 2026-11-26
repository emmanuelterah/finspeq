"""Seeded arrival model: delayed and duplicated events, and the resulting arrival order."""

from __future__ import annotations

import heapq
import random
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass

from finspeq.application.errors import InvalidMessageError
from finspeq.application.messages import LabeledTransaction


@dataclass(frozen=True, slots=True)
class Disorder:
    """How events arrive relative to their event time.

    Attributes:
        delayed_fraction: Probability that an event arrives after a delay.
        mean_delay_s: Mean of the exponential delay of a delayed event, in seconds.
        max_delay_s: Upper bound of every delay, in seconds.
        duplicate_fraction: Probability that an event arrives a second time, with its own delay.
        seed: Seed of every random draw.
    """

    delayed_fraction: float = 0.0
    mean_delay_s: float = 0.0
    max_delay_s: int = 0
    duplicate_fraction: float = 0.0
    seed: int = 0

    def __post_init__(self) -> None:
        """Raise InvalidMessageError for fractions outside [0, 1] or negative delays.

        Also raises when delayed_fraction is positive and mean_delay_s is not.
        """
        for name in ("delayed_fraction", "duplicate_fraction"):
            if not 0.0 <= getattr(self, name) <= 1.0:
                raise InvalidMessageError(f"{name} must be in [0, 1], got {self}")
        if self.mean_delay_s < 0 or self.max_delay_s < 0:
            raise InvalidMessageError(f"delays must be non-negative, got {self}")
        if self.delayed_fraction > 0 and self.mean_delay_s <= 0:
            raise InvalidMessageError("mean_delay_s must be positive when events are delayed")


NO_DISORDER = Disorder()


def arrival_order(
    items: Iterable[LabeledTransaction], disorder: Disorder
) -> Iterator[LabeledTransaction]:
    """Yield items, and their duplicates, in order of arrival time.

    Arrival time is event time plus a delay: 0 with probability 1 - delayed_fraction, otherwise
    an exponential draw with mean mean_delay_s, rounded to whole seconds and capped at
    max_delay_s. Equal arrival times keep input order. items must be in canonical order; at most
    the items arriving after the current event time are held in memory.
    """
    rng = random.Random(disorder.seed)
    pending: list[tuple[int, int, LabeledTransaction]] = []
    counter = 0
    for item in items:
        event_time = item.transaction.time.epoch_s
        copies = 2 if rng.random() < disorder.duplicate_fraction else 1
        for _ in range(copies):
            heapq.heappush(pending, (event_time + _delay(rng, disorder), counter, item))
            counter += 1
        while pending and pending[0][0] <= event_time:
            yield heapq.heappop(pending)[2]
    while pending:
        yield heapq.heappop(pending)[2]


def _delay(rng: random.Random, disorder: Disorder) -> int:
    """Return one delay in whole seconds drawn from rng under disorder."""
    if disorder.delayed_fraction == 0 or rng.random() >= disorder.delayed_fraction:
        return 0
    return min(disorder.max_delay_s, round(rng.expovariate(1.0 / disorder.mean_delay_s)))


def round_robin_consumption(
    items: Sequence[LabeledTransaction], partitions: int, batch_size: int
) -> list[LabeledTransaction]:
    """Return items in the order consumers apply them when published without a key.

    Item n goes to partition n % partitions, as a round-robin producer sends it. Consumers
    then take batch_size items from partition 0, then from partition 1, and so on, repeating
    until every partition is empty.
    """
    lanes = [list(items[lane::partitions]) for lane in range(partitions)]
    order: list[LabeledTransaction] = []
    start = 0
    while any(start < len(lane) for lane in lanes):
        for lane in lanes:
            order.extend(lane[start : start + batch_size])
        start += batch_size
    return order
