"""Deterministic crash and stall injection at fixed points of the worker loop, for drills."""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum


class CrashPoint(Enum):
    """Point in the worker loop where a crash can be injected."""

    AFTER_POLL = "after-poll"
    AFTER_APPLY = "after-apply"
    AFTER_ACK = "after-ack"


@dataclass(frozen=True, slots=True)
class FaultPlan:
    """Crash at `point` with `probability` per checkpoint, drawing from Random(seed).

    A plan with point None never crashes. With stall_s > 0, the worker also sleeps stall_s
    seconds once, after the poll of batch number stall_at_batch (counted from 1), before
    applying it.
    """

    point: CrashPoint | None = None
    probability: float = 0.0
    seed: int = 0
    stall_s: float = 0.0
    stall_at_batch: int = 1


NO_FAULTS = FaultPlan()


class FaultInjector:
    """Calls `crash` at a plan's checkpoint with the plan's probability, and `sleep` once."""

    def __init__(
        self, plan: FaultPlan, crash: Callable[[], None], sleep: Callable[[float], None]
    ) -> None:
        """Create an injector.

        Args:
            plan: Where and how often to crash, and when to stall.
            crash: Ends the process (for example os._exit).
            sleep: Blocks for the given number of seconds.
        """
        self._plan = plan
        self._crash = crash
        self._sleep = sleep
        self._random = random.Random(plan.seed)
        self._polls = 0
        self.checkpoints = 0

    def checkpoint(self, point: CrashPoint) -> None:
        """Stall or crash as the plan says for point.

        At AFTER_POLL the batch counter advances and the one stall happens when it reaches
        stall_at_batch. If point is the plan's crash point, one random number is drawn and
        crash is called when it is below the probability, so the same seed gives the same
        crash sequence.
        """
        if point is CrashPoint.AFTER_POLL:
            self._polls += 1
            if self._plan.stall_s > 0 and self._polls == self._plan.stall_at_batch:
                self._sleep(self._plan.stall_s)
        if point is not self._plan.point:
            return
        self.checkpoints += 1
        if self._random.random() < self._plan.probability:
            self._crash()
