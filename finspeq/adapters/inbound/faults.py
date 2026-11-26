"""Deterministic crash injection at fixed points of the worker loop, for recovery drills."""

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

    A plan with point None never crashes.
    """

    point: CrashPoint | None = None
    probability: float = 0.0
    seed: int = 0


NO_FAULTS = FaultPlan()


class FaultInjector:
    """Calls `crash` at a plan's checkpoint with the plan's probability."""

    def __init__(self, plan: FaultPlan, crash: Callable[[], None]) -> None:
        """Create an injector; crash is called to end the process (for example os._exit)."""
        self._plan = plan
        self._crash = crash
        self._random = random.Random(plan.seed)
        self.checkpoints = 0

    def checkpoint(self, point: CrashPoint) -> None:
        """Count the checkpoint and call crash if point is the plan's point and the draw hits.

        One random number is drawn per checkpoint at the plan's point, so the same seed gives
        the same crash sequence.
        """
        if point is not self._plan.point:
            return
        self.checkpoints += 1
        if self._random.random() < self._plan.probability:
            self._crash()
