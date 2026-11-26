"""Clock implementation backed by the operating system's monotonic counter."""

from __future__ import annotations

import time

from finspeq.application.ports import Clock


class SystemClock(Clock):
    """Monotonic time from time.perf_counter."""

    def now(self) -> float:
        """Return time.perf_counter() in seconds."""
        return time.perf_counter()
