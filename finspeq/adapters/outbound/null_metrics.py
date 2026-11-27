"""Metrics implementation that discards every measurement."""

from __future__ import annotations

from collections.abc import Mapping

from finspeq.application.ports import Metrics


class NullMetrics(Metrics):
    """Discards every measurement."""

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        """Do nothing."""

    def set_gauge(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        """Do nothing."""

    def increment(
        self, name: str, amount: float = 1.0, labels: Mapping[str, str] | None = None
    ) -> None:
        """Do nothing."""
