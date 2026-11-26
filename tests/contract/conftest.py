"""Fixtures that provide every WindowStore adapter to the contract suite."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

import pytest

from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.application.ports import WindowReader, WindowWriter


class WindowStore(WindowReader, WindowWriter, Protocol):
    """An adapter that implements both window ports."""


StoreFactory = Callable[[int], WindowStore]


def _memory_store(allowed_lateness_s: int) -> WindowStore:
    """Return a new MemoryWindowStore with the given allowed lateness."""
    return MemoryWindowStore(allowed_lateness_s=allowed_lateness_s)


@pytest.fixture(params=[_memory_store], ids=["memory"])
def make_store(request: pytest.FixtureRequest) -> StoreFactory:
    """Return a factory that creates an empty store for the parametrized adapter."""
    factory: StoreFactory = request.param
    return factory
