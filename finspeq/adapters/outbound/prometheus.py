"""Metrics implementation backed by a private Prometheus client registry."""

from __future__ import annotations

from collections.abc import Mapping

from prometheus_client import CollectorRegistry, Counter, Histogram, start_http_server

from finspeq.application.ports import Metrics

LATENCY_BUCKETS_MS = (0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 1_000.0)
SECONDS_BUCKETS = (1.0, 10.0, 60.0, 300.0, 600.0, 1_800.0, 3_600.0, 21_600.0, 86_400.0)
SECONDS_SUFFIX = "_s"


class PrometheusMetrics(Metrics):
    """Histograms and counters in a private Prometheus registry, created on first use.

    A metric's label names are fixed by its first call. Histograms whose name ends in "_s" use
    SECONDS_BUCKETS; the others use LATENCY_BUCKETS_MS.
    """

    def __init__(self) -> None:
        """Create an empty registry."""
        self._registry = CollectorRegistry()
        self._histograms: dict[str, Histogram] = {}
        self._counters: dict[str, Counter] = {}

    def serve(self, port: int) -> None:
        """Expose the registry over HTTP at /metrics on port, from a background thread."""
        start_http_server(port, registry=self._registry)

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        """Record value in the histogram `name`."""
        labels = labels or {}
        histogram = self._histograms.get(name)
        if histogram is None:
            buckets = SECONDS_BUCKETS if name.endswith(SECONDS_SUFFIX) else LATENCY_BUCKETS_MS
            histogram = Histogram(
                name, name, sorted(labels), registry=self._registry, buckets=buckets
            )
            self._histograms[name] = histogram
        (histogram.labels(**labels) if labels else histogram).observe(value)

    def increment(
        self, name: str, amount: float = 1.0, labels: Mapping[str, str] | None = None
    ) -> None:
        """Add amount to the counter `name`."""
        labels = labels or {}
        counter = self._counters.get(name)
        if counter is None:
            counter = Counter(name, name, sorted(labels), registry=self._registry)
            self._counters[name] = counter
        (counter.labels(**labels) if labels else counter).inc(amount)
