"""Tests for the scoring API routes and error mapping, with in-memory adapters."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from finspeq.adapters.inbound.http_api import ApiDeps, create_api, error_code
from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.application.errors import StoreUnavailableError
from finspeq.application.score import ScoreTransaction, ScoringDeps
from finspeq.domain.features import FEATURE_SET_V2
from finspeq.domain.policy import DecisionPolicy, Thresholds
from tests.factories import BASE_TIME, batch, make_tx
from tests.unit.application.test_score import (
    CountModel,
    ListPublisher,
    RecordingMetrics,
    TickClock,
)


def body(n: int = 3, tx_id: str | None = None) -> dict[str, object]:
    """Return a valid request body for transaction n of the default card at BASE_TIME."""
    tx = make_tx(n, BASE_TIME)
    return {
        "tx_id": tx_id or tx.tx_id.value,
        "card": tx.card.value,
        "time": tx.time.epoch_s,
        "amount_cents": tx.amount.cents,
        "merchant": tx.merchant,
        "category": tx.category.value,
        "merchant_point": [tx.merchant_point.lat, tx.merchant_point.lon],
        "home_point": [tx.home_point.lat, tx.home_point.lon],
    }


def client(publisher: ListPublisher, ready: bool = True) -> TestClient:
    """Return a test client over a memory store holding two earlier transactions."""
    store = MemoryWindowStore()
    store.apply(batch((0, make_tx(1, BASE_TIME - 60)), (1, make_tx(2, BASE_TIME - 30))))
    deps = ScoringDeps(
        store, FEATURE_SET_V2, CountModel(), DecisionPolicy(Thresholds(0.3, 0.5)), publisher
    )
    scoring = ScoreTransaction(deps, RecordingMetrics(), TickClock())

    def broken() -> bool:
        """Fail like an unreachable dependency."""
        raise StoreUnavailableError("down")

    checks = {"model": lambda: True, "redis": (lambda: True) if ready else broken}
    api = create_api(ApiDeps(scoring, checks, lambda: (b"fq_x 1\n", "text/plain")))
    return TestClient(api, raise_server_exceptions=False)


def test_score_returns_the_decision_and_step_timings() -> None:
    """Answer 200 with the decision, the logged position, and a Server-Timing header."""
    response = client(ListPublisher()).post("/score", json=body())
    assert response.status_code == 200
    payload = response.json()
    assert (payload["decision"], payload["window_entries"], payload["offset"]) == ("REVIEW", 2, 0)
    timing = response.headers["Server-Timing"]
    assert timing.startswith("read;dur=") and "publish;dur=" in timing


def test_domain_rule_violation_is_422() -> None:
    """Answer 422 naming the broken rule when a field is outside the domain."""
    response = client(ListPublisher()).post("/score", json=body(tx_id="not-hex"))
    assert response.status_code == 422
    assert response.json()["error"] == "invalid_tx_id"


def test_malformed_body_is_422() -> None:
    """Answer 422 when a field is missing."""
    payload = body()
    del payload["merchant"]
    assert client(ListPublisher()).post("/score", json=payload).status_code == 422


def test_broker_failure_is_503_and_returns_no_decision() -> None:
    """Answer 503 with the error code when the event cannot be logged."""
    response = client(ListPublisher(fail=True)).post("/score", json=body())
    assert response.status_code == 503
    assert response.json() == {"error": "publish_failed", "detail": "broker down"}


def test_health_readiness_and_metrics() -> None:
    """Report liveness always, readiness per check, and the metrics page."""
    ready, not_ready = client(ListPublisher()), client(ListPublisher(), ready=False)
    assert ready.get("/healthz").json() == {"status": "ok"}
    assert ready.get("/readyz").status_code == 200
    response = not_ready.get("/readyz")
    assert response.status_code == 503
    assert response.json() == {"checks": {"model": True, "redis": False}}
    assert ready.get("/metrics").text == "fq_x 1\n"


def test_error_code_is_snake_case_without_suffix() -> None:
    """Turn StoreUnavailableError into "store_unavailable"."""
    assert error_code(StoreUnavailableError("x")) == "store_unavailable"
