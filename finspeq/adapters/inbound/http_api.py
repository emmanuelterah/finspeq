"""HTTP adapter of the scoring API: request and response models, routes, and error mapping."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict

from finspeq.application.errors import PortError
from finspeq.application.score import ScoreOutcome, ScoreTransaction
from finspeq.domain.errors import DomainError
from finspeq.domain.model import (
    CardId,
    Category,
    EventTime,
    GeoPoint,
    Money,
    Transaction,
    TxId,
)

LOGGER = logging.getLogger(__name__)
HTTP_UNPROCESSABLE = 422
HTTP_UNAVAILABLE = 503
HTTP_INTERNAL_ERROR = 500
CAMEL_BOUNDARY = re.compile(r"(?<!^)(?=[A-Z])")


class TransactionIn(BaseModel):
    """Body of POST /score: one card transaction, in the event envelope's field names."""

    model_config = ConfigDict(extra="forbid")

    tx_id: str
    card: str
    time: int
    amount_cents: int
    merchant: str
    category: str
    merchant_point: tuple[float, float]
    home_point: tuple[float, float]


class ScoreOut(BaseModel):
    """Body of a successful POST /score response."""

    tx_id: str
    decision: str
    fraud_probability: float
    model_version: str
    window_entries: int
    partition: int
    offset: int


@dataclass(frozen=True, slots=True)
class ApiDeps:
    """What the API calls: the scoring use case, readiness checks, and the metrics page.

    Attributes:
        scoring: Scores one transaction and logs the decision.
        checks: Named readiness checks; each returns True when its dependency is usable.
        metrics_page: Returns the metrics exposition body and its content type.
    """

    scoring: ScoreTransaction
    checks: Mapping[str, Callable[[], bool]]
    metrics_page: Callable[[], tuple[bytes, str]]


def to_transaction(body: TransactionIn) -> Transaction:
    """Return the domain Transaction described by body.

    Raises:
        DomainError: A field violates a domain rule.
    """
    return Transaction(
        tx_id=TxId(body.tx_id),
        card=CardId(body.card),
        time=EventTime(body.time),
        amount=Money(body.amount_cents),
        merchant=body.merchant,
        category=Category.parse(body.category),
        merchant_point=GeoPoint(*body.merchant_point),
        home_point=GeoPoint(*body.home_point),
    )


def server_timing(outcome: ScoreOutcome) -> str:
    """Return a Server-Timing header value with each step's duration in milliseconds."""
    return ", ".join(f"{name};dur={ms:.3f}" for name, ms in outcome.step_ms)


def error_code(error: Exception) -> str:
    """Return error's class name in snake_case without "_error" ("store_unavailable")."""
    name = CAMEL_BOUNDARY.sub("_", type(error).__name__).lower()
    return name.removesuffix("_error")


def create_api(deps: ApiDeps) -> FastAPI:
    """Return the FastAPI application with /score, /healthz, /readyz, and /metrics.

    POST /score answers 200 with the decision, 422 when a field breaks a domain rule, 503 when
    a store or the broker fails (no decision is returned without a log entry), and 500 for
    anything else, which is logged.
    """
    app = FastAPI(title="Finspeq scoring API")

    @app.exception_handler(DomainError)
    def domain_error(_request: Request, error: DomainError) -> JSONResponse:
        """Answer 422 with the rule that the request broke."""
        body = {"error": error_code(error), "detail": str(error)}
        return JSONResponse(body, status_code=HTTP_UNPROCESSABLE)

    @app.exception_handler(PortError)
    def port_error(_request: Request, error: PortError) -> JSONResponse:
        """Answer 503 with the failing dependency's error code."""
        body = {"error": error_code(error), "detail": str(error)}
        return JSONResponse(body, status_code=HTTP_UNAVAILABLE)

    @app.exception_handler(Exception)
    def unexpected_error(_request: Request, error: Exception) -> JSONResponse:
        """Log the error with its traceback and answer 500."""
        LOGGER.exception("unexpected error while scoring", exc_info=error)
        return JSONResponse({"error": "internal"}, status_code=HTTP_INTERNAL_ERROR)

    @app.post("/score")
    def score(body: TransactionIn, response: Response) -> ScoreOut:
        """Score one transaction, log it with its decision, and return the decision."""
        outcome = deps.scoring.execute(to_transaction(body))
        record = outcome.scored.record
        response.headers["Server-Timing"] = server_timing(outcome)
        return ScoreOut(
            tx_id=body.tx_id,
            decision=record.decision.value,
            fraud_probability=record.score.fraud_probability,
            model_version=record.model_version,
            window_entries=outcome.window_entries,
            partition=outcome.position.partition,
            offset=outcome.position.offset,
        )

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        """Answer 200 while the process runs."""
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz() -> JSONResponse:
        """Run every readiness check; answer 200 if all pass, otherwise 503 with the results."""
        results = {name: _passes(check) for name, check in deps.checks.items()}
        status = 200 if all(results.values()) else HTTP_UNAVAILABLE
        return JSONResponse({"checks": results}, status_code=status)

    @app.get("/metrics")
    def metrics() -> Response:
        """Return this process's metrics in the Prometheus text format."""
        body, content_type = deps.metrics_page()
        return Response(body, media_type=content_type)

    return app


def _passes(check: Callable[[], bool]) -> bool:
    """Return check(), or False if it raises."""
    try:
        return check()
    except Exception:
        return False
