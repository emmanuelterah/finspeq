"""Event envelope codec: transactions and decision records to and from canonical JSON bytes."""

from __future__ import annotations

import json
from collections.abc import Mapping

from finspeq.application.errors import CodecError
from finspeq.application.messages import Event, EventPosition
from finspeq.domain.errors import DomainError
from finspeq.domain.features import FeatureVector
from finspeq.domain.model import (
    CardId,
    Category,
    EventTime,
    GeoPoint,
    Money,
    Transaction,
    TxId,
)
from finspeq.domain.policy import Decision, Score
from finspeq.domain.scored import DecisionRecord

SCHEMA_VERSION = 1
ENVELOPE_FIELDS = frozenset({"schema_version", "key", "transaction", "scored"})
TRANSACTION_FIELDS = frozenset(
    {
        "tx_id",
        "card",
        "time",
        "amount_cents",
        "merchant",
        "category",
        "merchant_point",
        "home_point",
    }
)
SCORED_FIELDS = frozenset(
    {"feature_schema", "features", "fraud_probability", "decision", "model_version"}
)
POINT_LENGTH = 2


def encode(tx: Transaction, record: DecisionRecord | None) -> tuple[bytes, bytes]:
    """Return the message key (card number) and canonical JSON value for tx and record.

    The same inputs always produce the same bytes: keys are sorted, separators are compact,
    and output is ASCII.
    """
    envelope = {
        "schema_version": SCHEMA_VERSION,
        "key": tx.card.value,
        "transaction": {
            "tx_id": tx.tx_id.value,
            "card": tx.card.value,
            "time": tx.time.epoch_s,
            "amount_cents": tx.amount.cents,
            "merchant": tx.merchant,
            "category": tx.category.value,
            "merchant_point": [tx.merchant_point.lat, tx.merchant_point.lon],
            "home_point": [tx.home_point.lat, tx.home_point.lon],
        },
        "scored": None if record is None else _scored_fields(record),
    }
    value = json.dumps(envelope, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return tx.card.value.encode("ascii"), value.encode("ascii")


def decode(key: bytes | None, value: bytes | None, position: EventPosition) -> Event:
    """Return the event stored at position with message key and value.

    Raises:
        CodecError: The key is missing or differs from the card, the value is not a schema
            version 1 envelope with exactly the expected fields, or a field violates a domain
            rule.
    """
    if key is None or value is None:
        raise CodecError(f"offset {position.offset}: missing key or value")
    try:
        envelope = _object(json.loads(value), ENVELOPE_FIELDS, "envelope")
        if envelope["schema_version"] != SCHEMA_VERSION:
            raise CodecError(f"unsupported schema_version {envelope['schema_version']!r}")
        tx = _transaction(envelope["transaction"])
        scored = envelope["scored"]
        record = None if scored is None else _record(scored)
    except (ValueError, TypeError, KeyError, DomainError) as exc:
        raise CodecError(f"offset {position.offset}: {exc}") from exc
    if key.decode("ascii", errors="replace") != tx.card.value or envelope["key"] != tx.card.value:
        raise CodecError(f"offset {position.offset}: key does not match the card")
    return Event(position=position, transaction=tx, record=record)


def _scored_fields(record: DecisionRecord) -> dict[str, object]:
    """Return the JSON fields of a decision record."""
    return {
        "feature_schema": record.vector.schema,
        "features": list(record.vector.values),
        "fraud_probability": record.score.fraud_probability,
        "decision": record.decision.value,
        "model_version": record.model_version,
    }


def _object(value: object, fields: frozenset[str], name: str) -> Mapping[str, object]:
    """Return value if it is an object with exactly the given fields.

    Raises:
        CodecError: value is not an object or has other fields.
    """
    if not isinstance(value, Mapping) or set(value) != fields:
        raise CodecError(f"{name} must have exactly the fields {sorted(fields)}")
    return value


def _transaction(value: object) -> Transaction:
    """Return the Transaction described by a decoded transaction object."""
    fields = _object(value, TRANSACTION_FIELDS, "transaction")
    return Transaction(
        tx_id=TxId(_string(fields["tx_id"])),
        card=CardId(_string(fields["card"])),
        time=EventTime(_integer(fields["time"])),
        amount=Money(_integer(fields["amount_cents"])),
        merchant=_string(fields["merchant"]),
        category=Category.parse(_string(fields["category"])),
        merchant_point=_point(fields["merchant_point"]),
        home_point=_point(fields["home_point"]),
    )


def _record(value: object) -> DecisionRecord:
    """Return the DecisionRecord described by a decoded scored object."""
    fields = _object(value, SCORED_FIELDS, "scored")
    values = fields["features"]
    if not isinstance(values, list):
        raise CodecError("features must be a list")
    return DecisionRecord(
        vector=FeatureVector(_string(fields["feature_schema"]), tuple(_number(v) for v in values)),
        score=Score(_number(fields["fraud_probability"])),
        decision=Decision(_string(fields["decision"])),
        model_version=_string(fields["model_version"]),
    )


def _point(value: object) -> GeoPoint:
    """Return the GeoPoint encoded as [lat, lon]."""
    if not isinstance(value, list) or len(value) != POINT_LENGTH:
        raise CodecError("points must be [lat, lon]")
    return GeoPoint(_number(value[0]), _number(value[1]))


def _string(value: object) -> str:
    """Return value if it is a string.

    Raises:
        CodecError: value is not a string.
    """
    if not isinstance(value, str):
        raise CodecError(f"expected a string, got {value!r}")
    return value


def _integer(value: object) -> int:
    """Return value if it is an int and not a bool.

    Raises:
        CodecError: value is not an integer.
    """
    if not isinstance(value, int) or isinstance(value, bool):
        raise CodecError(f"expected an integer, got {value!r}")
    return value


def _number(value: object) -> float:
    """Return value as a float if it is an int or float and not a bool.

    Raises:
        CodecError: value is not a number.
    """
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise CodecError(f"expected a number, got {value!r}")
    return float(value)
