"""Tests for the event envelope codec."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from finspeq.adapters.codec import decode, encode
from finspeq.application.errors import CodecError
from finspeq.application.messages import EventPosition
from finspeq.domain.features import FeatureVector
from finspeq.domain.policy import Decision, Score
from finspeq.domain.scored import DecisionRecord, ShadowDecision
from tests.factories import make_tx

POSITION = EventPosition(2, 41)
RECORD = DecisionRecord(FeatureVector("v2", (1.5, 0.25)), Score(0.875), Decision.BLOCK, "abc")


def test_raw_round_trip() -> None:
    """Decode an encoded raw transaction into the same transaction without a record."""
    tx = make_tx(7, merchant="fraud_Kling | Sons")
    key, value = encode(tx, None)
    event = decode(key, value, POSITION)
    assert event.transaction == tx
    assert event.record is None
    assert event.position == POSITION
    assert key == tx.card.value.encode()


def test_scored_round_trip() -> None:
    """Decode an encoded scored transaction into the same transaction and record."""
    tx = make_tx(8)
    event = decode(*encode(tx, RECORD), POSITION)
    assert event.record == RECORD


def test_shadow_round_trip_and_absence() -> None:
    """Round-trip a shadow verdict; a record without one has no "shadow" key at all."""
    shadow = ShadowDecision(Score(0.25), Decision.REVIEW, "shadow-1")
    record = replace(RECORD, shadow=shadow)
    tx = make_tx(10)
    assert decode(*encode(tx, record), POSITION).record == record
    _, plain = encode(tx, RECORD)
    assert b"shadow" not in plain


def test_unknown_shadow_fields_are_rejected() -> None:
    """Reject a shadow object with fields other than probability, decision, and version."""
    _, value = encode(make_tx(11), RECORD)
    data = json.loads(value)
    data["scored"]["shadow"] = {"fraud_probability": 0.1}
    with pytest.raises(CodecError):
        decode(make_tx(11).card.value.encode(), json.dumps(data).encode(), POSITION)


def test_encoding_is_canonical() -> None:
    """Produce identical, compact, key-sorted ASCII bytes for the same input."""
    tx = make_tx(9, merchant="café")
    _, first = encode(tx, RECORD)
    _, second = encode(tx, RECORD)
    assert first == second
    assert first.isascii()
    assert first.startswith(b'{"key":')


def envelope(**changes: object) -> bytes:
    """Return an encoded raw envelope of make_tx(1) with top-level fields replaced."""
    _, value = encode(make_tx(1), None)
    data = json.loads(value)
    data.update(changes)
    return json.dumps(data).encode()


@pytest.mark.parametrize(
    "value",
    [
        envelope(schema_version=2),
        envelope(extra=1),
        envelope(key="4111111111111111"),
        envelope(transaction={"tx_id": "x"}),
        b"not json",
    ],
)
def test_invalid_envelopes_are_rejected(value: bytes) -> None:
    """Reject other schema versions, unknown or missing fields, a wrong key, and bad JSON."""
    key, _ = encode(make_tx(1), None)
    with pytest.raises(CodecError):
        decode(key, value, POSITION)


def test_missing_or_mismatched_message_key_is_rejected() -> None:
    """Reject a message without a key or with a key for another card."""
    _, value = encode(make_tx(1), None)
    with pytest.raises(CodecError):
        decode(None, value, POSITION)
    with pytest.raises(CodecError):
        decode(b"4111111111111111", value, POSITION)


def test_boolean_is_not_accepted_as_integer() -> None:
    """Reject true as an amount."""
    _, value = encode(make_tx(1), None)
    data = json.loads(value)
    data["transaction"]["amount_cents"] = True
    with pytest.raises(CodecError):
        decode(make_tx(1).card.value.encode(), json.dumps(data).encode(), POSITION)
