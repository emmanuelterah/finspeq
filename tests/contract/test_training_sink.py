"""TrainingSink contract, run against every adapter."""

from __future__ import annotations

import pytest

from finspeq.application.messages import Split, TrainingRow
from finspeq.domain.errors import SchemaMismatchError
from finspeq.domain.features import FeatureVector
from finspeq.domain.model import EventTime
from tests.contract.conftest import SinkHarness
from tests.factories import tx_id


def row(seq: int, schema: str = "v1", width: int = 3) -> TrainingRow:
    """Return a training row with width feature values and the given schema."""
    vector = FeatureVector(schema=schema, values=tuple(float(seq + i) for i in range(width)))
    return TrainingRow(
        seq=seq,
        time=EventTime(1_000 + seq),
        tx_id=tx_id(seq + 1),
        vector=vector,
        is_fraud=seq % 2 == 0,
        split=Split.VALID,
    )


def test_written_rows_are_returned_after_close(sink_harness: SinkHarness) -> None:
    """Return every written row, in order, once the sink is closed."""
    sink = sink_harness.create()
    rows = [row(seq) for seq in range(5)]
    for item in rows:
        sink.write(item)
    ref = sink.close()
    assert ref.rows == 5
    assert ref.schema == "v1"
    assert sink_harness.read_back() == rows


def test_rows_are_not_visible_before_close(sink_harness: SinkHarness) -> None:
    """Expose no rows while the sink is still open."""
    sink = sink_harness.create()
    sink.write(row(0))
    assert sink_harness.read_back() == []


def test_row_with_other_schema_is_rejected(sink_harness: SinkHarness) -> None:
    """Raise SchemaMismatchError for a vector with another schema or width."""
    sink = sink_harness.create()
    with pytest.raises(SchemaMismatchError):
        sink.write(row(0, schema="v2"))
    with pytest.raises(SchemaMismatchError):
        sink.write(row(0, width=4))
