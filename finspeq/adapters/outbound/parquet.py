"""Parquet implementations of HistoricalSource and TrainingSink."""

from __future__ import annotations

import json
import os
from bisect import bisect_left
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from finspeq.application.errors import InvalidSourceDataError
from finspeq.application.messages import DatasetRef, LabeledTransaction, TimeRange, TrainingRow
from finspeq.application.ordering import CanonicalOrderGuard
from finspeq.application.ports import HistoricalSource, TrainingSink
from finspeq.domain.errors import DomainError, SchemaMismatchError
from finspeq.domain.model import (
    CENTS_PER_DOLLAR,
    CardId,
    Category,
    EventTime,
    GeoPoint,
    Money,
    Transaction,
    TxId,
)

EVENT_COLUMNS = (
    "seq",
    "trans_num",
    "cc_num",
    "unix_time",
    "category",
    "amt",
    "merchant",
    "merch_lat",
    "merch_long",
    "lat",
    "long",
    "is_fraud",
)
ROW_COLUMNS = ("seq", "time", "tx_id")
LABEL_COLUMNS = ("is_fraud", "split")
FEATURE_SCHEMA_KEY = b"finspeq.feature_schema"
FEATURE_NAMES_KEY = b"finspeq.feature_names"
CENTS_TOLERANCE = 1e-6
READ_BATCH_ROWS = 65_536
DEFAULT_ROWS_PER_GROUP = 100_000


class ParquetHistoricalSource(HistoricalSource):
    """Labelled transactions read from a canonical event file written by sort_events.py."""

    def __init__(self, path: Path) -> None:
        """Create a source over the Parquet file at path; the file is opened per iteration."""
        self._path = path

    def iter_labeled(self, time_range: TimeRange | None) -> Iterator[LabeledTransaction]:
        """Yield the file's transactions inside time_range (all if None) in canonical order.

        Skips row groups whose unix_time statistics lie outside time_range, converts only the
        rows of each batch whose unix_time is inside time_range, and stops at the first row at
        or after time_range.end. Rows outside time_range are not validated.

        Raises:
            InvalidSourceDataError: A yielded row is out of canonical order or violates a
                domain rule.
        """
        parquet_file = pq.ParquetFile(self._path)
        guard = CanonicalOrderGuard()
        for group in _row_groups_in_range(parquet_file, time_range):
            batches = parquet_file.iter_batches(
                batch_size=READ_BATCH_ROWS, row_groups=[group], columns=list(EVENT_COLUMNS)
            )
            for batch in batches:
                times = batch.column("unix_time").to_pylist()
                low, high = _bounds_in_range(times, time_range)
                columns = [batch.column(name)[low:high].to_pylist() for name in EVENT_COLUMNS]
                for values in zip(*columns, strict=True):
                    yield guard.check(_to_labeled(values))
                if high < len(times):
                    return


class ParquetTrainingSink(TrainingSink):
    """Training rows written to one Parquet file that becomes visible only on close."""

    def __init__(
        self,
        path: Path,
        schema: str,
        feature_names: tuple[str, ...],
        rows_per_group: int = DEFAULT_ROWS_PER_GROUP,
    ) -> None:
        """Open a temporary file next to path for writing.

        Args:
            path: Final location of the Parquet file.
            schema: Feature-set version every row's vector must carry.
            feature_names: Column names of the vector values, in order.
            rows_per_group: Rows buffered before a row group is written.

        Raises:
            SchemaMismatchError: A feature name equals a reserved column name.
        """
        reserved = set(ROW_COLUMNS) | set(LABEL_COLUMNS)
        if reserved & set(feature_names):
            raise SchemaMismatchError(f"feature names use reserved columns: {feature_names}")
        self._path = path
        self._temporary = path.with_name(path.name + ".tmp")
        self._schema_version = schema
        self._feature_names = feature_names
        self._rows_per_group = rows_per_group
        self._arrow_schema = _training_schema(schema, feature_names)
        self._columns: dict[str, list[object]] = {name: [] for name in self._arrow_schema.names}
        self._rows = 0
        path.parent.mkdir(parents=True, exist_ok=True)
        self._writer = pq.ParquetWriter(self._temporary, self._arrow_schema, compression="zstd")

    def write(self, row: TrainingRow) -> None:
        """Buffer row and write a row group when rows_per_group rows are buffered.

        Raises:
            SchemaMismatchError: row's vector has another schema or another number of values.
        """
        values = row.vector.values
        if row.vector.schema != self._schema_version or len(values) != len(self._feature_names):
            raise SchemaMismatchError(
                f"expected {self._schema_version} with {len(self._feature_names)} values"
            )
        columns = self._columns
        columns["seq"].append(row.seq)
        columns["time"].append(row.time.epoch_s)
        columns["tx_id"].append(row.tx_id.value)
        for name, value in zip(self._feature_names, values, strict=True):
            columns[name].append(value)
        columns["is_fraud"].append(row.is_fraud)
        columns["split"].append(row.split.value)
        self._rows += 1
        if len(columns["seq"]) >= self._rows_per_group:
            self._flush()

    def close(self) -> DatasetRef:
        """Write buffered rows, close the file, and rename it from the temporary name to path."""
        self._flush()
        self._writer.close()
        os.replace(self._temporary, self._path)
        return DatasetRef(path=self._path.as_posix(), rows=self._rows, schema=self._schema_version)

    def _flush(self) -> None:
        """Write the buffered rows as one row group and clear the buffers."""
        if not self._columns["seq"]:
            return
        table = pa.Table.from_pydict(self._columns, schema=self._arrow_schema)
        self._writer.write_table(table, row_group_size=self._rows_per_group)
        for column in self._columns.values():
            column.clear()


def _training_schema(schema: str, feature_names: tuple[str, ...]) -> pa.Schema:
    """Return the Arrow schema of a training file, with the feature schema in its metadata."""
    fields = [
        pa.field("seq", pa.int64(), nullable=False),
        pa.field("time", pa.int64(), nullable=False),
        pa.field("tx_id", pa.string(), nullable=False),
        *(pa.field(name, pa.float64(), nullable=False) for name in feature_names),
        pa.field("is_fraud", pa.bool_(), nullable=False),
        pa.field("split", pa.string(), nullable=False),
    ]
    metadata = {
        FEATURE_SCHEMA_KEY: schema.encode(),
        FEATURE_NAMES_KEY: json.dumps(list(feature_names)).encode(),
    }
    return pa.schema(fields, metadata=metadata)


def _bounds_in_range(times: list[int], time_range: TimeRange | None) -> tuple[int, int]:
    """Return [low, high) indexes of the sorted times that lie inside time_range."""
    if time_range is None:
        return 0, len(times)
    low = bisect_left(times, time_range.start.epoch_s)
    high = bisect_left(times, time_range.end.epoch_s, lo=low)
    return low, high


def _row_groups_in_range(parquet_file: pq.ParquetFile, time_range: TimeRange | None) -> list[int]:
    """Return the indexes of row groups that may hold rows inside time_range."""
    metadata = parquet_file.metadata
    groups = list(range(metadata.num_row_groups))
    if time_range is None:
        return groups
    column = parquet_file.schema_arrow.get_field_index("unix_time")
    return [
        group
        for group in groups
        if _may_overlap(metadata.row_group(group).column(column).statistics, time_range)
    ]


def _may_overlap(statistics: pq.Statistics | None, time_range: TimeRange) -> bool:
    """Return False only if min/max statistics prove no value lies inside time_range."""
    if statistics is None or not statistics.has_min_max:
        return True
    return bool(
        statistics.max >= time_range.start.epoch_s and statistics.min < time_range.end.epoch_s
    )


def _to_labeled(values: tuple[object, ...]) -> LabeledTransaction:
    """Return the labelled transaction for one row of EVENT_COLUMNS values.

    Raises:
        InvalidSourceDataError: A value has the wrong type or violates a domain rule.
    """
    seq, trans_num, cc_num, unix_time, category, amt, merchant = values[:7]
    merch_lat, merch_long, lat, long, is_fraud = values[7:]
    try:
        transaction = Transaction(
            tx_id=TxId(str(trans_num)),
            card=CardId(str(cc_num)),
            time=EventTime(_as_int(unix_time)),
            amount=Money(_to_cents(_as_float(amt))),
            merchant=str(merchant),
            category=Category.parse(str(category)),
            merchant_point=GeoPoint(_as_float(merch_lat), _as_float(merch_long)),
            home_point=GeoPoint(_as_float(lat), _as_float(long)),
        )
    except (DomainError, InvalidSourceDataError) as exc:
        raise InvalidSourceDataError(f"seq {seq} ({trans_num}): {exc}") from exc
    return LabeledTransaction(seq=_as_int(seq), transaction=transaction, is_fraud=bool(is_fraud))


def _to_cents(amount: float) -> int:
    """Return amount in dollars as integer cents.

    Raises:
        InvalidSourceDataError: amount is not within CENTS_TOLERANCE of a whole number of cents.
    """
    scaled = amount * CENTS_PER_DOLLAR
    cents = round(scaled)
    if abs(scaled - cents) > CENTS_TOLERANCE:
        raise InvalidSourceDataError(f"amount {amount} is not a whole number of cents")
    return cents


def _as_int(value: object) -> int:
    """Return value if it is an int (not a bool).

    Raises:
        InvalidSourceDataError: value is not an int.
    """
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise InvalidSourceDataError(f"expected an integer, got {value!r}")


def _as_float(value: object) -> float:
    """Return value as a float if it is an int or float (not a bool).

    Raises:
        InvalidSourceDataError: value is not numeric.
    """
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    raise InvalidSourceDataError(f"expected a number, got {value!r}")
