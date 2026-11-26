"""In-process implementations of HistoricalSource and TrainingSink."""

from __future__ import annotations

from collections.abc import Iterator, Sequence

from finspeq.application.messages import DatasetRef, LabeledTransaction, TimeRange, TrainingRow
from finspeq.application.ordering import CanonicalOrderGuard
from finspeq.application.ports import HistoricalSource, TrainingSink
from finspeq.domain.errors import SchemaMismatchError


class MemoryHistoricalSource(HistoricalSource):
    """Labelled transactions held in a list, yielded in list order."""

    def __init__(self, items: Sequence[LabeledTransaction]) -> None:
        """Create a source over items, which are checked for canonical order when yielded."""
        self._items = tuple(items)

    def iter_labeled(self, time_range: TimeRange | None) -> Iterator[LabeledTransaction]:
        """Yield the items inside time_range (all items if None) in canonical order.

        Raises:
            InvalidSourceDataError: An item is not above the previous one in canonical order.
        """
        guard = CanonicalOrderGuard()
        for item in self._items:
            guard.check(item)
            if time_range is None or time_range.contains(item.transaction.time):
                yield item


class MemoryTrainingSink(TrainingSink):
    """Training rows collected in a list; `rows` is readable after close."""

    def __init__(self, schema: str, width: int) -> None:
        """Create an empty sink.

        Args:
            schema: Feature-set version every row's vector must carry.
            width: Number of values every row's vector must have.
        """
        self._schema = schema
        self._width = width
        self._pending: list[TrainingRow] = []
        self.rows: tuple[TrainingRow, ...] = ()

    def write(self, row: TrainingRow) -> None:
        """Append row to the pending rows.

        Raises:
            SchemaMismatchError: row's vector has another schema or another number of values.
        """
        if row.vector.schema != self._schema or len(row.vector.values) != self._width:
            raise SchemaMismatchError(f"expected {self._schema}/{self._width} values")
        self._pending.append(row)

    def close(self) -> DatasetRef:
        """Publish the pending rows as `rows` and return a reference with the row count."""
        self.rows = tuple(self._pending)
        return DatasetRef(path="memory", rows=len(self.rows), schema=self._schema)
