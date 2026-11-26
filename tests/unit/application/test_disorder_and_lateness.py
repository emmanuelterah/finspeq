"""Tests for the disorder model, the watermark, and LatenessSweep."""

from __future__ import annotations

import pytest

from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.application.disorder import NO_DISORDER, Disorder, arrival_order
from finspeq.application.errors import InvalidMessageError
from finspeq.application.lateness import LatenessSweep, Scoring
from finspeq.application.messages import LabeledTransaction
from finspeq.application.ports import WindowStore
from finspeq.application.watermark import Watermark
from finspeq.domain.features import FEATURE_SET_V2, FeatureVector
from finspeq.domain.model import EventTime
from finspeq.domain.policy import DecisionPolicy, Score, Thresholds
from finspeq.domain.window import WINDOW_SECONDS
from tests.factories import BASE_TIME, OTHER_CARD, make_tx

DELAYED = Disorder(
    delayed_fraction=0.5, mean_delay_s=600.0, max_delay_s=3_600, duplicate_fraction=0.1, seed=7
)


def history(count: int, step_s: int = 60) -> list[LabeledTransaction]:
    """Return count transactions step_s apart, alternating between two cards."""
    return [
        LabeledTransaction(
            i,
            make_tx(
                i + 1,
                BASE_TIME + i * step_s,
                cents=100 + i,
                card=OTHER_CARD if i % 2 else "4000000000000002",
            ),
            is_fraud=i % 5 == 0,
        )
        for i in range(count)
    ]


def memory_store(lateness: int) -> WindowStore:
    """Return an empty memory store with the given allowed lateness."""
    return MemoryWindowStore(allowed_lateness_s=lateness)


class AmountModel:
    """FraudModel whose score is the amount in dollars divided by ten, capped at 1."""

    version = "test"
    schema = "v2"

    def predict(self, vector: FeatureVector) -> Score:
        """Return min(1, first feature / 10)."""
        return Score(min(1.0, vector.values[0] / 10))


@pytest.mark.parametrize(
    "fields",
    [
        {"delayed_fraction": 1.5},
        {"duplicate_fraction": -0.1},
        {"max_delay_s": -1},
        {"delayed_fraction": 0.5, "mean_delay_s": 0.0},
    ],
)
def test_disorder_rejects_invalid_settings(fields: dict[str, float]) -> None:
    """Fractions outside [0, 1], negative delays, and zero mean with delays are rejected."""
    with pytest.raises(InvalidMessageError):
        Disorder(**fields)  # type: ignore[arg-type]


def test_no_disorder_keeps_canonical_order() -> None:
    """Without delays or duplicates the arrival order is the input order."""
    items = history(50)
    assert list(arrival_order(items, NO_DISORDER)) == items


def test_arrival_order_is_seeded_and_bounded() -> None:
    """The same seed gives the same order; no arrival is behind by more than max_delay_s."""
    items = history(400)
    first = list(arrival_order(items, DELAYED))
    assert first == list(arrival_order(items, DELAYED))
    assert first != items
    assert {item.seq for item in first} == {item.seq for item in items}
    watermark = Watermark(0)
    behind = [watermark.observe(item.transaction.time) for item in first]
    assert max(behind) <= DELAYED.max_delay_s


def test_every_item_is_duplicated_at_fraction_one() -> None:
    """duplicate_fraction=1 makes every transaction arrive exactly twice."""
    items = history(20)
    arrivals = list(arrival_order(items, Disorder(duplicate_fraction=1.0, seed=1)))
    assert len(arrivals) == 40
    assert sorted(item.seq for item in arrivals) == sorted(2 * [item.seq for item in items])


def test_watermark_measures_distance_behind_newest() -> None:
    """Return distance behind the newest time; flag it late only beyond the lateness."""
    watermark = Watermark(60)
    assert watermark.observe(EventTime(1_000)) == 0
    assert watermark.observe(EventTime(1_100)) == 0
    assert watermark.observe(EventTime(1_050)) == 50
    assert watermark.observe(EventTime(1_000)) == 100
    assert not watermark.is_late(60)
    assert watermark.is_late(61)


def test_card_lateness_ignores_other_cards() -> None:
    """An event behind the stream but not behind its own card is not card-late and matches."""
    a1 = LabeledTransaction(0, make_tx(1, BASE_TIME), is_fraud=False)
    b1 = LabeledTransaction(1, make_tx(2, BASE_TIME + 300, card=OTHER_CARD), is_fraud=False)
    a2 = LabeledTransaction(2, make_tx(3, BASE_TIME + 600), is_fraud=False)
    sweep = LatenessSweep(memory_store, FEATURE_SET_V2, None)
    report = sweep.execute([a1, b1, a2], [a1, a2, b1], [0])
    assert (report.out_of_order, report.card_out_of_order) == (1, 0)
    (point,) = report.points
    assert (point.late, point.card_late, point.mismatched) == (1, 0, 0)


def test_sweep_without_disorder_matches_reference() -> None:
    """In-order arrivals give zero late events, mismatches, and decision changes."""
    items = history(30)
    scoring = Scoring(AmountModel(), DecisionPolicy(Thresholds(review=0.12, block=0.125)))
    sweep = LatenessSweep(memory_store, FEATURE_SET_V2, scoring)
    report = sweep.execute(items, items, [0, 600])
    assert report.out_of_order == 0
    assert report.duplicates == 0
    assert report.reference_rates is not None
    for point in report.points:
        assert (point.late, point.card_late, point.mismatched) == (0, 0, 0)
        assert point.decision_changes == 0
        assert point.rates == report.reference_rates


def test_sweep_separates_pruning_error_from_missing_predecessors() -> None:
    """Lateness recovers entries lost to pruning but not entries that have not arrived yet.

    e3 arrives first and moves the newest time 86,900 s past e1. With lateness 0 the store
    prunes e1 before late e2 is read, so e2 and e3 both differ from the reference. With
    lateness 3,600 e1 is kept and only e3, which never sees e2, differs.
    """
    e1 = LabeledTransaction(0, make_tx(1, BASE_TIME, cents=100), is_fraud=False)
    e2 = LabeledTransaction(1, make_tx(2, BASE_TIME + 1_000, cents=200), is_fraud=True)
    e3 = LabeledTransaction(
        2, make_tx(3, BASE_TIME + WINDOW_SECONDS + 500, cents=300), is_fraud=False
    )
    sweep = LatenessSweep(memory_store, FEATURE_SET_V2, None)
    report = sweep.execute([e1, e2, e3], [e1, e3, e2, e2], [0, 3_600])
    assert report.duplicates == 1
    assert report.out_of_order == 1
    assert report.max_behind_s == WINDOW_SECONDS - 500
    assert (report.card_out_of_order, report.card_max_behind_s) == (1, WINDOW_SECONDS - 500)
    assert report.reference_rates is None
    strict, relaxed = report.points
    assert (strict.late, strict.mismatched, strict.mismatched_late) == (1, 2, 1)
    assert (strict.card_late, strict.mismatched_card_late) == (1, 1)
    assert (relaxed.late, relaxed.mismatched, relaxed.mismatched_late) == (1, 1, 0)
    assert relaxed.retention_factor == (WINDOW_SECONDS + 3_600) / WINDOW_SECONDS
    assert dict(strict.feature_mismatches)["tx_count_24h"] == 2
