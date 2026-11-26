"""Tests for the naive counter's counting and comparison."""

from __future__ import annotations

import fakeredis

from experiments.naive_counter import CENTS_KEY, COUNT_KEY, Totals, compare, count, read_counters
from tests.factories import CARD, OTHER_CARD, make_tx


def test_count_sums_events_and_cents_per_card() -> None:
    """Count two transactions on one card and one on another."""
    totals = count([make_tx(1, cents=100), make_tx(2, cents=250), make_tx(3, card=OTHER_CARD)])
    assert totals == {CARD: Totals(2, 350), OTHER_CARD: Totals(1, 1_000)}


def test_compare_reports_over_count() -> None:
    """Report one extra event and its cents on one card."""
    truth = {CARD: Totals(2, 350), OTHER_CARD: Totals(1, 1_000)}
    counted = {CARD: Totals(3, 600), OTHER_CARD: Totals(1, 1_000)}
    result = compare(truth, counted)
    assert (result.events_true, result.events_counted, result.events_over) == (3, 4, 1)
    assert (result.cents_over, result.cards_over_counted) == (250, 1)
    assert abs(result.over_count_percent - 100 / 3) < 1e-9


def test_read_counters_treats_missing_keys_as_zero() -> None:
    """Read stored counters and return zero totals for a card without keys."""
    client = fakeredis.FakeRedis()
    client.set(COUNT_KEY + CARD, 5)
    client.set(CENTS_KEY + CARD, 700)
    assert read_counters(client, [CARD, OTHER_CARD]) == {
        CARD: Totals(5, 700),
        OTHER_CARD: Totals(0, 0),
    }
