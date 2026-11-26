"""Differential test: every WindowStore adapter behaves exactly like MemoryWindowStore."""

from __future__ import annotations

import random

from finspeq.adapters.outbound.memory_window import MemoryWindowStore
from finspeq.application.messages import LabeledTransaction
from finspeq.application.replay import partitioned_batches
from finspeq.application.verify import ReplayScenario, VerifyReplay
from finspeq.domain.window import WINDOW_SECONDS
from tests.contract.conftest import StoreFactory
from tests.factories import BASE_TIME, make_tx

CARDS = [f"41000000000{n:05d}" for n in range(40)]
LATENESS_S = 600


def disordered_history(seed: int) -> list[LabeledTransaction]:
    """Return 1,500 transactions over 4 days with ties, late events, and republished duplicates."""
    rng = random.Random(seed)
    items: list[LabeledTransaction] = []
    for seq in range(1_500):
        time = BASE_TIME + seq * 230 + rng.choice((0, 0, 0, -1))
        if rng.random() < 0.05:
            time -= rng.randrange(1, WINDOW_SECONDS + 2 * 3_600)
        card = CARDS[rng.randrange(len(CARDS))]
        tx = make_tx(
            seq + 1,
            time,
            cents=rng.randrange(100, 90_000),
            card=card,
            merchant=f"merchant_{rng.randrange(25)}",
        )
        items.append(LabeledTransaction(seq, tx, is_fraud=False))
        if rng.random() < 0.03 and items:
            items.append(LabeledTransaction(seq, rng.choice(items).transaction, is_fraud=False))
    return items


def test_store_matches_memory_reference_under_disorder(make_store: StoreFactory) -> None:
    """Match the memory reference on every window, result, and position, with redelivery."""
    batches = list(partitioned_batches(disordered_history(seed=11), partitions=3, batch_size=17))
    report = VerifyReplay(MemoryWindowStore(LATENESS_S), make_store(LATENESS_S)).execute(
        batches, ReplayScenario(redeliver_every=5)
    )
    assert report.passed, report.first_mismatch
    assert report.deliveries > report.batches
    assert report.window_reads == report.events


def test_replay_detects_a_store_that_differs() -> None:
    """Report mismatches when the candidate keeps late entries the reference prunes."""
    batches = list(partitioned_batches(disordered_history(seed=3), partitions=2, batch_size=10))
    report = VerifyReplay(MemoryWindowStore(0), MemoryWindowStore(3 * 3_600)).execute(
        batches, ReplayScenario()
    )
    assert not report.passed
    assert report.window_mismatches > 0
