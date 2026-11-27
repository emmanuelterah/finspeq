"""CompareShadow: the live model's decisions against a shadow model's, with labels."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from finspeq.application.messages import LedgerRow
from finspeq.domain.model import TxId
from finspeq.domain.policy import Decision

FLAGGED = frozenset({Decision.REVIEW, Decision.BLOCK})


@dataclass(frozen=True, slots=True)
class ModelRates:
    """Recall and false-positive rate of one model's REVIEW-or-BLOCK and BLOCK decisions."""

    model_version: str
    flag_recall: float
    flag_fpr: float
    block_recall: float
    block_fpr: float


@dataclass(frozen=True, slots=True)
class ShadowReport:
    """How the live and shadow decisions compare on the same transactions.

    Attributes:
        rows: Ledger rows with a shadow verdict and a known label.
        frauds: Fraudulent ones among them.
        agreement: Count per "LIVE->SHADOW" decision pair, for example "APPROVE->REVIEW".
        live: Rates of the live model.
        shadow: Rates of the shadow model.
        frauds_only_shadow_flags: Frauds the shadow flagged and the live model approved.
        frauds_only_live_flags: Frauds the live model flagged and the shadow approved.
    """

    rows: int
    frauds: int
    agreement: dict[str, int]
    live: ModelRates
    shadow: ModelRates
    frauds_only_shadow_flags: int
    frauds_only_live_flags: int


class CompareShadow:
    """Builds a ShadowReport from ledger rows and the transactions' fraud labels."""

    def execute(self, rows: Iterable[LedgerRow], labels: Mapping[TxId, bool]) -> ShadowReport:
        """Compare every row that has a shadow verdict and a label; skip the others."""
        pairs: list[tuple[Decision, Decision, bool]] = []
        versions = ("", "")
        for row in rows:
            shadow = row.record.shadow
            if shadow is None or row.tx_id not in labels:
                continue
            pairs.append((row.record.decision, shadow.decision, labels[row.tx_id]))
            versions = (row.record.model_version, shadow.model_version)
        agreement: dict[str, int] = {}
        for live, shadow_decision, _ in pairs:
            key = f"{live.value}->{shadow_decision.value}"
            agreement[key] = agreement.get(key, 0) + 1
        frauds = [(live, shadow_decision) for live, shadow_decision, fraud in pairs if fraud]
        return ShadowReport(
            rows=len(pairs),
            frauds=len(frauds),
            agreement=dict(sorted(agreement.items())),
            live=_rates(versions[0], [(live, fraud) for live, _, fraud in pairs]),
            shadow=_rates(versions[1], [(shadow, fraud) for _, shadow, fraud in pairs]),
            frauds_only_shadow_flags=sum(
                1 for live, shadow in frauds if shadow in FLAGGED and live not in FLAGGED
            ),
            frauds_only_live_flags=sum(
                1 for live, shadow in frauds if live in FLAGGED and shadow not in FLAGGED
            ),
        )


def _rates(version: str, decisions: list[tuple[Decision, bool]]) -> ModelRates:
    """Return flag and block recall and FPR of decisions against their labels."""
    frauds = sum(1 for _, fraud in decisions if fraud)
    legits = len(decisions) - frauds

    def share(selected: frozenset[Decision], fraud: bool) -> float:
        """Return the share of fraud (or legitimate) decisions that fall in selected."""
        whole = frauds if fraud else legits
        part = sum(1 for d, f in decisions if f is fraud and d in selected)
        return part / whole if whole else 0.0

    block = frozenset({Decision.BLOCK})
    return ModelRates(
        version,
        share(FLAGGED, True),
        share(FLAGGED, False),
        share(block, True),
        share(block, False),
    )
