"""Tests for the rebalance pause measurement."""

from __future__ import annotations

from scripts.rebalance_report import Change, changes_of, pauses_of, summarize


def test_pause_runs_from_revoke_to_the_next_assignment_anywhere() -> None:
    """Measure each revoked partition until any consumer is assigned it again."""
    changes = [
        Change(0.0, "assign", 0),
        Change(0.0, "assign", 1),
        Change(10.0, "revoke", 0),
        Change(10.0, "revoke", 1),
        Change(12.5, "assign", 0),
        Change(13.0, "assign", 1),
        Change(20.0, "lost", 1),
    ]
    pauses = pauses_of(changes)
    assert pauses == [2.5, 3.0]
    report = summarize(pauses, poll_timeouts=1)
    assert (report.count, report.median_s, report.max_s, report.poll_timeouts) == (2, 3.0, 3.0, 1)


def test_changes_are_read_from_worker_reports() -> None:
    """Expand each rebalance event into one change per partition and count poll timeouts."""
    report: dict[str, object] = {
        "consumers": [
            {
                "rebalances": [
                    {"at": 1.0, "kind": "assign", "partitions": [0, 1]},
                    {"at": 2.0, "kind": "poll-timeout", "partitions": []},
                ]
            }
        ]
    }
    changes, timeouts = changes_of(report)
    assert [(c.at, c.kind, c.partition) for c in changes] == [
        (1.0, "assign", 0),
        (1.0, "assign", 1),
    ]
    assert timeouts == 1
