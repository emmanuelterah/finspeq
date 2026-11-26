"""Check the canonical event file against the assumptions later stages rely on.

Writes a Markdown report and returns a non-zero exit code if any check fails.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb

KNOWN_CATEGORIES = (
    "entertainment",
    "food_dining",
    "gas_transport",
    "grocery_net",
    "grocery_pos",
    "health_fitness",
    "home",
    "kids_pets",
    "misc_net",
    "misc_pos",
    "personal_care",
    "shopping_net",
    "shopping_pos",
    "travel",
)
CENTS_TOLERANCE = 1e-6


@dataclass(frozen=True, slots=True)
class Check:
    """One data-quality rule: a name and a query that counts violating rows."""

    name: str
    violations_query: str


def checks(events: str) -> tuple[Check, ...]:
    """Return every check, each counting the rows of `events` that violate it."""
    categories = ", ".join(f"'{name}'" for name in KNOWN_CATEGORIES)
    return (
        Check("trans_num unique", f"SELECT count(*) - count(DISTINCT trans_num) FROM {events}"),
        Check(
            "trans_num is 32 lowercase hex",
            f"SELECT count(*) FROM {events} "
            "WHERE NOT regexp_full_match(trans_num, '[0-9a-f]{32}')",
        ),
        Check(
            "cc_num is 8-19 digits",
            f"SELECT count(*) FROM {events} WHERE NOT regexp_full_match(cc_num, '[0-9]{{8,19}}')",
        ),
        Check("amt > 0", f"SELECT count(*) FROM {events} WHERE amt <= 0 OR amt IS NULL"),
        Check(
            "amt is whole cents",
            f"SELECT count(*) FROM {events} "
            f"WHERE abs(amt * 100 - round(amt * 100)) > {CENTS_TOLERANCE}",
        ),
        Check(
            "category known",
            f"SELECT count(*) FROM {events} WHERE category NOT IN ({categories})",
        ),
        Check(
            "coordinates in range",
            f"SELECT count(*) FROM {events} WHERE NOT (lat BETWEEN -90 AND 90 "
            "AND long BETWEEN -180 AND 180 AND merch_lat BETWEEN -90 AND 90 "
            "AND merch_long BETWEEN -180 AND 180)",
        ),
        Check(
            "merchant not blank",
            f"SELECT count(*) FROM {events} WHERE merchant IS NULL OR trim(merchant) = ''",
        ),
        Check(
            "is_fraud is 0 or 1",
            f"SELECT count(*) FROM {events} WHERE is_fraud NOT IN (0, 1) OR is_fraud IS NULL",
        ),
        Check(
            "unix_time non-negative",
            f"SELECT count(*) FROM {events} WHERE unix_time < 0 OR unix_time IS NULL",
        ),
    )


def run(source: Path) -> tuple[list[tuple[str, int]], list[str]]:
    """Run every check and the descriptive queries against source.

    Returns:
        A list of (check name, violation count), and Markdown lines describing the data.
    """
    connection = duckdb.connect()
    events = f"'{source.as_posix()}'"
    results = []
    for check in checks(events):
        row = connection.execute(check.violations_query).fetchone()
        results.append((check.name, int(row[0]) if row else -1))
    episodes = connection.execute(
        "SELECT count(*), quantile_cont(n, 0.5), quantile_cont(span_h, 0.5), max(n) FROM "
        "(SELECT cc_num, count(*) AS n, (max(unix_time) - min(unix_time)) / 3600.0 AS span_h "
        f"FROM {events} WHERE is_fraud = 1 GROUP BY cc_num)"
    ).fetchone()
    monthly = connection.execute(
        "SELECT strftime(to_timestamp(unix_time), '%Y-%m') AS month, count(*), "
        f"round(avg(is_fraud) * 100, 3) FROM {events} GROUP BY 1 ORDER BY 1"
    ).fetchall()
    concentration = connection.execute(
        "WITH m AS (SELECT strftime(to_timestamp(unix_time), '%Y-%m') AS month, category, "
        f"count(*) AS n FROM {events} WHERE is_fraud = 0 GROUP BY 1, 2), "
        "t AS (SELECT month, sum(n) AS total FROM m GROUP BY 1) "
        "SELECT month, arg_max(category, n), round(100.0 * max(n) / any_value(total), 1), "
        "count(*) FILTER (WHERE n >= 0.01 * total) FROM m JOIN t USING (month) "
        "GROUP BY month ORDER BY month"
    ).fetchall()
    lines = ["## Fraud episodes", ""]
    if episodes:
        cards, median_n, median_span, max_n = episodes
        lines += [
            f"- Cards with fraud: {cards}",
            f"- Median fraud transactions per card: {median_n}",
            f"- Median episode span: {median_span:.1f} h",
            f"- Max fraud transactions on one card: {max_n}",
        ]
    lines += [
        "",
        "## Volume and fraud rate by month",
        "",
        "| Month | Rows | Fraud % |",
        "|---|---|---|",
    ]
    lines += [f"| {month} | {rows} | {rate} |" for month, rows, rate in monthly]
    lines += [
        "",
        "## Legitimate-traffic category concentration by month",
        "",
        "Categories with at least 1% of a month's legitimate rows, and the largest one.",
        "",
        "| Month | Top category | Top share % | Categories >= 1% |",
        "|---|---|---|---|",
    ]
    lines += [f"| {m} | {top} | {share} | {count} |" for m, top, share, count in concentration]
    return results, lines


def report(source: Path, results: list[tuple[str, int]], details: list[str]) -> str:
    """Return the Markdown report for the check results and descriptive lines."""
    lines = [
        "# Data-quality report",
        "",
        f"Source: `{source.name}`",
        "",
        "## Checks",
        "",
        "| Check | Violations | Result |",
        "|---|---|---|",
    ]
    lines += [
        f"| {name} | {count} | {'PASS' if count == 0 else 'FAIL'} |" for name, count in results
    ]
    return "\n".join([*lines, "", *details, ""])


def main() -> int:
    """Run the checks, write the report, and return 1 if any check failed, else 0."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", type=Path, default=Path("data/processed/events_sorted.parquet"))
    parser.add_argument("--report", type=Path, default=Path("notes/data_quality.md"))
    args = parser.parse_args()
    results, details = run(args.source)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(report(args.source, results, details), encoding="utf-8", newline="\n")
    failed = [name for name, count in results if count != 0]
    print(f"{len(results) - len(failed)}/{len(results)} checks passed; report: {args.report}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
