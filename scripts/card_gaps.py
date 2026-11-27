"""Distribution of the time between consecutive transactions of the same card, by label.

Reads the canonical event file with DuckDB and prints, for legitimate and fraudulent
transactions, the share whose previous same-card transaction is at most 1 s, 10 s, 60 s,
10 min, and 1 h earlier, plus gap percentiles in seconds.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

QUERY = """
WITH gaps AS (
    SELECT is_fraud,
           unix_time - lag(unix_time) OVER (PARTITION BY cc_num ORDER BY unix_time, trans_num)
               AS gap
    FROM read_parquet(?)
)
SELECT is_fraud,
       count(*) AS transactions,
       avg(CASE WHEN gap <= 1 THEN 1 ELSE 0 END) AS within_1s,
       avg(CASE WHEN gap <= 10 THEN 1 ELSE 0 END) AS within_10s,
       avg(CASE WHEN gap <= 60 THEN 1 ELSE 0 END) AS within_60s,
       avg(CASE WHEN gap <= 600 THEN 1 ELSE 0 END) AS within_10min,
       avg(CASE WHEN gap <= 3600 THEN 1 ELSE 0 END) AS within_1h,
       quantile_cont(gap, 0.01) AS p1_s,
       quantile_cont(gap, 0.10) AS p10_s,
       quantile_cont(gap, 0.50) AS p50_s
FROM gaps
WHERE gap IS NOT NULL
GROUP BY is_fraud
ORDER BY is_fraud
"""


def main() -> None:
    """Run the query and write the result as JSON."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", type=Path, default=Path("data/processed/events_sorted.parquet"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    connection = duckdb.connect()
    cursor = connection.execute(QUERY, [args.source.as_posix()])
    names = [column[0] for column in cursor.description]
    rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    report = {("fraud" if row.pop("is_fraud") else "legitimate"): row for row in rows}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(report, indent=2, default=float) + "\n"
    args.output.write_text(text, encoding="utf-8", newline="\n")
    print(text, end="")


if __name__ == "__main__":
    main()
