"""Convert the raw Sparkov CSV into the Parquet file used by every later step.

Keeps modelling columns, drops personal fields (SSN, names, street), and reads every column with
an explicit type.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

RAW_COLUMNS: dict[str, str] = {
    "ssn": "VARCHAR",
    "cc_num": "VARCHAR",
    "first": "VARCHAR",
    "last": "VARCHAR",
    "gender": "VARCHAR",
    "street": "VARCHAR",
    "city": "VARCHAR",
    "state": "VARCHAR",
    "zip": "VARCHAR",
    "lat": "DOUBLE",
    "long": "DOUBLE",
    "city_pop": "INTEGER",
    "job": "VARCHAR",
    "dob": "DATE",
    "acct_num": "VARCHAR",
    "profile": "VARCHAR",
    "trans_num": "VARCHAR",
    "trans_date": "VARCHAR",
    "trans_time": "VARCHAR",
    "unix_time": "BIGINT",
    "category": "VARCHAR",
    "amt": "DOUBLE",
    "is_fraud": "TINYINT",
    "merchant": "VARCHAR",
    "merch_lat": "DOUBLE",
    "merch_long": "DOUBLE",
}
KEPT_COLUMNS = (
    "trans_num",
    "cc_num",
    "acct_num",
    "unix_time",
    "category",
    "amt",
    "merchant",
    "merch_lat",
    "merch_long",
    "lat",
    "long",
    "city_pop",
    "gender",
    "dob",
    "job",
    "state",
    "zip",
    "profile",
    "is_fraud",
)


def convert(source: Path, target: Path, memory_limit: str) -> int:
    """Write the kept columns of source to target as zstd Parquet and return the row count.

    Writes to a temporary file next to target and renames it into place when complete.

    Args:
        source: Raw CSV file with a header row.
        target: Parquet file to create or replace.
        memory_limit: DuckDB memory limit, for example "2GB".
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".parquet.tmp")
    columns = "{" + ", ".join(f"'{k}': '{v}'" for k, v in RAW_COLUMNS.items()) + "}"
    query = (
        f"COPY (SELECT {', '.join(KEPT_COLUMNS)} FROM read_csv('{source.as_posix()}', "
        f"header = true, columns = {columns})) TO '{temporary.as_posix()}' "
        "(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 500000)"
    )
    connection = duckdb.connect()
    connection.execute(f"SET memory_limit = '{memory_limit}'")
    connection.execute("SET preserve_insertion_order = false")
    connection.execute(query)
    rows = connection.execute(f"SELECT count(*) FROM '{temporary.as_posix()}'").fetchone()
    temporary.replace(target)
    return int(rows[0]) if rows else 0


def main() -> int:
    """Parse arguments, run the conversion, print the row count, and return 0."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", type=Path, default=Path("data/raw/combined_transactions.csv"))
    parser.add_argument("--target", type=Path, default=Path("data/processed/transactions.parquet"))
    parser.add_argument("--memory-limit", default="2GB")
    args = parser.parse_args()
    rows = convert(args.source, args.target, args.memory_limit)
    print(f"wrote {rows} rows to {args.target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
