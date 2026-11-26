"""Write the canonical event file: modelling columns ordered by (unix_time, trans_num).

Adds a zero-based `seq` column (row number in canonical order), verifies the order, and writes
`<target stem>.manifest.json` next to the output with the row count, time range, fraud count,
and SHA-256.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import duckdb

EVENT_COLUMNS = (
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
HASH_CHUNK_BYTES = 1 << 20


def sort_events(source: Path, target: Path, memory_limit: str, temp_dir: Path) -> None:
    """Write source's event columns to target in canonical order with a `seq` column.

    Writes to a temporary file next to target and renames it into place when complete.

    Args:
        source: Parquet file produced by convert_csv_to_parquet.py.
        target: Parquet file to create or replace.
        memory_limit: DuckDB memory limit, for example "2GB".
        temp_dir: Directory DuckDB spills to when the sort exceeds memory_limit.
    """
    temporary = target.with_suffix(".parquet.tmp")
    columns = ", ".join(EVENT_COLUMNS)
    connection = duckdb.connect()
    connection.execute(f"SET memory_limit = '{memory_limit}'")
    connection.execute(f"SET temp_directory = '{temp_dir.as_posix()}'")
    connection.execute("SET preserve_insertion_order = true")
    connection.execute(
        f"COPY (SELECT row_number() OVER (ORDER BY unix_time, trans_num) - 1 AS seq, {columns} "
        f"FROM '{source.as_posix()}' ORDER BY unix_time, trans_num) "
        f"TO '{temporary.as_posix()}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 250000)"
    )
    temporary.replace(target)


def order_violations(target: Path) -> int:
    """Return the number of rows whose (unix_time, trans_num) is not above the previous row's."""
    query = (
        "SELECT count(*) FROM (SELECT unix_time, trans_num, "
        "lag(unix_time) OVER (ORDER BY seq) AS prev_time, "
        "lag(trans_num) OVER (ORDER BY seq) AS prev_tx "
        f"FROM '{target.as_posix()}') "
        "WHERE prev_time IS NOT NULL AND (unix_time < prev_time "
        "OR (unix_time = prev_time AND trans_num <= prev_tx))"
    )
    row = duckdb.connect().execute(query).fetchone()
    return int(row[0]) if row else -1


def file_sha256(path: Path) -> str:
    """Return the hexadecimal SHA-256 of the file's bytes."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(HASH_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def manifest(target: Path) -> dict[str, object]:
    """Return the manifest fields for target: name, rows, frauds, time range, SHA-256, versions."""
    row = (
        duckdb.connect()
        .execute(
            "SELECT count(*), sum(is_fraud), min(unix_time), max(unix_time), max(seq) "
            f"FROM '{target.as_posix()}'"
        )
        .fetchone()
    )
    if row is None:
        raise RuntimeError(f"no rows in {target}")
    rows, frauds, min_time, max_time, max_seq = (int(value) for value in row)
    return {
        "file": target.name,
        "rows": rows,
        "fraud_rows": frauds,
        "min_unix_time": min_time,
        "max_unix_time": max_time,
        "max_seq": max_seq,
        "columns": ["seq", *EVENT_COLUMNS],
        "order": ["unix_time", "trans_num"],
        "sha256": file_sha256(target),
        "duckdb_version": duckdb.__version__,
    }


def manifest_path_for(target: Path) -> Path:
    """Return the manifest path for target, for example events_sorted.manifest.json."""
    return target.with_name(f"{target.stem}.manifest.json")


def main() -> int:
    """Sort, verify order, write the manifest, print it, and return 0; return 1 on bad order."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", type=Path, default=Path("data/processed/transactions.parquet"))
    parser.add_argument("--target", type=Path, default=Path("data/processed/events_sorted.parquet"))
    parser.add_argument("--memory-limit", default="2GB")
    parser.add_argument("--temp-dir", type=Path, default=Path("data/tmp"))
    args = parser.parse_args()
    sort_events(args.source, args.target, args.memory_limit, args.temp_dir)
    violations = order_violations(args.target)
    if violations != 0:
        print(f"order violations: {violations}", file=sys.stderr)
        return 1
    fields = manifest(args.target)
    manifest_path = manifest_path_for(args.target)
    manifest_path.write_text(json.dumps(fields, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps(fields, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
