"""Command-line arguments shared by commands that read history: selection and disorder."""

from __future__ import annotations

import argparse
import calendar
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from finspeq.application.disorder import Disorder
from finspeq.application.messages import TimeRange
from finspeq.application.replay import ALL_CARDS, CardSlice
from finspeq.domain.model import EventTime

DEFAULT_SOURCE = Path("data/processed/events_sorted.parquet")
ALL_SLICE_TEXT = "all"


@dataclass(frozen=True, slots=True)
class EventSelection:
    """Historical events chosen by source file, time range, and card slice."""

    source: Path
    time_range: TimeRange | None
    cards: CardSlice


def parse_slice(text: str) -> CardSlice:
    """Return the card slice written as "modulus:remainder", or every card for "all"."""
    if text == ALL_SLICE_TEXT:
        return ALL_CARDS
    modulus, remainder = text.split(":")
    return CardSlice(int(modulus), int(remainder))


def add_selection(parser: argparse.ArgumentParser) -> None:
    """Add --source, --from-date, --to-date, and --slice to parser."""
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--from-date", help="first day, YYYY-MM-DD (UTC)")
    parser.add_argument("--to-date", help="day after the last, YYYY-MM-DD (UTC)")
    parser.add_argument("--slice", default=ALL_SLICE_TEXT, help='"modulus:remainder" or "all"')


def selection_from(args: argparse.Namespace) -> EventSelection:
    """Return the EventSelection described by --source, --from-date, --to-date, --slice.

    Raises:
        SystemExit: Only one of --from-date and --to-date is given.
    """
    if (args.from_date is None) != (args.to_date is None):
        raise SystemExit("give both --from-date and --to-date, or neither")
    time_range = None
    if args.from_date is not None:
        time_range = TimeRange(_day(args.from_date), _day(args.to_date))
    return EventSelection(args.source, time_range, parse_slice(args.slice))


def add_disorder(parser: argparse.ArgumentParser) -> None:
    """Add the arguments of a Disorder to parser; the defaults give canonical order.

    The arguments are --delayed-fraction, --mean-delay-s, --max-delay-s, --duplicate-fraction,
    and --disorder-seed.
    """
    parser.add_argument("--delayed-fraction", type=float, default=0.0)
    parser.add_argument("--mean-delay-s", type=float, default=0.0)
    parser.add_argument("--max-delay-s", type=int, default=0)
    parser.add_argument("--duplicate-fraction", type=float, default=0.0)
    parser.add_argument("--disorder-seed", type=int, default=0)


def disorder_from(args: argparse.Namespace) -> Disorder:
    """Return the Disorder described by the arguments added by add_disorder."""
    return Disorder(
        delayed_fraction=args.delayed_fraction,
        mean_delay_s=args.mean_delay_s,
        max_delay_s=args.max_delay_s,
        duplicate_fraction=args.duplicate_fraction,
        seed=args.disorder_seed,
    )


def _day(text: str) -> EventTime:
    """Return 00:00:00 UTC of the day written as YYYY-MM-DD."""
    return EventTime(calendar.timegm(date.fromisoformat(text).timetuple()))
