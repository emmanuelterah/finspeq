"""Report lines longer than 100 characters in source files and Markdown code blocks."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

MAX_LENGTH = 100
SOURCE_SUFFIXES = frozenset(
    {".py", ".lua", ".sql", ".toml", ".yml", ".yaml", ".cfg", ".ini", ".sh"}
)
MARKDOWN_SUFFIX = ".md"
SKIPPED_DIRS = frozenset(
    {
        ".git",
        ".venv",
        "venv",
        "data",
        "models",
        "__pycache__",
        ".mypy_cache",
        ".ruff_cache",
        ".pytest_cache",
    }
)
FENCE = "```"


def iter_files(root: Path) -> Iterator[Path]:
    """Yield source and Markdown files under root in sorted order, skipping SKIPPED_DIRS."""
    for path in sorted(root.rglob("*")):
        if any(part in SKIPPED_DIRS for part in path.relative_to(root).parts):
            continue
        if path.is_file() and path.suffix in SOURCE_SUFFIXES | {MARKDOWN_SUFFIX}:
            yield path


def long_lines(path: Path) -> Iterator[tuple[int, int]]:
    """Yield (line number, length) for each over-long line of path.

    Every line is checked in source files. Only lines inside fenced code blocks are checked in
    Markdown files.
    """
    markdown = path.suffix == MARKDOWN_SUFFIX
    in_fence = False
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if markdown and line.startswith(FENCE):
            in_fence = not in_fence
            continue
        if (in_fence or not markdown) and len(line) > MAX_LENGTH:
            yield number, len(line)


def main(root: Path) -> int:
    """Print each over-long line as path:line:length and return 1 if any exist, else 0."""
    found = False
    for path in iter_files(root):
        for number, length in long_lines(path):
            print(f"{path.relative_to(root)}:{number}: {length} > {MAX_LENGTH}")
            found = True
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main(Path(__file__).resolve().parent.parent))
