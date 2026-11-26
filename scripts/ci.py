"""Run every CI check in order and stop at the first failure."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable


def steps() -> list[tuple[str, list[str]]]:
    """Return the CI steps as (name, command) pairs in execution order."""
    lint_imports = shutil.which("lint-imports") or "lint-imports"
    return [
        ("format", [PYTHON, "-m", "ruff", "format", "--check", "."]),
        ("lint", [PYTHON, "-m", "ruff", "check", "."]),
        ("types", [PYTHON, "-m", "mypy"]),
        ("imports", [lint_imports]),
        ("line length", [PYTHON, "scripts/check_line_length.py"]),
        (
            "tests",
            [
                PYTHON,
                "-m",
                "pytest",
                "tests/unit",
                "tests/contract",
                "tests/adapters",
                "tests/experiments",
            ],
        ),
    ]


def main() -> int:
    """Run each step from the repository root; return the first non-zero exit code, or 0."""
    for name, command in steps():
        print(f"==> {name}", flush=True)
        result = subprocess.run(command, cwd=ROOT, check=False)
        if result.returncode != 0:
            print(f"FAILED: {name}", flush=True)
            return result.returncode
    print("All checks passed.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
