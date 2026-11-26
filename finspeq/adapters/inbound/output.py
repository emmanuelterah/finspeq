"""JSON output of command results: conversion of results to plain data, printing, files."""

from __future__ import annotations

import dataclasses
import json
from enum import Enum
from pathlib import Path


def to_plain(value: object) -> object:
    """Return value as JSON-compatible data.

    Dataclasses become dicts of their fields and public properties, enums become their
    values, and tuples become lists.
    """
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        plain = {f.name: to_plain(getattr(value, f.name)) for f in dataclasses.fields(value)}
        for name, attribute in vars(type(value)).items():
            if isinstance(attribute, property) and not name.startswith("_"):
                plain[name] = to_plain(getattr(value, name))
        return plain
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, tuple | list):
        return [to_plain(item) for item in value]
    if isinstance(value, dict):
        return {str(key): to_plain(item) for key, item in value.items()}
    return value


def write_json(path: Path, payload: object) -> None:
    """Write payload, converted with to_plain, to path as indented JSON with LF line endings."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(to_plain(payload), indent=2) + "\n"
    path.write_text(text, encoding="utf-8", newline="\n")


def render(result: object) -> str:
    """Return result, converted with to_plain, as indented JSON."""
    return json.dumps(to_plain(result), indent=2)
