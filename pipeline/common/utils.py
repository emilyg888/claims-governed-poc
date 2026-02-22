"""Utility helpers for file IO, dates, and checksums."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml


def parse_batch_date(raw_value: str) -> str:
    """Validate and normalize an input batch date as YYYY-MM-DD."""
    return datetime.strptime(raw_value, "%Y-%m-%d").date().isoformat()


def load_yaml(path: str | Path) -> dict[str, Any]:
    """Load a YAML document into a dictionary."""
    with Path(path).open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping YAML in {path}")
    return data


def load_json(path: str | Path) -> dict[str, Any]:
    """Load a JSON file into a dictionary."""
    with Path(path).open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping JSON in {path}")
    return data


def sha256_for_file(path: str | Path) -> str:
    """Compute SHA-256 checksum for manifest tracking."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def csv_row_count(path: str | Path) -> int:
    """Count CSV data rows excluding header."""
    with Path(path).open("r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        next(reader, None)
        return sum(1 for _ in reader)
