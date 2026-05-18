"""Nightly drop file discovery with deterministic ordering."""

from __future__ import annotations

from pathlib import Path


def discover_nightly_files(base_dir: str | Path, batch_date: str) -> list[Path]:
    """Return matching claims files for a batch date (YYYY-MM-DD)."""
    stamp = batch_date.replace("-", "")
    root = Path(base_dir)
    files = sorted(root.glob(f"claims_*_{stamp}.csv"))
    return [path for path in files if path.is_file()]
