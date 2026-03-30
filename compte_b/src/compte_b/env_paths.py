"""Centralized paths and env vars for local runs, monorepo layout, and Docker."""

from __future__ import annotations

import os
from pathlib import Path

DOWNLOADS_DIR_ENV = "DOWNLOADS_DIR"


def get_downloads_dir() -> Path:
    """User Downloads (or bind mount in Docker). Override with DOWNLOADS_DIR."""
    override = os.environ.get(DOWNLOADS_DIR_ENV)
    if override:
        return Path(override).expanduser().resolve()
    return (Path.home() / "Downloads").resolve()


def resolve_compte_b_project_root(*, start: Path) -> Path:
    """Directory that contains `pyproject.toml` and `src/compte_b` (e.g. `compte_b/` or `/app` in Docker).

    Works for:
    - Standalone package: ``.../compte_b/pyproject.toml``
    - Does not depend on a parent folder named ``compte_b`` (fixes flat Docker layout).
    """
    start = start.resolve()
    for parent in start.parents:
        if (parent / "pyproject.toml").is_file() and (parent / "src" / "compte_b").is_dir():
            return parent
    raise RuntimeError(
        "Could not locate compte_b project root (expected pyproject.toml + src/compte_b)."
    )


def get_classed_input_dir(project_root: Path) -> Path:
    """Directory with *Compte 20xx - Tableau 2*.csv (default: ``data/input``)."""
    override = os.environ.get("COMPTE_B_INPUT_DIR")
    if override:
        return Path(override).expanduser().resolve()
    return (project_root / "data" / "input").resolve()
