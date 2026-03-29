"""Idempotent stub for partage.sqlite so dbt sqlite_scan and Dagster export never hit a missing table."""

from __future__ import annotations

import sqlite3
from pathlib import Path

# Aligned with stg_operations_with_type / Integration_SQLite pandas export and stg_classed_data.sql.
_CREATE_PARTAGE_SQLITE = """
CREATE TABLE IF NOT EXISTS "partage_sqlite_table" (
    "Type" TEXT,
    "date" TEXT,
    "libelle" TEXT,
    "montant_euros" REAL
)
"""


def ensure_partage_sqlite_stub(db_path: str | Path) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(_CREATE_PARTAGE_SQLITE)
        conn.commit()
