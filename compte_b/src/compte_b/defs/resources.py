from __future__ import annotations

import os
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource


DB_ENV_VAR = "UNIVERSITY_DUCKDB_PATH"
DEFAULT_DB_PATH = Path(__file__).resolve().parents[3] / "data" / "analysis.duckdb"


def _get_db_path() -> str:
    override = os.getenv(DB_ENV_VAR)
    if override:
        return override
    return str(DEFAULT_DB_PATH)


database_resource = DuckDBResource(database=_get_db_path())

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={"database": database_resource})

