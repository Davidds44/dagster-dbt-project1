
import os
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource
from compte_b.defs.project import dbt_project


DB_ENV_VAR = "UNIVERSITY_DUCKDB_PATH"
DEFAULT_DB_PATH = Path(__file__).resolve().parents[3] / "data" / "compte_b.duckdb"


def _get_db_path() -> str:
    override = os.getenv(DB_ENV_VAR)
    if override:
        return override
    return str(DEFAULT_DB_PATH)


database_resource = DuckDBResource(database=_get_db_path())

dbt_resource = DbtCliResource(
    project_dir=dbt_project
)

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "database": database_resource,
            "dbt": dbt_resource
        }
    )

