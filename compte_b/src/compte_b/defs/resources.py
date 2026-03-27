
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource
from compte_b.defs.project import dbt_project


DB_ENV_VAR = "COMPTE_B_DUCKDB_PATH"
DEFAULT_DB_PATH = Path(__file__).resolve().parents[3] / "data" / "compte_b.duckdb"
SQLITE_ENV_VAR = "COMPTE_B_SQLITE_PATH"
DEFAULT_SQLITE_PATH = Path(__file__).resolve().parents[3] / "data" / "partage.sqlite"


def _get_db_path() -> str:
    override = os.getenv(DB_ENV_VAR)
    if override:
        return override
    return str(DEFAULT_DB_PATH)


def _get_sqlite_path() -> str:
    override = os.getenv(SQLITE_ENV_VAR)
    if override:
        return override
    return str(DEFAULT_SQLITE_PATH)


class SQLiteResource(dg.ConfigurableResource):
    database: str

    @contextmanager
    def get_connection(self):
        db_path = Path(self.database)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(db_path)
        try:
            yield connection
        finally:
            connection.close()


database_resource = DuckDBResource(database=_get_db_path())
sqlite_resource = SQLiteResource(database=_get_sqlite_path())

dbt_resource = DbtCliResource(
    project_dir=dbt_project
)

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "database": database_resource,
            "sqlite": sqlite_resource,
            "dbt": dbt_resource
        }
    )

