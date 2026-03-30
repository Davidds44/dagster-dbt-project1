from pathlib import Path

import dagster as dg
from compte_b.env_paths import resolve_compte_b_project_root


def _repo_root() -> Path:
    """Racine du projet `compte_b` (contient `pyproject.toml` et `data/`)."""
    return resolve_compte_b_project_root(start=Path(__file__))


@dg.asset(
    name="partage_sqlite",
    deps=["stg_operations_with_type"],
    required_resource_keys={"database", "sqlite"},
    group_name="Marts",
)
def partage_sqlite(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    with context.resources.database.get_connection() as con:
        df = con.execute("SELECT * FROM stg_operations_with_type").df()

    if df.empty:
        raise RuntimeError("No rows found in `stg_operations_with_type` to export to SQLite.")

    sqlite_path = Path(context.resources.sqlite.database)
    if not sqlite_path.is_absolute():
        sqlite_path = (_repo_root() / sqlite_path).resolve()

    table_name = "partage_sqlite_table"

    with context.resources.sqlite.get_connection() as sqlite_con:
        df.to_sql(table_name, sqlite_con, if_exists="replace", index=False)

    return dg.MaterializeResult(
        metadata={
            "rows": dg.MetadataValue.int(len(df)),
            "sqlite_path": dg.MetadataValue.path(str(sqlite_path)),
            "table_name": dg.MetadataValue.text(table_name),
        }
    )
