from pathlib import Path

import dagster as dg


def _repo_root() -> Path:
    """Locate repo root by searching for compte_b/pyproject.toml."""

    this_file = Path(__file__).resolve()
    root = next(
        (
            parent
            for parent in this_file.parents
            if (parent / "compte_b" / "pyproject.toml").exists()
        ),
        None,
    )
    if root is None:
        raise RuntimeError("Could not locate repo root (missing compte_b/pyproject.toml)")
    return root


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
        sqlite_path = (_repo_root() / "compte_b" / sqlite_path).resolve()

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
