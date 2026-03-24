import os
from pathlib import Path

from dagster_dbt import DbtProject

# dbt resolves profile paths relative to the process cwd. Dagster's DbtProjectComponent runs dbt from a
# mirrored copy under .local_defs_state/..., so a relative DuckDB path breaks. Pin the same file the
# DuckDB resource uses (see resources.py / COMPTE_B_DUCKDB_PATH).
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DUCKDB = _PROJECT_ROOT / "data" / "compte_b.duckdb"
os.environ.setdefault("COMPTE_B_DUCKDB_PATH", str(_DEFAULT_DUCKDB))

dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("../../../..", "analysis").resolve()
)

dbt_project.prepare_if_dev()
