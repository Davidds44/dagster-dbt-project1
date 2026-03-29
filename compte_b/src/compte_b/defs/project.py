import os
from pathlib import Path

from dagster_dbt import DbtProject

from compte_b.defs.sqlite_bootstrap import ensure_partage_sqlite_stub

# dbt resolves profile paths relative to the process cwd. Dagster's DbtProjectComponent runs dbt from a
# mirrored copy under .local_defs_state/..., so a relative DuckDB path breaks. Pin the same file the
# DuckDB resource uses (see resources.py / COMPTE_B_DUCKDB_PATH).
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DUCKDB = _PROJECT_ROOT / "data" / "compte_b.duckdb"
os.environ.setdefault("COMPTE_B_DUCKDB_PATH", str(_DEFAULT_DUCKDB))

_DEFAULT_SQLITE = _PROJECT_ROOT / "data" / "partage.sqlite"
os.environ.setdefault("COMPTE_B_SQLITE_PATH", str(_DEFAULT_SQLITE))

# dbt does not open the SQLite resource; ensure stub exists before manifest/build so sqlite_scan succeeds.
ensure_partage_sqlite_stub(os.environ["COMPTE_B_SQLITE_PATH"])

dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("../../../..", "analysis").resolve()
)

dbt_project.prepare_if_dev()
