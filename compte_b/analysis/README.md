# Analysis (dbt + DuckDB)

dbt project colocated with `compte_b`. Database file: `data/analysis.duckdb` under `compte_b/` (same level as `analysis/`; `*.duckdb` is gitignored in `compte_b/.gitignore`).

## Run dbt

From the `compte_b` directory:

```bash
export DBT_PROFILES_DIR="$PWD/analysis"
uv run dbt debug --project-dir analysis
uv run dbt build --project-dir analysis
```

Or from `compte_b/analysis`:

```bash
export DBT_PROFILES_DIR="$PWD"
uv run dbt debug
uv run dbt build
```
