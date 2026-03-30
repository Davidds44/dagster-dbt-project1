# compte_b

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

### Environment variables

| Variable | Role |
| --- | --- |
| `COMPTE_B_DUCKDB_PATH` | Path to the main DuckDB file (default: `data/compte_b.duckdb` under the package root). |
| `COMPTE_B_SQLITE_PATH` | Path to SQLite `partage` (default: `data/partage.sqlite`). |
| `DOWNLOADS_DIR` | Folder read by `raw_csv_import` for `4340561S033*.csv` (default: `~/Downloads`). In Docker, use `/downloads` with a bind mount. |
| `COMPTE_B_INPUT_DIR` | Optional override for *Compte 20xx - Tableau 2*.csv (default: `data/input`). |

Place the *Compte 20xx* CSVs under `data/input/` (or set `COMPTE_B_INPUT_DIR`).

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)

## accesing the sqlite data

uv run streamlit run streamlit/app.py

## launching the dbt commandes

export COMPTE_B_DUCKDB_PATH="$PWD/data/compte_b.duckdb"
uv run dbt parse --project-dir analysis --profiles-dir analysis
uv run dbt build --project-dir analysis --profiles-dir analysis

## looking at the duckdb data

uv run duckdb -ui

## Docker

Build and open an interactive shell (from the `compte_b` directory):

```bash
docker compose build
docker compose run --rm -it compte_b bash
```

Inside the container, project root is `/app`, `DOWNLOADS_DIR` is set to `/downloads` (macOS `~/Downloads` mounted read-only), and `./data` on the host is mounted at `/app/data` (so `data/input` stays editable). The image also includes a copy of `data/input` from the build context.

Examples:

```bash
dg dev
# UI: http://localhost:3000 (port 3000 is published)
```

```bash
uv run streamlit run streamlit/app.py --server.port 8501 --server.address 0.0.0.0
```

For Streamlit, add `8501:8501` under `ports` in `docker-compose.yml` or use `docker compose run --rm -p 8501:8501 compte_b bash`.

Plain Docker (without Compose):

```bash
docker build -t compte_b:local .
docker run --rm -it -p 3000:3000 -v "$PWD/data:/app/data" -v "$HOME/Downloads:/downloads:ro" -w /app compte_b:local bash
```
