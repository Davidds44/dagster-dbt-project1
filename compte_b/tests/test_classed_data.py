import csv
from datetime import datetime
from pathlib import Path

import dagster as dg
import pytest
from dagster_duckdb import DuckDBResource

from compte_b.defs.assets.assets import classed_data


@pytest.fixture
def file_example_input() -> Path:
    return Path(__file__).parent / "data" / "input_exemple.csv"

@pytest.fixture
def row_example_output() -> list[tuple[str, str, str, float, float, float]]:
    
    data = """Type;Date ;Libellé;Montants (EUROS);Cumul/Mois En Euros;Total en Euros
Normal;18/12/2023;ACHAT CB KLEIN D ALSACE 16.12.23 CARTE NUMERO                469;-10,7;4557,58;6867,93
"""
    reader = csv.DictReader(data.strip().splitlines(), delimiter=";")
    rows: list[tuple[str, str, str, float, float, float]] = []
    for row in reader:
        rows.append(
            (
                row["Type"],
                datetime.strptime(row["Date "].strip(), "%d/%m/%Y").date().isoformat(),
                row["Libellé"],
                float(row["Montants (EUROS)"].replace(",", ".")),
                float(row["Cumul/Mois En Euros"].replace(",", ".")),
                float(row["Total en Euros"].replace(",", ".")),
            )
        )
    return rows

# tests/test_assets.py::test_classed_data
def test_classed_data(
    file_example_input: Path,
    row_example_output: list[tuple[str, str, str, float, float, float]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    # Keep Dagster asset discovery simple by using the expected Tableau 2 filename.
    target_csv = input_dir / "Compte 2026 - Tableau 2.csv"
    target_csv.write_text(file_example_input.read_text(encoding="utf-8"), encoding="utf-8")

    db_path = tmp_path / "test.duckdb"
    monkeypatch.setenv("COMPTE_B_INPUT_DIR", str(input_dir))
    monkeypatch.setenv("COMPTE_B_DUCKDB_PATH", str(db_path))

    context = dg.build_asset_context(resources={"database": DuckDBResource(database=str(db_path))})
    classed_data(context)

    with DuckDBResource(database=str(db_path)).get_connection() as con:
        raw_rows = con.execute(
            '''
            SELECT
                "Type",
                "Date",
                "Libellé",
                "Montants (EUROS)",
                "Cumul/Mois En Euros",
                "Total en Euros"
            FROM "classed_data"
            ORDER BY "Date", "Libellé"
            '''
        ).fetchall()


    # L’asset parcourt trois noms de fichiers (2026–2024) ; avec un seul CSV présent,
    # le même fichier est importé trois fois.
    assert raw_rows == row_example_output * 3
