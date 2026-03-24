import csv
import io
import shutil
from datetime import date as Date, datetime, timedelta
from pathlib import Path

import dagster as dg

@dg.asset(
    name="compte_classed_data",
    required_resource_keys={"database"}
)
def compte_classed_data(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    downloads_dir = Path("/Users/daviddasilva/Projets/GitRepo")
    csv_filename = "Compte 2026 - Tableau 2.csv"
    csv_path = downloads_dir / csv_filename

    # If the exact filename is not present, attempt a small fallback match.
    if not csv_path.exists():
        candidates = sorted(downloads_dir.glob("*Compte*2026*Tableau 2*.csv"))
        if not candidates:
            candidates = sorted(downloads_dir.glob("*Tableau 2*.csv"))
        if not candidates:
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        csv_path = candidates[0]

    with context.resources.database.get_connection() as con:
        # Import directly from CSV into DuckDB.
        # read_csv_auto will infer the delimiter and column types.
        con.execute(
            '''
            CREATE OR REPLACE TABLE "compte_classed_data" AS
            SELECT
                column00 AS "Type",
                column01 AS "Date",
                column02 AS "Libellé",
                column03 AS "Montants (EUROS)",
                column04 AS "Cumul/Mois En Euros",
                column05 AS "Total en Euros"
            FROM read_csv_auto(?, delim=';', header=false, skip=1)
            WHERE column00 IS NOT NULL
            ''',
            [str(csv_path)],
        )
        imported_rows = con.execute('SELECT COUNT(*) FROM "compte_classed_data"').fetchone()[0]

    return dg.MaterializeResult(
        metadata={
            "csv_path": dg.MetadataValue.path(str(csv_path)),
            "imported_rows": dg.MetadataValue.int(imported_rows),
        }
    )

@dg.asset(
    name="compte_raw_csv_import",
    required_resource_keys={"database"}
)
def compte_raw_csv_import(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    Copy raw CSV files from Downloads into `data/raw/`.

    Matches: `4340561S033*.csv`
    """

    downloads_dir = Path("/Users/daviddasilva/Downloads")
    filename_pattern = "4340561S033*.csv"

    # Resolve repo root by locating `compte_b/pyproject.toml` in parent dirs.
    this_file = Path(__file__).resolve()
    repo_root = next(
        (
            parent
            for parent in this_file.parents
            if (parent / "compte_b" / "pyproject.toml").exists()
        ),
        None,
    )
    if repo_root is None:
        raise RuntimeError("Could not locate repo root (missing compte_b/pyproject.toml)")
    destination_dir = repo_root / "compte_b" / "data" / "raw"

    destination_dir.mkdir(parents=True, exist_ok=True)

    matches = sorted(downloads_dir.glob(filename_pattern))
    if not matches:
        raise FileNotFoundError(
            f"No files found in {downloads_dir} matching pattern {filename_pattern}"
        )

    copied_files: list[str] = []
    for src_path in matches:
        dst_path = destination_dir / src_path.name
        shutil.copy2(src_path, dst_path)
        copied_files.append(src_path.name)

    context.log.info(
        f"Copied {len(copied_files)} file(s) from {downloads_dir} to {destination_dir}"
    )

    # ---- DuckDB load into "raw_csv_import" -------------------------------------
    # Requirements:
    # - Remove the first 7 lines of each CSV prior to importing transactions.
    # - Line 4 => date-import; line 5 => total amount-import.
    # - Anti-duplication:
    #   - last_date_import = max("date-import") in "raw_csv_import"
    #   - delete rows where "date" >= last_date_import - 2 days
    #   - insert from each CSV only rows where "date" >= same threshold

    def parse_euro_amount(amount_str: str) -> float:
        s = amount_str.strip().replace("\xa0", " ")
        s = s.replace(" ", "")
        s = s.replace(",", ".")
        return float(s)

    def parse_date_fr(date_str: str) -> Date:
        return datetime.strptime(date_str.strip(), "%d/%m/%Y").date()

    def parse_metadata_and_transactions(
        csv_path: Path,
    ) -> tuple[Date, float, list[tuple[Date, str, float]]]:
        # We rely on the user's stated CSV structure:
        # - 1..3: metadata
        # - 4: "Date;DD/MM/YYYY"
        # - 5: "Solde (EUROS);<amount_euros>"
        # - 1 extra line (6): often blank
        # - 7: transaction header
        # - 8..: transactions (Date;Libellé;Montant(EUROS))
        lines = csv_path.read_text(encoding="utf-8", errors="replace").splitlines()
        if len(lines) < 8:
            raise ValueError(f"CSV {csv_path} does not contain enough lines for import")

        first7 = lines[:7]
        line4 = first7[3]
        line5 = first7[4]

        try:
            date_import_raw = line4.split(";", 1)[1].strip()
            total_amount_raw = line5.split(";", 1)[1].strip()
        except IndexError as exc:
            raise ValueError(f"CSV {csv_path} has unexpected metadata lines (4/5)") from exc

        date_import = parse_date_fr(date_import_raw)
        total_amount_import = parse_euro_amount(total_amount_raw)

        # Transactions begin after removing the first 7 lines.
        tx_lines = "\n".join(lines[7:])
        reader = csv.reader(io.StringIO(tx_lines), delimiter=";", quotechar='"')

        transactions: list[tuple[Date, str, float]] = []
        for row in reader:
            if not row:
                continue
            if len(row) < 3:
                # Skip malformed lines to avoid breaking the whole import.
                continue

            tx_date = parse_date_fr(row[0])
            libelle = row[1].strip()
            amount_euros = parse_euro_amount(row[2])
            transactions.append((tx_date, libelle, amount_euros))

        return date_import, total_amount_import, transactions


    db_path = repo_root / "compte_b" / "data" / "analysis.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with context.resources.database.get_connection() as con:
        # Create table if needed (names with spaces/dashes must be quoted in SQL).
        # Column order: ensure "date" is the first column, as requested.
        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS "compte_raw_csv_import" (
                "date" DATE,
                "libelle" VARCHAR,
                "montant_euros" DOUBLE,
                "date-import" DATE,
                "total amount-import" DOUBLE
            )
            '''
        )

        # Step 6: insert only after anti-dup deletion.
        for filename in copied_files:

            ## Anti-duplication ##
            last_date_import_row = con.execute('SELECT max("date-import") FROM "compte_raw_csv_import"').fetchone()
            last_date_import: Date | None = last_date_import_row[0]  # type: ignore[assignment]

            threshold_date = (
                (last_date_import - timedelta(days=2)) if last_date_import is not None else Date(1900, 1, 1)
            )

            deleted_count = con.execute(
                'SELECT COUNT(*) FROM "compte_raw_csv_import" WHERE "date" >= ?',
                [threshold_date],
            ).fetchone()[0]
            con.execute('DELETE FROM "raw_csv_import" WHERE "date" >= ?', [threshold_date])

            total_inserted = 0
            per_file_inserted: dict[str, int] = {}
            ## Anti-duplication ##

            csv_path = destination_dir / filename
            date_import, total_amount_import, transactions = parse_metadata_and_transactions(csv_path)

            rows_to_insert: list[tuple[Date, str, float, Date, float]] = [
                (tx_date, libelle, amount_euros, date_import, total_amount_import)
                for (tx_date, libelle, amount_euros) in transactions
                if tx_date >= threshold_date
            ]
            
            rows_to_insert.sort(key=lambda r: r[0])  # r[0] = tx_date

            if not rows_to_insert:
                per_file_inserted[filename] = 0
                continue

            con.executemany(
                '''
                INSERT INTO "compte_raw_csv_import"
                    ("date", "libelle", "montant_euros", "date-import", "total amount-import")
                VALUES (?, ?, ?, ?, ?)
                ''',
                rows_to_insert,
            )
            inserted = len(rows_to_insert)
            per_file_inserted[filename] = inserted
            total_inserted += inserted

        context.log.info(
            f'DuckDB load into "compte_raw_csv_import": threshold={threshold_date}, deleted={deleted_count}, inserted={total_inserted}'
        )

    return dg.MaterializeResult(
        metadata={
            "copied_count": dg.MetadataValue.int(len(copied_files)),
            "destination_dir": dg.MetadataValue.path(str(destination_dir)),
            "copied_files": dg.MetadataValue.json(copied_files),
            "duckdb_path": dg.MetadataValue.path(str(db_path)),
            "compte_raw_csv_import_threshold_date": dg.MetadataValue.text(str(threshold_date)),
            "compte_raw_csv_import_total_inserted": dg.MetadataValue.int(total_inserted),
            "compte_raw_csv_import_per_file_inserted": dg.MetadataValue.json(per_file_inserted),
        }
    )

