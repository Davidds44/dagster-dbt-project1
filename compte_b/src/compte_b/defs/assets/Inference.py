import re
from pathlib import Path

import dagster as dg
import mlflow
import mlflow.sklearn
import pandas as pd


@dg.asset(
    name="extract_inferenced_data",
    group_name="Marts",
    deps=["stg_operations_with_type"],
    required_resource_keys={"database"},
)
def extract_inferenced_data(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    # 1) Charger les données inférées depuis DuckDB
    with context.resources.database.get_connection() as con:
        ops = con.execute("SELECT * FROM stg_operations_with_type").df()

    # 2) Export CSV dans data/export
    export_dir = _repo_root() / "compte_b" / "data" / "export"
    export_dir.mkdir(parents=True, exist_ok=True)
    export_filename = f"extract_inferenced_data_{pd.Timestamp.now().strftime('%Y-%m-%d')}.csv"
    export_path = export_dir / export_filename

    # Tri "meilleur" pour un export opérationnel : du plus récent au plus ancien.
    # (On convertit en datetime pour être robuste si DuckDB renvoie un type inattendu.)
    ops["date"] = pd.to_datetime(ops["date"], errors="coerce")
    ops.sort_values(by="date", ascending=False, inplace=True)

    # Format d'export demandé : date en DD/MM/YYYY et décimales avec virgule.
    ops["date"] = ops["date"].dt.strftime("%d/%m/%Y")
    ops["montant_euros"] = ops["montant_euros"].map(lambda x: f"{float(x):.2f}".replace(".", ","))

    ops.to_csv(export_path, index=False, sep=";")

    return dg.MaterializeResult(
        metadata={
            "rows": dg.MetadataValue.int(len(ops)),
            "export_csv_path": dg.MetadataValue.path(str(export_path)),
        }
    )




def clean_text(t: object) -> str:
    """
    Nettoyage minimal inspiré du notebook.

    Doit rester cohérent avec `clean_text` utilisé lors de l'entraînement
    et avec la colonne `"Libellé"` de `stg_classed_data`.
    """

    s = "" if t is None else str(t)
    s = s.lower()
    # Aligné sur `stg_classed_data.sql` (REGEXP_REPLACE sur la colonne "Libellé")
    s = re.sub(r"\s\d{2}h\d{2}", " ", s)
    s = re.sub(r"\s\d{2}\.\d{2}\.\d{2}.", " ", s)
    s = re.sub(r"\s\d{2}/\d{2}/\d{2}.", " ", s)

    # Suppression de tous les chiffres (équivalent à `[0-9]+`, flags globaux)
    s = re.sub(r"[0-9]+", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _repo_root() -> Path:
    """Repère la racine du repo via `compte_b/pyproject.toml`."""

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


def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str:
    """Retourne le premier nom de colonne correspondant (tolérance casse)."""

    for c in candidates:
        if c in df.columns:
            return c

    lower_map = {col.lower(): col for col in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]

    raise KeyError(f"None of columns {candidates!r} found in {list(df.columns)!r}")


@dg.asset(
    name="stg_operations_with_type",
    deps=["ML_model", "stg_operations"],
    required_resource_keys={"database"},
    group_name="ML_model",
)
def stg_operations_with_type(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    # 1) Charger le modèle entraîné depuis MLflow
    model_dir = _repo_root() / "compte_b" / "data" / "models"
    mlflow_db_path = model_dir / "mlflow.db"
    tracking_uri = f"sqlite:///{mlflow_db_path.as_posix()}"
    model_uri_file = model_dir / "latest_model_uri.txt"
    if not model_uri_file.exists():
        raise FileNotFoundError(f"Missing MLflow model URI file: {model_uri_file}")

    model_uri = model_uri_file.read_text(encoding="utf-8").strip()
    if not model_uri:
        raise RuntimeError(f"Empty MLflow model URI in: {model_uri_file}")

    mlflow.set_tracking_uri(tracking_uri)
    model = mlflow.sklearn.load_model(model_uri=model_uri)

    # 2) Charger les opérations (features brutes)
    with context.resources.database.get_connection() as con:
        ops = con.execute("SELECT * FROM stg_operations").df()

        if ops.empty:
            raise RuntimeError("No rows found in `stg_operations` for inference.")

        date_col = _pick_column(ops, ["date"])
        libelle_col = _pick_column(ops, ["libelle"])
        montant_col = _pick_column(ops, ["montant_euros"])

        # 3) Préparer les features attendues par le pipeline
        ops["Libelle_clean"] = ops[libelle_col].apply(clean_text)

        bins = [-10000, -1000, -500, -100, -50, -10, 0, 10, 50, 500, 1000, 10000]
        ops["montant_bin"] = (
            pd.cut(ops[montant_col].astype(float), bins=bins, labels=False)
            .fillna(-1)
            .astype(int)
        )

        X_pred_df = ops[["Libelle_clean", "montant_bin"]]
        y_pred = model.predict(X_pred_df)

        # 4) Construire le résultat à stocker
        out_df = pd.DataFrame(
            {
                "Type": y_pred.astype(str),
                "date": ops[date_col],
                "libelle": ops[libelle_col],
                "montant_euros": ops[montant_col].astype(float),
            }
        )

        # 5) Ecrire dans DuckDB (on overwrite pour éviter les doublons)
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS "stg_operations_with_type" (
                "Type" VARCHAR,
                "date" DATE,
                "libelle" VARCHAR,
                "montant_euros" DOUBLE
            )
            """
        )
        con.execute('DELETE FROM "stg_operations_with_type"')

        rows = [
            (row.Type, row.date, row.libelle, float(row.montant_euros))
            for row in out_df.itertuples(index=False)
        ]
        con.executemany(
            """
            INSERT INTO "stg_operations_with_type"
                ("Type", "date", "libelle", "montant_euros")
            VALUES (?, ?, ?, ?)
            """,
            rows,
        )

    return dg.MaterializeResult(
        metadata={
            "rows": dg.MetadataValue.int(len(out_df)),
            "model_uri": dg.MetadataValue.text(model_uri),
            "tracking_uri": dg.MetadataValue.text(tracking_uri),
            "output_table": dg.MetadataValue.text("stg_operations_with_type"),
        }
    )