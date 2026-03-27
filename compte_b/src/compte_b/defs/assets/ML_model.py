import dagster as dg
import pandas as pd
import re
from pathlib import Path
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from sklearn.svm import LinearSVC
from sklearn.metrics import accuracy_score, classification_report
from nltk.corpus import stopwords
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

def clean_text(t: object) -> str:
    """
    Nettoyage minimal inspiré du notebook :
    - lower
    - remplacements regex alignés sur `stg_classed_data.sql` ("Libellé")
    - suppression des chiffres
    - suppression de "carte numero"
    - normalisation whitespace
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


def _get_my_stop_words(context: dg.AssetExecutionContext) -> list[str]:
    """
    Stopwords inspirées du notebook (french + portuguese + tokens métier).

    Note: si le corpus NLTK n'est pas présent, on tente un téléchargement,
    sinon on retombe sur les tokens métier seulement.
    """

    extra_stop_words = [
        "CB",
        "REF",
        "ref",
        "cb",
        "SAS",
        "sas",
        "CARTE",
        "NUMERO",
        "EUR",
        "REFERENCE",
        "NO",
        ",",
    ]
    # Ton `clean_text` met tout en minuscules, donc on normalise aussi ici.
    extra_stop_words = [w.lower() if isinstance(w, str) else w for w in extra_stop_words]

    try:
        my_stop_words = stopwords.words("french") + stopwords.words("portuguese") + extra_stop_words
    except LookupError:
        # Dans certains environnements, le corpus NLTK "stopwords" peut ne pas être installé.
        try:
            import nltk

            nltk.download("stopwords", quiet=True)
            my_stop_words = (
                stopwords.words("french") + stopwords.words("portuguese") + extra_stop_words
            )
        except Exception as exc:  # pragma: no cover
            context.log.warning(f"NLTK stopwords unavailable; falling back to custom list: {exc}")
            my_stop_words = extra_stop_words

    # Dedup tout en gardant l'ordre
    return list(dict.fromkeys(str(w).lower() for w in my_stop_words))


def regex_features(X: object) -> pd.DataFrame:
    """
    Features "regex métier" reproduites du notebook.

    ColumnTransformer peut passer une colonne en DataFrame (n,1) ou un ndarray.
    """

    if isinstance(X, pd.DataFrame):
        X = X.iloc[:, 0]
    elif isinstance(X, (list, tuple, np.ndarray)):
        X = pd.Series(np.asarray(X).ravel())

    # Sécuriser le .str
    X = pd.Series(X).fillna("").astype(str)

    return pd.DataFrame(
        {
            "sncf": X.str.contains("sncf", regex=False).astype(int),
            "amazon": X.str.contains("amazon", regex=False).astype(int),
            "uber": X.str.contains("uber", regex=False).astype(int),
            "carrefour": X.str.contains("carrefour", regex=False).astype(int),
            "virement": X.str.contains("virement", regex=False).astype(int),
            "edf": X.str.contains("edf", regex=False).astype(int),
            "retrait": X.str.contains("retrait", regex=False).astype(int),
            "dgfip": X.str.contains("dgfip", regex=False).astype(int),
        }
    )


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



@dg.asset(
    name="ML_model",
    deps=["stg_classed_data"],
    required_resource_keys={"database"},
    group_name="ML_model",
)
def ML_model(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    # 1) Charger les données depuis DuckDB (modèle dbt `stg_classed_data`)
    query = """
        SELECT
            type as Classification,
            "Libellé" as Libelle,
            CAST(Montant as FLOAT) as Montant
        FROM stg_classed_data
    """

    with context.resources.database.get_connection() as con:
        data = con.execute(query).df()

    if data.empty:
        raise RuntimeError("No training data found in `stg_classed_data`.")

    # 2) Préparation features : clean_text + montant_bin
    data["Libelle_clean"] = data["Libelle"].apply(clean_text)

    data["montant_bin"] = pd.cut(
        data["Montant"],
        bins=[-10000, -1000, -500, -100, -50, -10, 0, 10, 50, 500, 1000, 10000],
        labels=False,
    )
    # Robustesse: `pd.cut(..., labels=False)` peut produire des NaN si valeurs hors bornes.
    data["montant_bin"] = data["montant_bin"].fillna(-1).astype(int)

    # 3) Split entraînement / test (hold-out)
    X_train, X_test, y_train, y_test = train_test_split(
        data[["Libelle_clean", "Montant"]],
        data["Classification"],
        test_size=0.2,
        random_state=42,
    )

    # 4) Reconstituer les DataFrames attendus par le ColumnTransformer
    X_train_df = data.loc[X_train.index, ["Libelle_clean", "montant_bin"]]
    X_test_df = data.loc[X_test.index, ["Libelle_clean", "montant_bin"]]

    # 5) Pipeline features (Tfidf + regex + montant_bin)
    my_stop_words = _get_my_stop_words(context)
    regex_transformer = FunctionTransformer(regex_features, validate=False)

    preprocess = ColumnTransformer(
        [
            (
                "text",
                TfidfVectorizer(
                    ngram_range=(1, 3),
                    min_df=2,
                    max_df=0.9,
                    stop_words=my_stop_words,
                ),
                "Libelle_clean",
            ),
            (
                "regex",
                regex_transformer,
                "Libelle_clean",
            ),
            (
                "amount",
                StandardScaler(),
                ["montant_bin"],
            ),
        ]
    )

    pipeline = Pipeline(
        [
            ("features", preprocess),
            ("clf", LinearSVC(C=1)),
        ]
    )

    # --- IMPORTANT: param names pour ce pipeline ---
    param_grid = {
        "features__text__ngram_range": [(1, 1), (1, 2), (1, 3)],
        "features__text__min_df": [1, 2, 5],
        "features__text__max_df": [0.7, 0.8, 0.9],
        "clf__C": [0.1, 1, 10],
    }

    # 6) Hyperparamètres via cross-validation (RandomizedSearchCV)
    search = RandomizedSearchCV(
        pipeline,
        param_distributions=param_grid,
        n_iter=30,
        cv=5,
        scoring="f1_weighted",
        n_jobs=-1,
        verbose=1,
    )

    search.fit(X_train_df, y_train)
    best_model = search.best_estimator_

    # 7) Évaluation sur le hold-out test
    y_pred = best_model.predict(X_test_df)
    accuracy = float(accuracy_score(y_test, y_pred))
    report = classification_report(y_test, y_pred)

    # 8) Sauvegarde du modèle entraîné via MLflow
    model_dir = _repo_root() / "compte_b" / "data" / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    mlflow_db_path = model_dir / "mlflow.db"
    artifacts_dir = model_dir / "mlartifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    tracking_uri = f"sqlite:///{mlflow_db_path.as_posix()}"
    mlflow.set_tracking_uri(tracking_uri)
    artifact_uri = artifacts_dir.as_uri()
    client = MlflowClient(tracking_uri=tracking_uri)
    experiment_name = "ML_model"
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        client.create_experiment(experiment_name, artifact_location=artifact_uri)
    elif experiment.artifact_location != artifact_uri:
        # Existing experiment may still point to legacy "mlruns" artifacts.
        experiment_name = "ML_model_mlartifacts"
        alt_experiment = client.get_experiment_by_name(experiment_name)
        if alt_experiment is None:
            client.create_experiment(experiment_name, artifact_location=artifact_uri)
    mlflow.set_experiment(experiment_name)
    model_uri_file = model_dir / "latest_model_uri.txt"
    model_uri = ""

    try:
        with mlflow.start_run(run_name="ML_model") as run:
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_params(search.best_params_)
            mlflow.log_text(report, "classification_report.txt")
            mlflow.sklearn.log_model(sk_model=best_model, artifact_path="model")
            model_uri = f"runs:/{run.info.run_id}/model"
            model_uri_file.write_text(model_uri, encoding="utf-8")
    except Exception as exc:  # pragma: no cover
        context.log.warning(f"MLflow logging skipped: {exc}")

    return dg.MaterializeResult(
        metadata={
            "rows": dg.MetadataValue.int(len(data)),
            "accuracy": dg.MetadataValue.float(accuracy),
            "best_params": dg.MetadataValue.json(search.best_params_),
            "classification_report": dg.MetadataValue.text(report),
            "model_uri": dg.MetadataValue.text(model_uri),
            "tracking_uri": dg.MetadataValue.text(tracking_uri),
            "artifacts_uri": dg.MetadataValue.path(str(artifacts_dir)),
            "experiment_name": dg.MetadataValue.text(experiment_name),
            "model_uri_file": dg.MetadataValue.path(str(model_uri_file)),
        }
    )