import dagster as dg

# Job "training" : matérialise les assets Python+dbt nécessaires à l'entraînement ML.
Model_training_with_new_data = dg.define_asset_job(
    name="Model_training_with_new_data",
    selection=dg.AssetSelection.assets("stg_classed_data", "ML_model"),
)

Model_training_with_all_data = dg.define_asset_job(
    name="Model_training_with_all_data",
    selection=dg.AssetSelection.assets("classed_data", "stg_classed_data", "ML_model"),
)

# Job "inference" : lance uniquement la chaîne d'inférence.
# Important: on ne sélectionne pas `ML_model` pour éviter de ré-entraîner.
Inference = dg.define_asset_job(
    name="Inference",
    selection=dg.AssetSelection.assets(
        "raw_csv_import",
        "stg_operations",
        "stg_operations_with_type",
        "extract_inferenced_data",
        "partage_sqlite"
    ),
)


@dg.definitions
def job_definitions() -> dg.Definitions:
    # Explicitement exposer le job pour s'assurer qu'il est inclus dans `load_from_defs_folder`.
    return dg.Definitions(jobs=[Model_training_with_new_data, Model_training_with_all_data, Inference])

