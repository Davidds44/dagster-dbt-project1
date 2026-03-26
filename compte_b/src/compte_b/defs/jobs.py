import dagster as dg

# Job "training" : matérialise les assets Python+dbt nécessaires à l'entraînement ML.
Model_training = dg.define_asset_job(
    name="Model_training",
    selection=dg.AssetSelection.assets("classed_data", "stg_classed_data", "ML_model").upstream(),
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
    ),
)


@dg.definitions
def job_definitions() -> dg.Definitions:
    # Explicitement exposer le job pour s'assurer qu'il est inclus dans `load_from_defs_folder`.
    return dg.Definitions(jobs=[Model_training, Inference])

