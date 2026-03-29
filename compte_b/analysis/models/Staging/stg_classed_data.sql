{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='sync_all_columns',
    )
}}

{% if is_incremental() %}

with partage_sqlite_base as (
    SELECT
        "Type",
        COALESCE(
            TRY_CAST("date" AS DATE),
            CAST(TRY_STRPTIME(TRIM(CAST("date" AS VARCHAR)), '%d/%m/%Y') AS DATE),
            CAST(TRY_STRPTIME(TRIM(CAST("date" AS VARCHAR)), '%Y-%m-%d') AS DATE)
        ) AS "Date",
        "libelle" AS "Libellé",
        CAST("montant_euros" AS DOUBLE) AS "Montant"
    FROM {{ source('sqlite', 'partage_sqlite_table') }}
),
max_existing as (
    SELECT MAX("Date") AS max_date
    FROM {{ this }}
),
partage_sqlite_filtered as (
    SELECT *
    FROM partage_sqlite_base
    WHERE
        (
            (SELECT max_date FROM max_existing) IS NULL
            OR "Date" > (SELECT max_date FROM max_existing)
        )
        AND "Date" < (current_date - interval '3 days')
)
SELECT
    "Type",
    "Date",
    "Libellé",
    "Montant"
FROM partage_sqlite_filtered

{% else %}

with raw_classed_data as (
    SELECT
        *
    FROM {{ source('compte_b', 'classed_data') }}
),
classed_data_base as (
    SELECT
        "Type",
        COALESCE(
            TRY_CAST("Date" AS DATE),
            CAST(TRY_STRPTIME(TRIM(CAST("Date" AS VARCHAR)), '%d/%m/%Y') AS DATE),
            CAST(TRY_STRPTIME(TRIM(CAST("Date" AS VARCHAR)), '%Y-%m-%d') AS DATE)
        ) AS "Date",
        "Libellé",
        "Montants (EUROS)" AS "Montant"
    FROM raw_classed_data
    WHERE "Type" NOT IN ('Bilan Mensuel')
)
SELECT *
FROM classed_data_base

{% endif %}
