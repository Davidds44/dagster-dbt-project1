with raw_classed_data as (
    SELECT 
        * 
    FROM {{ source('compte_b', 'classed_data') }}
)
,
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
),
max_classed_date as (
    SELECT MAX("Date") AS max_date
    FROM classed_data_base
),
partage_sqlite_base as (
    SELECT
        "Type",
        COALESCE(
            TRY_CAST("date" AS DATE),
            CAST(TRY_STRPTIME(TRIM(CAST("date" AS VARCHAR)), '%d/%m/%Y') AS DATE),
            CAST(TRY_STRPTIME(TRIM(CAST("date" AS VARCHAR)), '%Y-%m-%d') AS DATE)
        ) AS "Date",
        "libelle" AS "Libellé",
        CAST("montant_euros" AS DOUBLE) AS "Montant"
    FROM {{ source('sqlite', 'stg_operations_with_type') }}
),
partage_sqlite_filtered as (
    SELECT *
    FROM partage_sqlite_base
    WHERE
        (
            (SELECT max_date FROM max_classed_date) IS NULL
            OR "Date" > (SELECT max_date FROM max_classed_date)
        )
        AND "Date" < (current_date - interval '3 days')
),
unioned as (
    SELECT
        "Type",
        "Date",
        "Libellé",
        "Montant"
    FROM classed_data_base
    UNION ALL
    SELECT
        "Type",
        "Date",
        "Libellé",
        "Montant"
    FROM partage_sqlite_filtered
)
SELECT *
FROM unioned
