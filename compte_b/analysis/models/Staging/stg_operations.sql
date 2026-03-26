with raw_data as (
    SELECT 
        * 
    FROM {{ source('compte_b', 'raw_csv_import') }}
)
SELECT * FROM raw_data
WHERE "Date" >= today() - interval '2 months'
