with raw_classed_data as (
    SELECT 
        * 
    FROM {{ source('compte_b', 'classed_data') }}
)
SELECT * FROM raw_classed_data