with raw_classed_data as (
    SELECT 
        * 
    FROM {{ source('compte_b', 'classed_data') }}
)
SELECT  
    "Type",
    TRIM(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE("Libellé", ' CB ', ' '),
                        '\s[0-9]{2}H[0-9]{2}',
                        ' '
                    ),
                    '\s[0-9]{2}\.[0-9]{2}\.[0-9]{2}.',
                    ' '
                ),
                '\s[0-9]{2}/[0-9]{2}/[0-9]{2}.',
                ' '
            ),
            '[0-9]+',
            '',
            'g'
        )
    ) AS libelle_clean,
    "Montants (EUROS)" AS "Montant"
FROM raw_classed_data
WHERE "Type" NOT IN ('Bilan Mensuel')
