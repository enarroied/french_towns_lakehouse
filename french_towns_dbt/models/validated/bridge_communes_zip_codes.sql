{{ config(
    materialized='external',
    location='s3://validated/' ~ this.name ~ '.parquet'
) }}

SELECT DISTINCT
    CASE
        -- Paris, Marseille and Lyon have several codes in post office records
        WHEN
            Code_postal::INTEGER BETWEEN 75000 AND 75999 THEN '75056'
        WHEN
            Code_postal::INTEGER BETWEEN 13000 AND 13020 THEN '13055'
        WHEN
            Code_postal::INTEGER BETWEEN 69001 AND 69010 THEN '69123'
        ELSE "#Code_commune_INSEE"::CHAR(5)
    END AS commune_id,
        Code_postal::INTEGER AS zip_code_id,
        Code_postal::CHAR(5) AS zip_code_char
FROM {{ source('french_towns', 'zip_codes') }}
WHERE commune_id IN ( /* Remove communes not present in master dimension*/
    SELECT id FROM {{ ref('dim_communes') }}
)
