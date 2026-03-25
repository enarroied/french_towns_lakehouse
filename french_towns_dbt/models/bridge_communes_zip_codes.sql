{{ config(
    materialized='external',
    location='../data/processed/' ~ this.name ~ '.parquet'
) }}

SELECT DISTINCT
    "#Code_commune_INSEE"::CHAR(5) AS commune_id,
    Code_postal::INTEGER AS zip_code_id,
    Code_postal::CHAR(5) AS zip_code_char
FROM {{ source('french_towns', 'zip_codes') }}
WHERE commune_id IN ( /* Remove communes not present in master dimension*/
    SELECT id FROM {{ ref('dim_communes_france') }}
)
