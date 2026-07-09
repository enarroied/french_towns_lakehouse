{{ config(
    materialized='external',
    location='s3://validated/' ~ this.name ~ '.parquet'
) }}

SELECT
    indicateur_id::INTEGER          AS indicateur_id,
    name_fr::VARCHAR(100)           AS name_fr,
    name_en::VARCHAR(100)           AS name_en,
    unite_de_compte_fr::VARCHAR(50) AS unite_de_compte_fr,
    unite_de_compte_en::VARCHAR(50) AS unite_de_compte_en
FROM {{ source('french_towns', 'dim_criminality_indicateur') }}
ORDER BY indicateur_id
