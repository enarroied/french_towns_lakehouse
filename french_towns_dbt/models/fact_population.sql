{{ config(
    materialized='external',
    location='../data/processed/' ~ this.name ~ '.parquet'
) }}

SELECT
    GEO::CHAR(5)            AS id,
    TIME_PERIOD::INTEGER    AS year,
    OBS_VALUE::INTEGER      AS population
FROM {{ source('french_towns', 'populations_historiques') }}
WHERE GEO_OBJECT = 'COM'
