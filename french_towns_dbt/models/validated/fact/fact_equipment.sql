{{ config(
    materialized='external',
    location='s3://validated/' ~ this.name ~ '.parquet'
) }}

WITH equipment_base AS (
    SELECT
        GEO::CHAR(5)            AS id,
        TIME_PERIOD::INTEGER    AS year,
        FACILITY_TYPE           AS equipment_code,
        SUM(OBS_VALUE::INTEGER) AS count
    FROM {{ source('french_towns', 'bpe') }}
    WHERE GEO_OBJECT = 'COM'
      AND FACILITY_TYPE != '_T'
    GROUP BY GEO, TIME_PERIOD, FACILITY_TYPE
)
SELECT
    c.id                                    AS commune_id,
    e.year,
    e.year * 10000 + 101                    AS date_id,
    eq.equipment_type_id,
    e.count
FROM equipment_base e
LEFT JOIN {{ ref('dim_communes_france') }} c
    ON e.id = c.id
LEFT JOIN {{ ref('dim_equipment') }} eq
    ON e.equipment_code = eq.equipment_code
ORDER BY commune_id, year, equipment_type_id
