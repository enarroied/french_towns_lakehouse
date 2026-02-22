{{ config(
    materialized='external',
    location='../data/processed/' ~ this.name ~ '.parquet'
) }}

SELECT
    GEO::CHAR(5)                                          AS id,
    TIME_PERIOD::INTEGER                                  AS year,
    MAX(CASE WHEN SEX = 'M'  THEN OBS_VALUE::INTEGER END) AS mean_salary_men,
    MAX(CASE WHEN SEX = 'F'  THEN OBS_VALUE::INTEGER END) AS mean_salary_women,
    MAX(CASE WHEN SEX = '_T' THEN OBS_VALUE::INTEGER END) AS mean_salary
FROM {{ source('french_towns', 'salaries') }}
WHERE
    GEO_OBJECT = 'COM'
    AND PCS_ESE = '_T'
GROUP BY GEO, TIME_PERIOD
ORDER BY GEO, year
