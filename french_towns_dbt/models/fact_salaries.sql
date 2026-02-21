{{ config(
    materialized='external',
    location=var('output_dir') ~ '/' ~ this.name ~ '.parquet'
) }}

SELECT
    GEO::CHAR(5)                                                       AS id,
    TIME_PERIOD::INTEGER                                               AS year,
    MAX(CASE WHEN SEX = 'M'  THEN OBS_VALUE::INTEGER END)              AS mean_salary_men,
    MAX(CASE WHEN SEX = 'F'  THEN OBS_VALUE::INTEGER END)              AS mean_salary_women,
    MAX(CASE WHEN SEX = '_T' THEN OBS_VALUE::INTEGER END)              AS mean_salary
FROM read_csv_auto(
    '{{ env_var("DBT_INPUT_DIR", "../input") }}/DS_BTS_SAL_EQTP_SEX_PCS_2023_data.csv',
    types={'GEO': 'CHAR(5)'}
)
WHERE GEO_OBJECT = 'COM'
GROUP BY GEO, TIME_PERIOD
ORDER BY GEO, year
