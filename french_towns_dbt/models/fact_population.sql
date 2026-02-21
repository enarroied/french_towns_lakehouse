{{ config(
    materialized='external',
    location=var('output_dir') ~ '/' ~ this.name ~ '.parquet'
) }}

SELECT
    GEO::CHAR(5)            AS id,
    TIME_PERIOD::INTEGER    AS year,
    OBS_VALUE::INTEGER      AS population
FROM read_csv_auto(
    '{{ env_var("DBT_INPUT_DIR", "../input") }}/DS_POPULATIONS_HISTORIQUES_data.csv',
    types={'GEO': 'CHAR(5)'}
)
WHERE GEO_OBJECT = 'COM'
