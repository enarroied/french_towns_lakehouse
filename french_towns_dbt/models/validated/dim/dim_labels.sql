{{ config(
    materialized='external',
    location='s3://validated/' ~ this.name ~ '.parquet'
) }}

SELECT
    'placeholder' AS id,
    'placeholder' AS label_name,
    'placeholder' AS label_type
WHERE false
