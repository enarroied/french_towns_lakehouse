{{ config(
    materialized='external',
    location='s3://validated/' ~ this.name ~ '.parquet'
) }}

SELECT
    source_id::INTEGER   AS source_id,
    source_name::VARCHAR(255)   AS source_name,
    source_label::VARCHAR(255)  AS source_label,
    organization::VARCHAR(255)  AS organization,
    domain::VARCHAR(100)        AS domain,
    reference_url::VARCHAR(1024) AS reference_url,
    license::VARCHAR(255)       AS license,
    description::VARCHAR(4096)  AS description
FROM {{ source('french_towns', 'sources') }}
