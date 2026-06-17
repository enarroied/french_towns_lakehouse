{{ config(
    materialized='external',
    location='s3://validated/' ~ this.name ~ '.parquet'
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY equipment_code) AS equipment_type_id,
    equipment_code,
    equipment_name,
    subdomain_code,
    subdomain_name,
    domain_code,
    domain_name
FROM {{ source('french_towns', 'dim_equipment') }}
ORDER BY equipment_type_id
