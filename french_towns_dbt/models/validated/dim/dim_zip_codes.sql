{{ config(
    materialized='external',
    location='../data/processed/' ~ this.name ~ '.parquet'
) }}

SELECT
    Code_postal::INTEGER AS id,
    Code_postal::CHAR(5) AS zip_code_char,
    SUBSTR(Code_postal, 1, 2) AS zip_code_main_department,
    COUNT(DISTINCT "#Code_commune_INSEE") AS number_of_communes,
    COUNT(DISTINCT SUBSTR("#Code_commune_INSEE", 1, 2)) AS number_of_departments
FROM {{ source('french_towns', 'zip_codes') }}
GROUP BY Code_postal
