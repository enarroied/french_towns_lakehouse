{{ config(
    materialized='external',
    location='s3://validated/' ~ this.name ~ '.parquet'
) }}

WITH income_base AS (
    SELECT
        id::CHAR(5)                     AS id,
        year::INTEGER                   AS year,
        methodology_version::VARCHAR    AS methodology_version,
        nb_tax_households::INTEGER      AS nb_tax_households,
        nb_persons::INTEGER             AS nb_persons,
        median_income::FLOAT            AS median_income,
        poverty_rate::FLOAT             AS poverty_rate,
        decile1::FLOAT                  AS decile1,
        decile9::FLOAT                  AS decile9,
        gini::FLOAT                     AS gini,
        s80_s20::FLOAT                  AS s80_s20,
        activity_income_share::FLOAT    AS activity_income_share,
        salary_share::FLOAT             AS salary_share,
        unemployment_share::FLOAT       AS unemployment_share,
        pension_share::FLOAT            AS pension_share,
        property_income_share::FLOAT    AS property_income_share,
        social_benefits_share::FLOAT    AS social_benefits_share,
        family_benefits_share::FLOAT    AS family_benefits_share,
        minimum_social_share::FLOAT     AS minimum_social_share,
        housing_benefits_share::FLOAT   AS housing_benefits_share,
        tax_share::FLOAT                AS tax_share
    FROM {{ source('french_towns', 'filosofi_income') }}
),

filtered AS (
    SELECT
        *,
        CASE
            WHEN id ~ '^751[0-9]{2}$' THEN '75056'
            WHEN id ~ '^6938[0-9]$'  THEN '69123'
            WHEN id ~ '^132[0-9]{2}$' THEN '13055'
            ELSE id
        END AS id_mapped
    FROM income_base
)

SELECT
    id_mapped                                   AS id,
    year,
    methodology_version,
    nb_tax_households,
    nb_persons,
    median_income,
    poverty_rate,
    decile1,
    decile9,
    gini,
    s80_s20,
    activity_income_share,
    salary_share,
    unemployment_share,
    pension_share,
    property_income_share,
    social_benefits_share,
    family_benefits_share,
    minimum_social_share,
    housing_benefits_share,
    tax_share,
    year * 10000 + 101                          AS date_id
FROM filtered
WHERE id = id_mapped
ORDER BY id, year
