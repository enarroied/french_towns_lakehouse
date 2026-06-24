{{ config(
    materialized='external',
    location='s3://validated/' ~ this.name ~ '.parquet'
) }}

WITH source AS (
    SELECT
        reference::VARCHAR(10) AS monument_reference,
        cog_insee_lors_de_la_protection AS raw_insee_codes
    FROM ST_Read(
        {{ latest_file(var("input_dir") ~ "/cultural_heritage/monuments_historiques_*.geojson") }}
    )
),

exploded AS (
    SELECT
        monument_reference,
        UNNEST(raw_insee_codes) AS raw_code
    FROM source
),

cleaned AS (
    SELECT
        monument_reference,
        CASE
            WHEN raw_code LIKE '%:%'
                THEN trim(split_part(raw_code, ':', 1))
            ELSE trim(raw_code, ' "')
        END AS commune_code
    FROM exploded
    WHERE trim(raw_code, ' "') != ''
),

mapped AS (
    SELECT
        monument_reference,
        CASE
            WHEN commune_code ~ '^751[0-9]{2}$' THEN '75056'
            WHEN commune_code ~ '^6938[0-9]$'  THEN '69123'
            WHEN commune_code ~ '^132[0-9]{2}$' THEN '13055'
            ELSE commune_code
        END AS commune_code
    FROM cleaned
    WHERE commune_code ~ '^[0-9A-B]{5}$'
),

ranked AS (
    SELECT
        monument_reference,
        commune_code,
        ROW_NUMBER() OVER (
            PARTITION BY monument_reference ORDER BY commune_code
        ) = 1 AS is_primary
    FROM mapped
)

SELECT
    r.monument_reference,
    r.commune_code,
    r.is_primary,
    c.id IS NOT NULL AS commune_exists
FROM ranked r
LEFT JOIN {{ ref('dim_communes') }} c
    ON r.commune_code = c.id
