{{ config(
    materialized='external',
    location='s3://validated/' ~ this.name ~ '.parquet'
) }}

WITH metropole_drom AS (
    SELECT
        c.COM::CHAR(5)                  AS id,
        c.NCCENR::VARCHAR(255)          AS name,
        c.NCC::VARCHAR(255)             AS name_upper,
        NULLIF(c.ARR, '')::VARCHAR(4)   AS arrondissement_code,
        arr.LIBELLE::VARCHAR(255)       AS arrondissement_name,
        c.DEP::VARCHAR(3)               AS department_code,
        dep.LIBELLE::VARCHAR(255)       AS department_name,
        c.REG::VARCHAR(2)               AS region_code,
        reg.LIBELLE::VARCHAR(255)       AS region_name,
        'METROPOLE_DROM'               AS territory_type,
        CASE
            WHEN c.DEP IN ('2A', '2B') THEN 1
            ELSE 0
        END AS flag_corsica,
        CASE
            WHEN LENGTH(c.DEP) = 3 THEN 0
            ELSE 1
        END AS flag_metropole,
        CASE
            WHEN dep.CHEFLIEU = c.COM THEN 1
            ELSE 0
        END AS flag_prefecture,
        CASE
            WHEN arr.CHEFLIEU = c.COM THEN 1
            ELSE 0
        END AS flag_chef_lieu_arrondissement,
        CASE
            WHEN arr.CHEFLIEU = c.COM AND dep.CHEFLIEU != c.COM THEN 1
            ELSE 0
        END AS flag_sous_prefecture
    FROM {{ source('french_towns', 'cog_communes') }} AS c
    LEFT JOIN {{ source('french_towns', 'cog_departements') }} AS dep
        ON c.DEP = dep.DEP
    LEFT JOIN {{ source('french_towns', 'cog_arrondissements') }} AS arr
        ON c.ARR = arr.ARR
    LEFT JOIN {{ source('french_towns', 'cog_regions') }} AS reg
        ON c.REG = reg.REG
    WHERE c.TYPECOM = 'COM'
),
com AS (
    SELECT
        COM_COMER::CHAR(5)              AS id,
        NCCENR::VARCHAR(255)            AS name,
        NCC::VARCHAR(255)               AS name_upper,
        NULL::VARCHAR(4)                AS arrondissement_code,
        NULL::VARCHAR(255)              AS arrondissement_name,
        COMER::VARCHAR(3)               AS department_code,
        LIBELLE_COMER::VARCHAR(255)     AS department_name,
        COMER::VARCHAR(2)               AS region_code,
        LIBELLE_COMER::VARCHAR(255)     AS region_name,
        'COM'                           AS territory_type,
        0                               AS flag_corsica,
        0                               AS flag_metropole,
        0                               AS flag_prefecture,
        0                               AS flag_chef_lieu_arrondissement,
        0                               AS flag_sous_prefecture
    FROM {{ source('french_towns', 'cog_communes_comer') }}
)
SELECT * FROM metropole_drom
UNION ALL
SELECT * FROM com
