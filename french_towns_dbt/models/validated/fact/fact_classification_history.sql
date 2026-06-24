{{ config(
    materialized='external',
    location='s3://validated/' ~ this.name ~ '.parquet'
) }}

WITH source AS (
    SELECT
        reference::VARCHAR(10) AS monument_reference,
        date_et_typologie_de_la_protection::TEXT AS raw_history
    FROM ST_Read(
        {{ latest_file(var("input_dir") ~ "/cultural_heritage/monuments_historiques_*.geojson") }}
    )
    WHERE date_et_typologie_de_la_protection IS NOT NULL
      AND date_et_typologie_de_la_protection != ''
),

exploded AS (
    SELECT
        monument_reference,
        trim(e) AS event
    FROM source,
    LATERAL UNNEST(
        string_split(raw_history, ';')
    ) AS t(e)
    WHERE trim(e) != ''
),

parsed AS (
    SELECT
        monument_reference,
        try_strptime(
            split_part(event, ' : ', 1),
            '%Y/%m/%d'
        )::DATE AS event_date,
        CASE
            WHEN lower(split_part(event, ' : ', 2)) LIKE '%classé%'
                THEN 'classé'
            WHEN lower(split_part(event, ' : ', 2)) LIKE '%inscrit%'
                THEN 'inscrit'
            ELSE lower(trim(split_part(event, ' : ', 2)))
        END AS new_protection_level,
        CASE
            WHEN lower(split_part(event, ' : ', 2)) LIKE '%partiellement%'
                THEN 'partiellement'
            ELSE NULL
        END AS new_protection_scope
    FROM exploded
    WHERE split_part(event, ' : ', 1) ~ '^\d{4}/\d{2}/\d{2}$'
),

with_previous AS (
    SELECT
        monument_reference,
        event_date,
        new_protection_level,
        new_protection_scope,
        LAG(new_protection_level) OVER (
            PARTITION BY monument_reference ORDER BY event_date
        ) AS previous_protection_level,
        LAG(new_protection_scope) OVER (
            PARTITION BY monument_reference ORDER BY event_date
        ) AS previous_protection_scope
    FROM parsed
    WHERE event_date IS NOT NULL
),

with_event_type AS (
    SELECT
        *,
        CASE
            WHEN previous_protection_level IS NULL THEN 'initial'
            WHEN previous_protection_level = 'inscrit'
                 AND new_protection_level = 'classé'
                THEN 'upgrade'
            WHEN previous_protection_level = 'classé'
                 AND new_protection_level = 'inscrit'
                THEN 'downgrade'
            WHEN previous_protection_level = new_protection_level
                THEN 'update'
            ELSE 'change'
        END AS event_type
    FROM with_previous
)

SELECT
    monument_reference,
    event_date,
    new_protection_level,
    new_protection_scope,
    previous_protection_level,
    previous_protection_scope,
    event_type
FROM with_event_type
ORDER BY monument_reference, event_date
