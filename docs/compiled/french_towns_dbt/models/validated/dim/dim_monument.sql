

WITH source AS (
    SELECT
        reference::VARCHAR(10)                  AS reference,
        denomination_de_l_edifice::VARCHAR(255)  AS name,
        autre_appellation_de_l_edifice::VARCHAR(255)
                                                AS alternative_name,
        domaine::VARCHAR(100)                    AS domain,
        denomination_de_l_edifice::VARCHAR(255)  AS denomination,
        typologie_de_la_protection::VARCHAR(255) AS raw_protection,
        nature_de_la_protection::VARCHAR(50)     AS nature,
        statut_juridique_de_l_edifice::VARCHAR(255)
                                                AS legal_status,
        destination_actuelle_de_l_edifice::VARCHAR(255)
                                                AS current_use,
        siecle_de_la_campagne_principale_de_construction::VARCHAR(255)
                                                AS century,
        auteur_de_l_edifice::VARCHAR(255)        AS author,
        etat_de_conservation::VARCHAR(100)        AS conservation_state,
        cadastre::VARCHAR(255)                    AS cadastre,
        lieudit::VARCHAR(255)                     AS lieu_dit,
        vocable_pour_les_edifices_cultuels::VARCHAR(255)
                                                AS vocable,
        description_de_l_edifice::TEXT            AS description,
        historique::TEXT                          AS historical,
        lien_vers_la_base_archiv_mh::VARCHAR(255) AS source_url,
        lien_vers_la_base_palissy::VARCHAR(255)   AS palissy_url,
        date_et_typologie_de_la_protection::TEXT  AS protection_history,
        COALESCE(
            json_extract_string(cog_insee_lors_de_la_protection, '$[0]'),
            ''
        )::VARCHAR(5)                             AS raw_code,
        ST_X(geom)::DOUBLE                        AS longitude,
        ST_Y(geom)::DOUBLE                        AS latitude
    FROM ST_Read(
        
    
    
        's3://staging-current/cultural_heritage/monuments_historiques_*.geojson'
    

    )
),

with_clean_code AS (
    SELECT
        *,
        CASE
            WHEN raw_code LIKE '%:%' THEN split_part(raw_code, ' : ', 1)
            WHEN raw_code ~ '^751[0-9]{2}$' THEN '75056'
            WHEN raw_code ~ '^6938[0-9]$'  THEN '69123'
            WHEN raw_code ~ '^132[0-9]{2}$' THEN '13055'
            ELSE raw_code
        END AS mapped_code
    FROM source
),

with_code_commune AS (
    SELECT
        w.* EXCLUDE (raw_code, mapped_code),
        w.mapped_code AS primary_commune_code,
        c.id IS NOT NULL AS commune_found_by_code
    FROM with_clean_code w
    LEFT JOIN "french_towns"."main"."dim_communes" c
        ON w.mapped_code = c.id
),

with_spatial_fallback AS (
    SELECT
        w.* EXCLUDE (commune_found_by_code),
        CASE
            WHEN w.commune_found_by_code THEN w.primary_commune_code
            WHEN w.longitude IS NOT NULL THEN COALESCE(g.commune_id, w.primary_commune_code)
            ELSE w.primary_commune_code
        END AS resolved_code
    FROM with_code_commune w
    LEFT JOIN "french_towns"."main"."dim_geography" g
        ON NOT w.commune_found_by_code
        AND w.longitude IS NOT NULL
        AND w.longitude BETWEEN g.bbox_xmin AND g.bbox_xmax
        AND w.latitude BETWEEN g.bbox_ymin AND g.bbox_ymax
        AND ST_Contains(
            g.geometry,
            ST_Point(w.longitude, w.latitude)
        )
),

with_protection AS (
    SELECT
        * EXCLUDE (resolved_code, primary_commune_code),
        resolved_code::VARCHAR(5) AS primary_commune_code,
        CASE
            WHEN raw_protection LIKE '%classé%' THEN 'classé'
            WHEN raw_protection LIKE '%inscrit%' THEN 'inscrit'
            ELSE NULL
        END AS protection_level,
        CASE
            WHEN raw_protection LIKE '%partiellement%' THEN 'partiellement'
            ELSE NULL
        END AS protection_scope
    FROM with_spatial_fallback
),

with_dates AS (
    SELECT
        *,
        CASE
            WHEN protection_history IS NOT NULL
                 AND protection_history != ''
                THEN try_strptime(
                    split_part(
                        string_split(protection_history, ';')[1],
                        ' : ',
                        1
                    ),
                    '%Y/%m/%d'
                )
            ELSE NULL
        END AS first_protection_date,
        CASE
            WHEN protection_history IS NOT NULL
                 AND protection_history != ''
                THEN try_strptime(
                    split_part(
                        string_split(protection_history, ';')[-1],
                        ' : ',
                        1
                    ),
                    '%Y/%m/%d'
                )
            ELSE NULL
        END AS last_protection_date
    FROM with_protection
)

SELECT
    reference,
    name,
    alternative_name,
    domain,
    denomination,
    nature,
    protection_level,
    protection_scope,
    first_protection_date,
    last_protection_date,
    legal_status,
    current_use,
    century,
    author,
    conservation_state,
    cadastre,
    lieu_dit,
    vocable,
    description,
    historical,
    source_url,
    palissy_url,
    primary_commune_code,
    longitude,
    latitude
FROM with_dates