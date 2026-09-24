

WITH raw AS (
    SELECT
        CODGEO_2025::VARCHAR(5)   AS commune_id,
        annee::INTEGER            AS annee,
        indicateur::VARCHAR(100)  AS indicateur,
        nombre::INTEGER           AS nombre,
        taux_pour_mille::DOUBLE   AS taux_pour_mille,
        est_diffuse::VARCHAR(5)   AS est_diffuse,
        complement_info_nombre::DOUBLE AS complement_info_nombre,
        complement_info_taux::DOUBLE   AS complement_info_taux,
        insee_pop::INTEGER        AS insee_pop,
        insee_log::INTEGER        AS insee_log
    FROM read_parquet('s3://staging-current/criminality/delinquance_*.parquet')
),

mapped AS (
    SELECT
        *,
        CASE
            WHEN commune_id ~ '^751[0-9]{2}$' THEN '75056'
            WHEN commune_id ~ '^6938[0-9]$'  THEN '69123'
            WHEN commune_id ~ '^132[0-9]{2}$' THEN '13055'
            ELSE commune_id
        END AS commune_id_mapped
    FROM raw
),

aggregated AS (
    SELECT
        commune_id_mapped                            AS commune_id,
        annee,
        indicateur,
        SUM(nombre)                                  AS nombre,
        ROUND(SUM(nombre) * 1000.0 / NULLIF(SUM(insee_pop), 0), 2)
                                                     AS taux_pour_mille,
        CASE
            WHEN MAX(CASE WHEN est_diffuse = 'diff' THEN 1 ELSE 0 END) = 1
            THEN 'diff'
            ELSE 'ndiff'
        END                                          AS est_diffuse,
        SUM(complement_info_nombre)                  AS complement_info_nombre,
        ROUND(SUM(complement_info_nombre) * 1000.0 / NULLIF(SUM(insee_pop), 0), 2)
                                                     AS complement_info_taux,
        SUM(insee_pop)                               AS insee_pop,
        SUM(insee_log)                               AS insee_log
    FROM mapped
    GROUP BY commune_id_mapped, annee, indicateur
)

SELECT
    a.commune_id,
    a.annee,
    i.indicateur_id,
    a.nombre,
    a.taux_pour_mille,
    a.est_diffuse,
    a.complement_info_nombre,
    a.complement_info_taux,
    a.insee_pop,
    a.insee_log
FROM aggregated a
JOIN "french_towns"."main"."dim_criminality_indicateur" i
    ON a.indicateur = i.name_fr
ORDER BY a.commune_id, a.annee, i.indicateur_id