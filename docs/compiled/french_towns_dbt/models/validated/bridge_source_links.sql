



SELECT
    ROW_NUMBER() OVER () AS source_link_id,
    'fact_population' AS target_table,
    id || '|' || year AS target_key,
    s.source_id
FROM "french_towns"."main"."fact_population" t
CROSS JOIN (
    SELECT source_id FROM "french_towns"."main"."dim_source"
    WHERE source_name IN ('historical_population', 'family', 'migration', 'births', 'deaths')
) s


UNION ALL


SELECT
    ROW_NUMBER() OVER () AS source_link_id,
    'fact_salaries' AS target_table,
    id || '|' || year AS target_key,
    s.source_id
FROM "french_towns"."main"."fact_salaries" t
CROSS JOIN (
    SELECT source_id FROM "french_towns"."main"."dim_source"
    WHERE source_name IN ('salaries')
) s


UNION ALL


SELECT
    ROW_NUMBER() OVER () AS source_link_id,
    'fact_equipment' AS target_table,
    commune_id || '|' || year || '|' || equipment_type_id AS target_key,
    s.source_id
FROM "french_towns"."main"."fact_equipment" t
CROSS JOIN (
    SELECT source_id FROM "french_towns"."main"."dim_source"
    WHERE source_name IN ('bpe')
) s


UNION ALL


SELECT
    ROW_NUMBER() OVER () AS source_link_id,
    'dim_communes' AS target_table,
    id AS target_key,
    s.source_id
FROM "french_towns"."main"."dim_communes" t
CROSS JOIN (
    SELECT source_id FROM "french_towns"."main"."dim_source"
    WHERE source_name IN ('cog_ensemble')
) s


UNION ALL


SELECT
    ROW_NUMBER() OVER () AS source_link_id,
    'dim_geography' AS target_table,
    commune_id AS target_key,
    s.source_id
FROM "french_towns"."main"."dim_geography" t
CROSS JOIN (
    SELECT source_id FROM "french_towns"."main"."dim_source"
    WHERE source_name IN ('french_communes')
) s


UNION ALL


SELECT
    ROW_NUMBER() OVER () AS source_link_id,
    'dim_zip_codes' AS target_table,
    id::VARCHAR AS target_key,
    s.source_id
FROM "french_towns"."main"."dim_zip_codes" t
CROSS JOIN (
    SELECT source_id FROM "french_towns"."main"."dim_source"
    WHERE source_name IN ('zip_codes')
) s


UNION ALL


SELECT
    ROW_NUMBER() OVER () AS source_link_id,
    'dim_equipment' AS target_table,
    equipment_type_id::VARCHAR AS target_key,
    s.source_id
FROM "french_towns"."main"."dim_equipment" t
CROSS JOIN (
    SELECT source_id FROM "french_towns"."main"."dim_source"
    WHERE source_name IN ('dim_equipment')
) s


UNION ALL



SELECT
    ROW_NUMBER() OVER () AS source_link_id,
    'dim_labels' AS target_table,
    id || '|' || COALESCE(label_type, '') AS target_key,
    m.source_id
FROM "french_towns"."main"."dim_labels" t
JOIN (
    
    SELECT 'petites_cites' AS match_value, source_id FROM "french_towns"."main"."dim_source" WHERE source_name = 'petites_cites'
    UNION ALL
    
    SELECT 'villes_fleuries' AS match_value, source_id FROM "french_towns"."main"."dim_source" WHERE source_name = 'villes_fleuries'
    UNION ALL
    
    SELECT 'plus_beaux_villages' AS match_value, source_id FROM "french_towns"."main"."dim_source" WHERE source_name = 'plus_beaux_villages'
    UNION ALL
    
    SELECT 'villes_prudentes' AS match_value, source_id FROM "french_towns"."main"."dim_source" WHERE source_name = 'villes_prudentes'
    UNION ALL
    
    SELECT 'village_etape' AS match_value, source_id FROM "french_towns"."main"."dim_source" WHERE source_name = 'village_etape'
    UNION ALL
    
    SELECT 'famille_plus' AS match_value, source_id FROM "french_towns"."main"."dim_source" WHERE source_name = 'famille_plus'
    UNION ALL
    
    SELECT 'ville_sportive' AS match_value, source_id FROM "french_towns"."main"."dim_source" WHERE source_name = 'ville_sportive'
    
    
) m ON t.label_type = m.match_value


UNION ALL


SELECT
    ROW_NUMBER() OVER () AS source_link_id,
    'bridge_communes_zip_codes' AS target_table,
    commune_id || '|' || zip_code_id::VARCHAR AS target_key,
    s.source_id
FROM "french_towns"."main"."bridge_communes_zip_codes" t
CROSS JOIN (
    SELECT source_id FROM "french_towns"."main"."dim_source"
    WHERE source_name IN ('zip_codes')
) s


UNION ALL


SELECT
    ROW_NUMBER() OVER () AS source_link_id,
    'dim_neighbour_communes' AS target_table,
    parcel_id || '|' || neighbor_id AS target_key,
    s.source_id
FROM "french_towns"."main"."dim_neighbour_communes" t
CROSS JOIN (
    SELECT source_id FROM "french_towns"."main"."dim_source"
    WHERE source_name IN ('french_communes')
) s


UNION ALL


SELECT
    ROW_NUMBER() OVER () AS source_link_id,
    'dim_criminality_indicateur' AS target_table,
    indicateur_id::VARCHAR AS target_key,
    s.source_id
FROM "french_towns"."main"."dim_criminality_indicateur" t
CROSS JOIN (
    SELECT source_id FROM "french_towns"."main"."dim_source"
    WHERE source_name IN ('criminality')
) s


UNION ALL


SELECT
    ROW_NUMBER() OVER () AS source_link_id,
    'fact_criminality' AS target_table,
    commune_id || '|' || annee || '|' || indicateur_id AS target_key,
    s.source_id
FROM "french_towns"."main"."fact_criminality" t
CROSS JOIN (
    SELECT source_id FROM "french_towns"."main"."dim_source"
    WHERE source_name IN ('criminality')
) s

