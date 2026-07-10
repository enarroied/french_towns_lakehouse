{{ config(
    materialized='external',
    location='s3://validated/' ~ this.name ~ '.parquet'
) }}

{{ bridge_source_links_simple('fact_population', "id || '|' || year", ['historical_population', 'family', 'migration', 'births', 'deaths']) }}
UNION ALL
{{ bridge_source_links_simple('fact_salaries', "id || '|' || year", ['salaries']) }}
UNION ALL
{{ bridge_source_links_simple('fact_equipment', "commune_id || '|' || year || '|' || equipment_type_id", ['bpe']) }}
UNION ALL
{{ bridge_source_links_simple('dim_communes', "id", ['cog_ensemble']) }}
UNION ALL
{{ bridge_source_links_simple('dim_geography', "commune_id", ['french_communes']) }}
UNION ALL
{{ bridge_source_links_simple('dim_zip_codes', "id::VARCHAR", ['zip_codes']) }}
UNION ALL
{{ bridge_source_links_simple('dim_equipment', "equipment_type_id::VARCHAR", ['dim_equipment']) }}
UNION ALL
{{ bridge_source_links_mapped('dim_labels', "id || '|' || COALESCE(label_type, '')", 'label_type', {
    'petites_cites': 'petites_cites',
    'villes_fleuries': 'villes_fleuries',
    'plus_beaux_villages': 'plus_beaux_villages',
    'villes_prudentes': 'villes_prudentes',
    'village_etape': 'village_etape',
    'famille_plus': 'famille_plus',
    'ville_sportive': 'ville_sportive',
}) }}
UNION ALL
{{ bridge_source_links_simple('bridge_communes_zip_codes', "commune_id || '|' || zip_code_id::VARCHAR", ['zip_codes']) }}
UNION ALL
{{ bridge_source_links_simple('dim_neighbour_communes', "parcel_id || '|' || neighbor_id", ['french_communes']) }}
UNION ALL
{{ bridge_source_links_simple('dim_criminality_indicateur', 'indicateur_id::VARCHAR', ['criminality']) }}
UNION ALL
{{ bridge_source_links_simple('fact_criminality', "commune_id || '|' || annee || '|' || indicateur_id", ['criminality']) }}
