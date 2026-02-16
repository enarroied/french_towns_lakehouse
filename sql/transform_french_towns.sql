COPY (
    SELECT
        com_code[1]::CHAR(5) AS id,
        com_current_code[1]::CHAR(5) as current_code,
        com_name[1]::VARCHAR(255) AS name,
        com_name_upper AS name_upper,
        com_name_lower AS name_lower,
        com_siren_code AS siren_code,
        arrdep_name[1]::VARCHAR(255) AS arrondissement_name,
        arrdep_code[1]::CHAR(7) AS arrondissement_code,
        dep_name[1]::VARCHAR(255) AS department_name,
        dep_code[1]::VARCHAR(3) AS department_code,
        reg_name[1]::VARCHAR(255) AS region_name,
        reg_code[1]::INTEGER AS region_code,
        CASE
            WHEN department_code IN ('2A', '2B') THEN 1
            ELSE 0
        END AS flag_corsica,
        CASE
            WHEN LENGTH(department_code) = 3 THEN 0
            ELSE 1
        END AS flag_metropole,
        CASE
            WHEN dpt.CHEFLIEU IS NOT NULL THEN 1
            ELSE 0
        END AS flag_prefecture,
        CASE
            WHEN arr.CHEFLIEU IS NOT NULL THEN 1
            ELSE 0
        END AS flag_chef_lieu_arrondissement,
        CASE
            WHEN arr.CHEFLIEU IS NOT NULL AND flag_prefecture = 0 THEN 1
            ELSE 0
        END AS flag_sous_prefecture,
        geom AS geometry,
        geo_point_2d,
        ST_Area(ST_Transform(geom, 'EPSG:4326', 'EPSG:2154')) as area_m2,
        ST_Centroid(geom) as centroid,
        ST_XMin(geom) as bbox_xmin,
        ST_XMax(geom) as bbox_xmax,
        ST_YMin(geom) as bbox_ymin,
        ST_YMax(geom) as bbox_ymax,
        ST_Perimeter(geom) as perimeter,
        ST_NumInteriorRings(geom) as number_enclaves
    FROM ST_Read('{{input_file}}')
        LEFT JOIN read_csv_auto('{{departements_file}}') AS dpt
            ON dpt.CHEFLIEU = com_code[1]
        LEFT JOIN read_csv_auto('{{arrondissements_file}}') AS arr
            ON arr.CHEFLIEU = com_code[1]
) TO '{{output_file}}'
