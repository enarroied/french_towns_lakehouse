

SELECT
    com_code[1]::CHAR(5)            AS id,
    com_current_code[1]::CHAR(5)    AS current_code,
    com_name[1]::VARCHAR(255)       AS name,
    com_name_upper::VARCHAR(255)    AS name_upper,
    com_name_lower::VARCHAR(255)    AS name_lower,
    com_siren_code::CHAR(10)    AS siren_code,
    arrdep_name[1]::VARCHAR(255)    AS arrondissement_name,
    arrdep_code[1]::CHAR(7)         AS arrondissement_code,
    dep_name[1]::VARCHAR(255)       AS department_name,
    dep_code[1]::VARCHAR(3)         AS department_code,
    reg_name[1]::VARCHAR(255)       AS region_name,
    reg_code[1]::INTEGER            AS region_code,
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
    geom                                                            AS geometry,
    geo_point_2d,
    CASE
        WHEN flag_metropole = 1
            THEN ST_Area(ST_Transform(geom, 'EPSG:4326', 'EPSG:2154'))  / 1000000  -- Lambert-93 for mainland
        WHEN department_code = '971' -- Guadeloupe
            THEN ST_Area(ST_Transform(geom, 'EPSG:4326', 'EPSG:2970')) / 1000000
        WHEN department_code = '972'  -- Martinique
            THEN ST_Area(ST_Transform(geom, 'EPSG:4326', 'EPSG:2973')) / 1000000
        WHEN department_code = '973'  -- French Guiana
            THEN ST_Area(ST_Transform(geom, 'EPSG:4326', 'EPSG:2972')) / 1000000
        WHEN department_code = '974'  -- Reunion
            THEN ST_Area(ST_Transform(geom, 'EPSG:4326', 'EPSG:2975')) / 1000000
        WHEN department_code = '975'  -- St. Pierre et Miquelon
            THEN ST_Area(ST_Transform(geom, 'EPSG:4326', 'EPSG:2981')) / 1000000
        WHEN department_code = '976'  -- Mayotte
            THEN ST_Area(ST_Transform(geom, 'EPSG:4326', 'EPSG:4471')) / 1000000
        ELSE NULL   -- We cant assume area if we don't have a proper projection
    END AS area_km2,
    ST_Centroid(geom)                                               AS centroid,
    ST_XMin(geom)                                                   AS bbox_xmin,
    ST_XMax(geom)                                                   AS bbox_xmax,
    ST_YMin(geom)                                                   AS bbox_ymin,
    ST_YMax(geom)                                                   AS bbox_ymax,
    ST_Perimeter(geom)                                              AS perimeter,
    ST_NumInteriorRings(geom)                                       AS number_enclaves
FROM ST_Read('../input/communes_france.geojson')
LEFT JOIN read_csv_auto('../input/departements.csv') AS dpt
    ON dpt.CHEFLIEU = com_code[1]
LEFT JOIN read_csv_auto('../input/arrondissements.csv') AS arr
    ON arr.CHEFLIEU = com_code[1]