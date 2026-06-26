

WITH source AS (
    SELECT
        code::CHAR(5)                   AS commune_id,
        nom::VARCHAR(255)               AS name,
        departement::VARCHAR(3)         AS department_code,
        region::VARCHAR(2)              AS region_code,
        epci::VARCHAR(9)                AS epci,
        geom::GEOMETRY                  AS geometry
    FROM ST_Read(
        
    
    
        's3://staging-current/geography/french_towns_*.geojson'
    

    )
),
mountain_zones AS (
    SELECT DISTINCT commune_id::CHAR(5) AS commune_id
    FROM read_csv_auto(
        's3://staging-current/geography/mountain_zones_*.csv',
        header=true
    )
),
littoral AS (
    SELECT DISTINCT
        commune_id::CHAR(5) AS commune_id,
        is_coast::BOOLEAN   AS is_coast,
        has_estuary::BOOLEAN AS has_estuary,
        has_lake::BOOLEAN   AS has_lake
    FROM read_csv_auto(
        's3://staging-current/geography/littoral_*.csv',
        header=true
    )
),
altitude AS (
    SELECT DISTINCT
        commune_id::CHAR(5)  AS commune_id,
        altitude_min::INTEGER AS altitude_min,
        altitude_max::INTEGER AS altitude_max,
        altitude_moyenne::DOUBLE AS altitude_moyenne
    FROM read_csv_auto(
        's3://staging-current/geography/altitude_*.csv',
        header=true
    )
),
island_communes AS (
    SELECT DISTINCT
        id::CHAR(5)            AS commune_id,
        is_insular::BOOLEAN    AS is_insular,
        is_island_commune::BOOLEAN AS is_island_commune
    FROM read_csv_auto(
        's3://staging-current/geography/island_communes_*.csv',
        header=true
    )
)
SELECT
    source.commune_id,
    source.name,
    source.department_code,
    source.region_code,
    source.epci,
    source.geometry,
    mz.commune_id IS NOT NULL          AS is_mountain,
    COALESCE(l.is_coast, false)        AS is_coast,
    COALESCE(l.has_estuary, false)     AS has_estuary,
    COALESCE(l.has_lake, false)        AS has_lake,
    a.altitude_min,
    a.altitude_max,
    a.altitude_moyenne,
    COALESCE(ic.is_insular, false)             AS is_insular,
    COALESCE(ic.is_island_commune, false)      AS is_island_commune,
    ST_Centroid(source.geometry)       AS centroid,
    CASE
        WHEN LENGTH(source.department_code) = 2 AND source.department_code NOT IN ('2A', '2B')
            THEN ST_Area(ST_Transform(source.geometry, 'EPSG:4326', 'EPSG:2154'))  / 1000000
        WHEN source.department_code = '2A' OR source.department_code = '2B'
            THEN NULL
        WHEN source.department_code = '971'
            THEN ST_Area(ST_Transform(source.geometry, 'EPSG:4326', 'EPSG:2970')) / 1000000
        WHEN source.department_code = '972'
            THEN ST_Area(ST_Transform(source.geometry, 'EPSG:4326', 'EPSG:2973')) / 1000000
        WHEN source.department_code = '973'
            THEN ST_Area(ST_Transform(source.geometry, 'EPSG:4326', 'EPSG:2972')) / 1000000
        WHEN source.department_code = '974'
            THEN ST_Area(ST_Transform(source.geometry, 'EPSG:4326', 'EPSG:2975')) / 1000000
        WHEN source.department_code = '975'
            THEN ST_Area(ST_Transform(source.geometry, 'EPSG:4326', 'EPSG:2981')) / 1000000
        WHEN source.department_code = '976'
            THEN ST_Area(ST_Transform(source.geometry, 'EPSG:4326', 'EPSG:4471')) / 1000000
        WHEN source.department_code = '988'
            THEN ST_Area(ST_Transform(source.geometry, 'EPSG:4326', 'EPSG:2980')) / 1000000
        ELSE NULL
    END AS area_km2,
    ST_XMin(source.geometry)                   AS bbox_xmin,
    ST_XMax(source.geometry)                   AS bbox_xmax,
    ST_YMin(source.geometry)                   AS bbox_ymin,
    ST_YMax(source.geometry)                   AS bbox_ymax,
    ST_Perimeter(source.geometry)              AS perimeter,
    ST_NumGeometries(source.geometry) > 1      AS is_multipolygon,
    CASE
        WHEN ST_GeometryType(source.geometry) = 'POLYGON'
            THEN ST_NumInteriorRings(source.geometry)
        WHEN ST_GeometryType(source.geometry) = 'MULTIPOLYGON'
            THEN (SELECT SUM(ST_NumInteriorRings(d.geom))
                  FROM UNNEST(ST_Dump(source.geometry)) AS t(d))
        ELSE NULL
    END AS number_enclaves
FROM source
LEFT JOIN mountain_zones mz ON source.commune_id = mz.commune_id
LEFT JOIN littoral l ON source.commune_id = l.commune_id
LEFT JOIN altitude a ON source.commune_id = a.commune_id
LEFT JOIN island_communes ic ON source.commune_id = ic.commune_id