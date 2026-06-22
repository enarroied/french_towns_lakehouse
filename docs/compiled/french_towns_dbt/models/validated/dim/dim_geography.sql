

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
)
SELECT
    commune_id,
    name,
    department_code,
    region_code,
    epci,
    geometry,
    ST_Centroid(geometry)                                                   AS centroid,
    CASE
        WHEN LENGTH(department_code) = 2 AND department_code NOT IN ('2A', '2B')
            THEN ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:2154'))  / 1000000
        WHEN department_code = '2A' OR department_code = '2B'
            THEN NULL
        WHEN department_code = '971'
            THEN ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:2970')) / 1000000
        WHEN department_code = '972'
            THEN ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:2973')) / 1000000
        WHEN department_code = '973'
            THEN ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:2972')) / 1000000
        WHEN department_code = '974'
            THEN ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:2975')) / 1000000
        WHEN department_code = '975'
            THEN ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:2981')) / 1000000
        WHEN department_code = '976'
            THEN ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:4471')) / 1000000
        WHEN department_code = '988'
            THEN ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:2980')) / 1000000
        ELSE NULL
    END AS area_km2,
    ST_XMin(geometry)                   AS bbox_xmin,
    ST_XMax(geometry)                   AS bbox_xmax,
    ST_YMin(geometry)                   AS bbox_ymin,
    ST_YMax(geometry)                   AS bbox_ymax,
    ST_Perimeter(geometry)              AS perimeter,
    ST_NumGeometries(geometry) > 1      AS is_multipolygon,
    CASE
        WHEN ST_GeometryType(geometry) = 'POLYGON'
            THEN ST_NumInteriorRings(geometry)
        WHEN ST_GeometryType(geometry) = 'MULTIPOLYGON'
            THEN (SELECT SUM(ST_NumInteriorRings(d.geom))
                  FROM UNNEST(ST_Dump(geometry)) AS t(d))
        ELSE NULL
    END AS number_enclaves
FROM source