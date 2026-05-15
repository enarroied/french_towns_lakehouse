

WITH prepare_join AS (
    SELECT
        id,
        geometry,
        department_code,
        flag_corsica,
        flag_metropole,
        ST_Envelope(geometry) AS bbox
    FROM "french_towns"."main"."dim_communes_france"
),
suspected AS (
    SELECT
        a.id AS parcel_id,
        a.geometry AS geometry_a,
        b.id AS neighbor_id,
        b.geometry AS geometry_b
    FROM prepare_join a
    JOIN prepare_join b ON ST_Intersects(a.bbox, b.bbox)
    WHERE a.id != b.id
      AND a.flag_corsica = b.flag_corsica
      AND a.flag_metropole = b.flag_metropole
      AND CASE
              WHEN a.flag_metropole = 1 AND b.flag_metropole = 1
              THEN a.department_code = b.department_code
          END
)
SELECT
    parcel_id,
    neighbor_id
FROM suspected
WHERE ST_Intersects(geometry_a, geometry_b) IS TRUE