

WITH prepare_join AS (
    SELECT
        g.commune_id AS id,
        g.geometry,
        c.department_code,
        c.flag_corsica,
        c.flag_metropole,
        ST_Envelope(g.geometry) AS bbox
    FROM "french_towns"."main"."dim_geography" g
    JOIN "french_towns"."main"."dim_communes" c
        ON g.commune_id = c.id
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