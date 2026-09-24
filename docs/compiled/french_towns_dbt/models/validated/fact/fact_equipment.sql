

WITH equipment_base AS (
    SELECT
        GEO::CHAR(5)            AS id,
        TIME_PERIOD::INTEGER    AS year,
        FACILITY_TYPE           AS equipment_code,
        SUM(OBS_VALUE::INTEGER) AS count
    FROM read_csv_auto('s3://staging-current/equipment/bpe_*.csv', sample_size=-1, delim=';')
    WHERE GEO_OBJECT = 'COM'
      AND FACILITY_TYPE != '_T'
    GROUP BY GEO, TIME_PERIOD, FACILITY_TYPE
)
SELECT
    c.id                                    AS commune_id,
    e.year,
    e.year * 10000 + 101                    AS date_id,
    eq.equipment_type_id,
    1                   AS source_id,
    e.count
FROM equipment_base e
LEFT JOIN "french_towns"."main"."dim_communes" c
    ON e.id = c.id
LEFT JOIN "french_towns"."main"."dim_equipment" eq
    ON e.equipment_code = eq.equipment_code
ORDER BY commune_id, year, equipment_type_id