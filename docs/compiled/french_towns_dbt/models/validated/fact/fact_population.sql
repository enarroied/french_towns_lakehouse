

WITH base AS (
    SELECT
        GEO::CHAR(5)            AS id,
        TIME_PERIOD::INTEGER    AS year,
        OBS_VALUE::INTEGER      AS population
    FROM read_csv_auto('s3://staging-current/demographics/DS_POPULATIONS_HISTORIQUES_data.csv', sample_size=-1)
    WHERE GEO_OBJECT = 'COM'
)
SELECT
    b.id,
    b.year,
    b.population,
    (b.population - p.population) AS year_evolution,
    CASE
        WHEN p.population IS NOT NULL AND p.population != 0
        THEN ((b.population - p.population) * 100.0 / p.population)
        ELSE NULL
    END AS year_evolution_percent
FROM base b
LEFT JOIN base p
    ON b.id = p.id
    AND b.year = p.year + 1
ORDER BY b.id, b.year