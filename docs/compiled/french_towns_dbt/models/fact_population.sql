

SELECT
    GEO::CHAR(5)            AS id,
    TIME_PERIOD::INTEGER    AS year,
    OBS_VALUE::INTEGER      AS population
FROM read_csv_auto('../input/DS_POPULATIONS_HISTORIQUES_data.csv', sample_size=-1)
WHERE GEO_OBJECT = 'COM'