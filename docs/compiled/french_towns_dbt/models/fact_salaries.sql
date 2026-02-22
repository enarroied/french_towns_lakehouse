

SELECT
    GEO::CHAR(5)                                          AS id,
    TIME_PERIOD::INTEGER                                  AS year,
    MAX(CASE WHEN SEX = 'M'  THEN OBS_VALUE::INTEGER END) AS mean_salary_men,
    MAX(CASE WHEN SEX = 'F'  THEN OBS_VALUE::INTEGER END) AS mean_salary_women,
    MAX(CASE WHEN SEX = '_T' THEN OBS_VALUE::INTEGER END) AS mean_salary
FROM read_csv_auto('../input/DS_BTS_SAL_EQTP_SEX_PCS_2023_data.csv', sample_size=-1)
WHERE
    GEO_OBJECT = 'COM'
    AND PCS_ESE = '_T'
GROUP BY GEO, TIME_PERIOD
ORDER BY GEO, year