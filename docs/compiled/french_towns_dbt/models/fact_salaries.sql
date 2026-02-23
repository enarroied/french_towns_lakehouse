

SELECT
    GEO::CHAR(5)                                          AS id,
    TIME_PERIOD::INTEGER                                  AS year,
    MAX(CASE WHEN SEX = 'M' AND PCS_ESE = '_T' THEN OBS_VALUE::INTEGER END) AS mean_salary_men,
    MAX(CASE WHEN SEX = 'F' AND PCS_ESE = '_T' THEN OBS_VALUE::INTEGER END) AS mean_salary_women,
    MAX(CASE WHEN SEX = '_T' AND PCS_ESE = '1T3' THEN OBS_VALUE::INTEGER END) AS mean_salary_management_position,
    MAX(CASE WHEN SEX = '_T' AND PCS_ESE = '4' THEN OBS_VALUE::INTEGER END) AS mean_salary_intermediate_position,
    MAX(CASE WHEN SEX = '_T' AND PCS_ESE = '5' THEN OBS_VALUE::INTEGER END) AS mean_salary_service_workers,
    MAX(CASE WHEN SEX = '_T' AND PCS_ESE = '6' THEN OBS_VALUE::INTEGER END) AS mean_salary_manual_workers,
    MAX(CASE WHEN SEX = 'M' AND PCS_ESE = '1T3' THEN OBS_VALUE::INTEGER END) AS mean_salary_management_position_men,
    MAX(CASE WHEN SEX = 'M' AND PCS_ESE = '4' THEN OBS_VALUE::INTEGER END) AS mean_salary_intermediate_position_men,
    MAX(CASE WHEN SEX = 'M' AND PCS_ESE = '5' THEN OBS_VALUE::INTEGER END) AS mean_salary_service_workers_men,
    MAX(CASE WHEN SEX = 'M' AND PCS_ESE = '6' THEN OBS_VALUE::INTEGER END) AS mean_salary_manual_workers_men,
    MAX(CASE WHEN SEX = 'F' AND PCS_ESE = '1T3' THEN OBS_VALUE::INTEGER END) AS mean_salary_management_position_women,
    MAX(CASE WHEN SEX = 'F' AND PCS_ESE = '4' THEN OBS_VALUE::INTEGER END) AS mean_salary_intermediate_position_women,
    MAX(CASE WHEN SEX = 'F' AND PCS_ESE = '5' THEN OBS_VALUE::INTEGER END) AS mean_salary_service_workers_women,
    MAX(CASE WHEN SEX = 'F' AND PCS_ESE = '6' THEN OBS_VALUE::INTEGER END) AS mean_salary_manual_workers_women,
    MAX(CASE WHEN SEX = 'F' AND PCS_ESE = '_T' THEN OBS_VALUE::INTEGER END) AS mean_salary
FROM read_csv_auto('../input/DS_BTS_SAL_EQTP_SEX_PCS_2023_data.csv', sample_size=-1)
WHERE
    GEO_OBJECT = 'COM'

GROUP BY GEO, TIME_PERIOD
ORDER BY GEO, year