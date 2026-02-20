COPY (
    SELECT
        GEO AS id,
        TIME_PERIOD::INTEGER AS year,
        MAX(CASE WHEN SEX = 'M'  THEN OBS_VALUE::INTEGER END) AS mean_salary_men,
        MAX(CASE WHEN SEX = 'F'  THEN OBS_VALUE::INTEGER END) AS mean_salary_women,
        MAX(CASE WHEN SEX = '_T' THEN OBS_VALUE::INTEGER END) AS mean_salary
    FROM read_csv_auto(
        '{{input_file}}',
        types = {'GEO': 'CHAR(5)'}   -- good for Corsica / overseas etc.
    )
    WHERE GEO_OBJECT = 'COM'
    GROUP BY GEO, TIME_PERIOD
    ORDER BY GEO, year
) TO '{{output_file}}';
