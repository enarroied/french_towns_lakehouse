

WITH population_base AS (
    SELECT DISTINCT
        GEO::CHAR(5)            AS id,
        TIME_PERIOD::INTEGER    AS year,
        OBS_VALUE::INTEGER      AS population
    FROM read_csv_auto('s3://staging-current/demographics/historical_population_*.csv', sample_size=-1)
    WHERE GEO_OBJECT = 'COM'
),
births_base AS (
    SELECT DISTINCT
        GEO::CHAR(5)            AS id,
        TIME_PERIOD::INTEGER    AS year,
        OBS_VALUE::INTEGER      AS births
    FROM read_csv_auto('s3://staging-current/demographics/births_*.csv', sample_size=-1)
    WHERE GEO_OBJECT = 'COM' AND EC_MEASURE = 'LVB'
),
deaths_base AS (
    SELECT DISTINCT
        GEO::CHAR(5)            AS id,
        TIME_PERIOD::INTEGER    AS year,
        OBS_VALUE::INTEGER      AS deaths
    FROM read_csv_auto('s3://staging-current/demographics/deaths_*.csv', sample_size=-1)
    WHERE GEO_OBJECT = 'COM' AND EC_MEASURE = 'DTH' AND OBS_STATUS = 'A'
),
family_base AS (
    SELECT
        GEO::CHAR(5)            AS id,
        TIME_PERIOD::INTEGER    AS year,
        MAX(CASE WHEN NCH = 'CH0_Y_LT25' AND TFN = '_T' THEN OBS_VALUE::INTEGER END) AS no_children_u24,
        MAX(CASE WHEN NCH = 'CH1_Y_LT25' AND TFN = '_T' THEN OBS_VALUE::INTEGER END) AS one_child_u24,
        MAX(CASE WHEN NCH = 'CH2_Y_LT25' AND TFN = '_T' THEN OBS_VALUE::INTEGER END) AS two_children_u24,
        MAX(CASE WHEN NCH = 'CH3_Y_LT25' AND TFN = '_T' THEN OBS_VALUE::INTEGER END) AS three_children_u24,
        MAX(CASE WHEN NCH = 'CH_GE4_Y_LT25' AND TFN = '_T' THEN OBS_VALUE::INTEGER END) AS four_or_more_children_u24,
        MAX(CASE WHEN TFN = '11' AND NCH = '_T' THEN OBS_VALUE::INTEGER END) AS number_single_father_families,
        MAX(CASE WHEN TFN = '12' AND NCH = '_T' THEN OBS_VALUE::INTEGER END) AS number_single_mother_families,
        MAX(CASE WHEN TFN = '223' AND NCH = '_T' THEN OBS_VALUE::INTEGER END) AS number_traditional_families,
        MAX(CASE WHEN TFN = '21' AND NCH = '_T' THEN OBS_VALUE::INTEGER END) AS number_couples_no_children_home,
        MAX(CASE WHEN TFN = '22' AND NCH = '_T' THEN OBS_VALUE::INTEGER END) AS number_couples_children_home
    FROM read_csv_auto('s3://staging-current/demographics/family_*.csv', sample_size=-1)
    WHERE GEO_OBJECT = 'COM'
    GROUP BY GEO, TIME_PERIOD
),
migration_base AS (
    SELECT
        CODGEO::CHAR(5)         AS id,
        2022                    AS year,
        SUM(CASE WHEN IMMI = '1' AND SEXE = '1' THEN NB::DOUBLE END) AS number_migrant_male,
        SUM(CASE WHEN IMMI = '1' AND SEXE = '2' THEN NB::DOUBLE END) AS number_migrant_female,
        SUM(CASE WHEN IMMI = '2' AND SEXE = '1' THEN NB::DOUBLE END) AS number_french_male,
        SUM(CASE WHEN IMMI = '2' AND SEXE = '2' THEN NB::DOUBLE END) AS number_french_female,
        SUM(CASE WHEN IMMI = '1' THEN NB::DOUBLE END) AS number_migrant,
        SUM(CASE WHEN IMMI = '2' THEN NB::DOUBLE END) AS number_french,
        SUM(CASE WHEN SEXE = '1' THEN NB::DOUBLE END) AS number_male,
        SUM(CASE WHEN SEXE = '2' THEN NB::DOUBLE END) AS number_female,
        SUM(NB::DOUBLE) AS number_total
    FROM read_csv_auto('s3://staging-current/demographics/migration_*.csv', sample_size=-1)
    WHERE NIVGEO = 'COM'
    GROUP BY CODGEO
),
all_years AS (
    SELECT DISTINCT id, year FROM (
        SELECT id, year FROM population_base
        UNION
        SELECT id, year FROM births_base
        UNION
        SELECT id, year FROM deaths_base
        UNION
        SELECT id, year FROM family_base
        UNION
        SELECT id, year FROM migration_base
    )
)
SELECT
    a.id,
    a.year,
    p.population,
    (p.population - pp.population) AS year_evolution,
    CASE
        WHEN pp.population IS NOT NULL AND pp.population != 0
        THEN ((p.population - pp.population) * 100.0 / pp.population)
        ELSE NULL
    END AS year_evolution_percent,
    br.births,
    de.deaths,
    (br.births - de.deaths) AS birth_death_diff,
    fa.no_children_u24,
    fa.one_child_u24,
    fa.two_children_u24,
    fa.three_children_u24,
    fa.four_or_more_children_u24,
    fa.number_single_father_families,
    fa.number_single_mother_families,
    fa.number_traditional_families,
    fa.number_couples_no_children_home,
    fa.number_couples_children_home,
    mg.number_french,
    mg.number_migrant,
    mg.number_migrant_male,
    mg.number_migrant_female,
    mg.number_french_male,
    mg.number_french_female,
    mg.number_male,
    mg.number_female,
    mg.number_total
FROM all_years a
LEFT JOIN population_base p
    ON a.id = p.id AND a.year = p.year
LEFT JOIN population_base pp
    ON a.id = pp.id AND a.year = pp.year + 1
LEFT JOIN births_base br
    ON a.id = br.id AND a.year = br.year
LEFT JOIN deaths_base de
    ON a.id = de.id AND a.year = de.year
LEFT JOIN family_base fa
    ON a.id = fa.id AND a.year = fa.year
LEFT JOIN migration_base mg
    ON a.id = mg.id AND a.year = mg.year
ORDER BY a.id, a.year