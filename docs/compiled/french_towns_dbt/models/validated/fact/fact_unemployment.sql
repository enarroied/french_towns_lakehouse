

WITH source AS (
    SELECT
        CODGEO::CHAR(5)                                            AS commune_code,
        year::INTEGER                                               AS year,

        ACT1564::DECIMAL                                            AS labor_force_count_total_15_64,
        HACT1564::DECIMAL                                           AS labor_force_count_men_15_64,
        FACT1564::DECIMAL                                           AS labor_force_count_women_15_64,
        ACT1524::DECIMAL                                            AS labor_force_count_total_15_24,
        HACT1524::DECIMAL                                           AS labor_force_count_men_15_24,
        FACT1524::DECIMAL                                           AS labor_force_count_women_15_24,
        ACT2554::DECIMAL                                            AS labor_force_count_total_25_54,
        HACT2554::DECIMAL                                           AS labor_force_count_men_25_54,
        FACT2554::DECIMAL                                           AS labor_force_count_women_25_54,
        ACT5564::DECIMAL                                            AS labor_force_count_total_55_64,
        HACT5564::DECIMAL                                           AS labor_force_count_men_55_64,
        FACT5564::DECIMAL                                           AS labor_force_count_women_55_64,

        COALESCE(CHOM1564::DECIMAL, HCHOM1564::DECIMAL + FCHOM1564::DECIMAL)
                                                                    AS unemployed_count_total_15_64,
        HCHOM1564::DECIMAL                                          AS unemployed_count_men_15_64,
        FCHOM1564::DECIMAL                                          AS unemployed_count_women_15_64,
        COALESCE(CHOM1524::DECIMAL, HCHOM1524::DECIMAL + FCHOM1524::DECIMAL)
                                                                    AS unemployed_count_total_15_24,
        HCHOM1524::DECIMAL                                          AS unemployed_count_men_15_24,
        FCHOM1524::DECIMAL                                          AS unemployed_count_women_15_24,
        COALESCE(CHOM2554::DECIMAL, HCHOM2554::DECIMAL + FCHOM2554::DECIMAL)
                                                                    AS unemployed_count_total_25_54,
        HCHOM2554::DECIMAL                                          AS unemployed_count_men_25_54,
        FCHOM2554::DECIMAL                                          AS unemployed_count_women_25_54,
        COALESCE(CHOM5564::DECIMAL, HCHOM5564::DECIMAL + FCHOM5564::DECIMAL)
                                                                    AS unemployed_count_total_55_64,
        HCHOM5564::DECIMAL                                          AS unemployed_count_men_55_64,
        FCHOM5564::DECIMAL                                          AS unemployed_count_women_55_64

    FROM read_csv_auto('s3://staging-current/demographics/unemployment_*.csv', sample_size=-1)
),

with_rates AS (
    SELECT
        *,

        unemployed_count_total_15_64
            / NULLIF(labor_force_count_total_15_64, 0) * 100
            AS unemployment_rate_total_15_64,
        unemployed_count_men_15_64
            / NULLIF(labor_force_count_men_15_64, 0) * 100
            AS unemployment_rate_men_15_64,
        unemployed_count_women_15_64
            / NULLIF(labor_force_count_women_15_64, 0) * 100
            AS unemployment_rate_women_15_64,

        unemployed_count_total_15_24
            / NULLIF(labor_force_count_total_15_24, 0) * 100
            AS unemployment_rate_total_15_24,
        unemployed_count_men_15_24
            / NULLIF(labor_force_count_men_15_24, 0) * 100
            AS unemployment_rate_men_15_24,
        unemployed_count_women_15_24
            / NULLIF(labor_force_count_women_15_24, 0) * 100
            AS unemployment_rate_women_15_24,

        unemployed_count_total_25_54
            / NULLIF(labor_force_count_total_25_54, 0) * 100
            AS unemployment_rate_total_25_54,
        unemployed_count_men_25_54
            / NULLIF(labor_force_count_men_25_54, 0) * 100
            AS unemployment_rate_men_25_54,
        unemployed_count_women_25_54
            / NULLIF(labor_force_count_women_25_54, 0) * 100
            AS unemployment_rate_women_25_54,

        unemployed_count_total_55_64
            / NULLIF(labor_force_count_total_55_64, 0) * 100
            AS unemployment_rate_total_55_64,
        unemployed_count_men_55_64
            / NULLIF(labor_force_count_men_55_64, 0) * 100
            AS unemployment_rate_men_55_64,
        unemployed_count_women_55_64
            / NULLIF(labor_force_count_women_55_64, 0) * 100
            AS unemployment_rate_women_55_64,

        CASE
            WHEN labor_force_count_total_15_64 >= 500 THEN 'reliable'
            WHEN labor_force_count_total_15_64 >= 200 THEN 'caution'
            ELSE 'unreliable'
        END AS reliability_flag

    FROM source
),

prev AS (
    SELECT * FROM with_rates
)

SELECT
    c.commune_code,
    c.year,

    c.labor_force_count_total_15_64,
    c.labor_force_count_men_15_64,
    c.labor_force_count_women_15_64,
    c.labor_force_count_total_15_24,
    c.labor_force_count_men_15_24,
    c.labor_force_count_women_15_24,
    c.labor_force_count_total_25_54,
    c.labor_force_count_men_25_54,
    c.labor_force_count_women_25_54,
    c.labor_force_count_total_55_64,
    c.labor_force_count_men_55_64,
    c.labor_force_count_women_55_64,

    c.unemployed_count_total_15_64,
    c.unemployed_count_men_15_64,
    c.unemployed_count_women_15_64,
    c.unemployed_count_total_15_24,
    c.unemployed_count_men_15_24,
    c.unemployed_count_women_15_24,
    c.unemployed_count_total_25_54,
    c.unemployed_count_men_25_54,
    c.unemployed_count_women_25_54,
    c.unemployed_count_total_55_64,
    c.unemployed_count_men_55_64,
    c.unemployed_count_women_55_64,

    c.unemployment_rate_total_15_64,
    c.unemployment_rate_men_15_64,
    c.unemployment_rate_women_15_64,
    c.unemployment_rate_total_15_24,
    c.unemployment_rate_men_15_24,
    c.unemployment_rate_women_15_24,
    c.unemployment_rate_total_25_54,
    c.unemployment_rate_men_25_54,
    c.unemployment_rate_women_25_54,
    c.unemployment_rate_total_55_64,
    c.unemployment_rate_men_55_64,
    c.unemployment_rate_women_55_64,

    c.reliability_flag,

    p.labor_force_count_total_15_64  AS prev_labor_force_count_total_15_64,
    p.unemployed_count_total_15_64   AS prev_unemployed_count_total_15_64,
    p.unemployment_rate_total_15_64  AS prev_unemployment_rate_total_15_64,

    c.labor_force_count_total_15_64 - p.labor_force_count_total_15_64
        AS labor_force_evolution_total_15_64,
    c.unemployed_count_total_15_64 - p.unemployed_count_total_15_64
        AS unemployed_evolution_total_15_64,
    c.unemployment_rate_total_15_64 - p.unemployment_rate_total_15_64
        AS unemployment_rate_evolution_total_15_64

FROM with_rates c
LEFT JOIN prev p
    ON c.commune_code = p.commune_code AND c.year = p.year + 1
ORDER BY c.commune_code, c.year