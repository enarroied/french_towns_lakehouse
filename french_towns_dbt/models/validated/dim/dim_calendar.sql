{{ config(
    materialized='external',
    location='s3://validated/' ~ this.name ~ '.parquet'
) }}

WITH calendar AS (
    SELECT UNNEST(GENERATE_SERIES(DATE '1900-01-01', DATE '2100-12-31', INTERVAL 1 DAY)) AS date
),
calendar_parts AS (
    SELECT
        date,
        YEAR(date) * 10000 + MONTH(date) * 100 + DAY(date) AS date_id,
        YEAR(date) AS year,
        MONTH(date) AS month,
        DAY(date) AS day,
        (DAYOFYEAR(date))::INT AS day_of_year,
        QUARTER(date)::INT AS quarter,
        STRFTIME(date, '%Y-%m') AS year_month,
        STRFTIME(date, '%V')::INT AS iso_week,
        ISODOW(date)::INT AS iso_day_of_week,
        CASE STRFTIME(date, '%A')
            WHEN 'Monday' THEN 'Lundi'
            WHEN 'Tuesday' THEN 'Mardi'
            WHEN 'Wednesday' THEN 'Mercredi'
            WHEN 'Thursday' THEN 'Jeudi'
            WHEN 'Friday' THEN 'Vendredi'
            WHEN 'Saturday' THEN 'Samedi'
            WHEN 'Sunday' THEN 'Dimanche'
        END AS day_name_fr,
        CASE MONTH(date)
            WHEN 1 THEN 'Janvier'
            WHEN 2 THEN 'Février'
            WHEN 3 THEN 'Mars'
            WHEN 4 THEN 'Avril'
            WHEN 5 THEN 'Mai'
            WHEN 6 THEN 'Juin'
            WHEN 7 THEN 'Juillet'
            WHEN 8 THEN 'Août'
            WHEN 9 THEN 'Septembre'
            WHEN 10 THEN 'Octobre'
            WHEN 11 THEN 'Novembre'
            WHEN 12 THEN 'Décembre'
        END AS month_name_fr,
        CASE STRFTIME(date, '%A')
            WHEN 'Monday' THEN 'Monday'
            WHEN 'Tuesday' THEN 'Tuesday'
            WHEN 'Wednesday' THEN 'Wednesday'
            WHEN 'Thursday' THEN 'Thursday'
            WHEN 'Friday' THEN 'Friday'
            WHEN 'Saturday' THEN 'Saturday'
            WHEN 'Sunday' THEN 'Sunday'
        END AS day_name_en,
        CASE MONTH(date)
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END AS month_name_en,
        CASE WHEN ISODOW(date)::INT >= 6 THEN TRUE ELSE FALSE END AS is_weekend
    FROM calendar
)
SELECT
    cp.date_id,
    cp.date,
    cp.year,
    cp.month,
    cp.day,
    cp.day_of_year,
    cp.quarter,
    cp.year_month,
    cp.iso_week,
    cp.iso_day_of_week,
    cp.day_name_fr,
    cp.month_name_fr,
    cp.day_name_en,
    cp.month_name_en,
    cp.is_weekend,
    COALESCE(f.is_public_holiday::BOOLEAN, FALSE) AS is_public_holiday,
    COALESCE(f.holiday_name, '') AS fete,
    COALESCE(m.is_market_holiday::BOOLEAN, FALSE) AS is_market_holiday,
    COALESCE(r.is_christian_holiday::BOOLEAN, FALSE) AS is_christian_holiday,
    COALESCE(r.is_jewish_holiday::BOOLEAN, FALSE) AS is_jewish_holiday,
    COALESCE(r.is_muslim_holiday::BOOLEAN, FALSE) AS is_muslim_holiday,
    COALESCE(r.is_chinese_holiday::BOOLEAN, FALSE) AS is_chinese_holiday,
    COALESCE(p.name, '') AS president,
    COALESCE(pm.name, '') AS prime_minister,
    COALESCE(l.name, '') AS legislature,
    COALESCE(lm.moon_phase_value::DOUBLE, 0.0) AS moon_phase_value,
    COALESCE(lm.moon_phase_name, '') AS moon_phase_name,
    COALESCE(lm.moon_illumination_fraction::DOUBLE, 0.0) AS moon_illumination_fraction,
    COALESCE(lm.is_full_moon::BOOLEAN, FALSE) AS is_full_moon,
    COALESCE(lm.is_new_moon::BOOLEAN, FALSE) AS is_new_moon,
    COALESCE(lm.lunar_cycle_id::INTEGER, 0) AS lunar_cycle_id
FROM calendar_parts cp
LEFT JOIN {{ source('french_towns', 'french_holidays') }} f
    ON cp.date = f.date
LEFT JOIN {{ source('french_towns', 'market_holidays') }} m
    ON cp.date = m.date
LEFT JOIN {{ source('french_towns', 'religious_holidays') }} r
    ON cp.date = r.date
LEFT JOIN {{ source('french_towns', 'french_presidents') }} p
    ON cp.date >= p.start_date AND (cp.date < p.end_date OR p.end_date IS NULL)
LEFT JOIN {{ source('french_towns', 'french_prime_ministers') }} pm
    ON cp.date >= pm.start_date AND (cp.date < pm.end_date OR pm.end_date IS NULL)
LEFT JOIN {{ source('french_towns', 'french_legislatures') }} l
    ON cp.date >= l.start_date AND (cp.date < l.end_date OR l.end_date IS NULL)
LEFT JOIN {{ source('french_towns', 'lunar_phases') }} lm
    ON cp.date = lm.date
ORDER BY cp.date_id
