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
    cp.is_weekend,
    COALESCE(f.is_public_holiday::BOOLEAN, FALSE) AS is_public_holiday,
    COALESCE(f.holiday_name, '') AS fete,
    COALESCE(m.is_market_holiday::BOOLEAN, FALSE) AS is_market_holiday,
    COALESCE(m.market_holiday_name, '') AS market_holiday_name,
    COALESCE(p.president, '') AS president,
    COALESCE(p.prime_minister, '') AS prime_minister,
    COALESCE(p.legislature::VARCHAR, '') AS legislature
FROM calendar_parts cp
LEFT JOIN {{ source('french_towns', 'french_holidays') }} f
    ON cp.date = f.date
LEFT JOIN {{ source('french_towns', 'market_holidays') }} m
    ON cp.date = m.date
LEFT JOIN {{ source('french_towns', 'political_context') }} p
    ON cp.date BETWEEN p.start_date AND p.end_date
ORDER BY cp.date_id
