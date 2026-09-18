WITH ranked_pop AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY year DESC) AS rn
    FROM silver.fact_population
    WHERE population IS NOT NULL
),
latest_pop AS (
    SELECT id, year, population, year_evolution_percent AS population_growth_pct
    FROM ranked_pop
    WHERE rn = 1
),
ranked_inc AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY year DESC) AS rn
    FROM silver.fact_income
    WHERE median_income IS NOT NULL
),
latest_inc AS (
    SELECT id, year, median_income
    FROM ranked_inc
    WHERE rn = 1
),
dept_avg AS (
    SELECT
        AVG(lp.population) AS dept_avg_population,
        AVG(li.median_income) AS dept_avg_income
    FROM silver.dim_communes c
    LEFT JOIN latest_pop lp ON c.id = lp.id
    LEFT JOIN latest_inc li ON c.id = li.id
    WHERE c.department_code = ?
)
SELECT
    c.id,
    c.name,
    c.department_code,
    c.department_name,
    lp.population,
    lp.year AS latest_population_year,
    lp.population_growth_pct,
    da.dept_avg_population,
    lp.population / NULLIF(da.dept_avg_population, 0) AS population_ratio,
    li.median_income,
    li.year AS latest_income_year,
    da.dept_avg_income,
    li.median_income / NULLIF(da.dept_avg_income, 0) AS income_ratio
FROM silver.dim_communes c
LEFT JOIN latest_pop lp ON c.id = lp.id
LEFT JOIN latest_inc li ON c.id = li.id
CROSS JOIN dept_avg da
WHERE c.department_code = ?
ORDER BY lp.population DESC NULLS LAST
