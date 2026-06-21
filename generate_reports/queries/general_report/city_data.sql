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
ranked_sal AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY year DESC) AS rn
    FROM silver.fact_salaries
    WHERE mean_salary IS NOT NULL
),
latest_sal AS (
    SELECT id, year, mean_salary
    FROM ranked_sal
    WHERE rn = 1
),
dept_avg AS (
    SELECT
        AVG(lp.population) AS dept_avg_population,
        AVG(ls.mean_salary) AS dept_avg_salary
    FROM silver.dim_communes_france c
    LEFT JOIN latest_pop lp ON c.id = lp.id
    LEFT JOIN latest_sal ls ON c.id = ls.id
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
    ls.mean_salary,
    ls.year AS latest_salary_year,
    da.dept_avg_salary,
    ls.mean_salary / NULLIF(da.dept_avg_salary, 0) AS salary_ratio
FROM silver.dim_communes_france c
LEFT JOIN latest_pop lp ON c.id = lp.id
LEFT JOIN latest_sal ls ON c.id = ls.id
CROSS JOIN dept_avg da
WHERE c.department_code = ?
ORDER BY lp.population DESC NULLS LAST
