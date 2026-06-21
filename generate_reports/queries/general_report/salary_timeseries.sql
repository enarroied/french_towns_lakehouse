SELECT year, mean_salary
FROM silver.fact_salaries
WHERE id = ?
ORDER BY year
