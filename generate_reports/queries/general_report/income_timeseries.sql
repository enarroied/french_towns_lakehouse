SELECT year, median_income
FROM silver.fact_income
WHERE id = ?
ORDER BY year
