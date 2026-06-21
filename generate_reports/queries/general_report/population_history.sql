SELECT year, population, year_evolution, year_evolution_percent
FROM silver.fact_population
WHERE id = ?
ORDER BY year DESC
