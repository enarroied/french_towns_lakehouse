# Overseas population — reference data

This CSV provides population counts for 105 overseas French communes that are not covered by the main INSEE `populations_historiques` source (which only covers departments except Mayotte).

## Source URLs

| Territory | Codes | Census years | Source |
|-----------|-------|--------------|--------|
| St‑Pierre‑et‑Miquelon | 97501, 97502 | 1968, 1975, 1982, 1990, 1999, 2007, 2012, 2017, 2023 | `base_cc_serie_historique_com_2023_csv.zip` from https://www.insee.fr/fr/statistiques/fichier/9003854/base_cc_serie_historique_com_2023_csv.zip |
| St‑Barthélemy | 97701 | same 9 years | same INSEE historical CSV |
| St‑Martin | 97801 | same 9 years | same INSEE historical CSV |
| Wallis‑et‑Futuna | 98611 (Alo), 98612 (Sigave), 98613 (Uvea) | 2008, 2013, 2018, 2023 | XLSX (2023, 2018) and HTML tables (2013, 2008) from https://www.insee.fr/fr/statistiques/7756942 and related pages |
| French Polynesia | 98711–98758 (48 communes) | 2007, 2012, 2017, 2022 | XLSX (2022, 2017) and HTML tables (2012, 2007) from https://www.insee.fr/fr/statistiques/6690039 and related pages |
| New Caledonia | 98801–98833 (33 communes) | 2009, 2014 | User‑provided table (manual transcription) |
| Mayotte | 97601–97617 (17 communes) | 2007, 2012, 2017, 2026 | INSEE HTML tables from https://www.insee.fr/fr/statistiques/7733542 and related pages |

## How it was built

1. The user located each INSEE page and shared the URL
2. opencode extracted the data — from XLSX downloads (2023, 2018, 2022, 2017), HTML tables (2013, 2008, 2012, 2007), and the historical CSV archive (975/977/978)
3. All rows were assembled into `overseas_population.csv` with columns `commune_id`, `year`, `population`
4. The CSV is consumed downstream by the `staging_overseas_population` Prefect flow, `fact_population.sql` (dbt), and the Polaris Iceberg integration

## Notes

- French Polynesia commune codes follow the pattern `987NN` where NN is the row number from the XLSX (e.g., row 11 "Anaa" → 98711).
- Wallis‑et‑Futuna data was aggregated from village‑level figures to circonscription level (Alo, Sigave, Uvea).
- Mayotte (976) was initially thought to be covered by the main INSEE `populations_historiques` source, but it returns NULL populations at commune level. Mayotte was therefore extracted from INSEE HTML pages and added to this CSV (17 communes, 4 census years).
- TAAF territories (984, 981, 989) are excluded entirely (uninhabited research stations).
