# Filosofi income — reference data

This directory documents the Filosofi income data series (Revenu disponible) for French communes, covering 2013–2023. The actual data is not stored here — it is downloaded and staged to MinIO bronze by `scripts/download_filosofi_income.py`.

## Source URLs

Data comes from the INSEE Filosofi (Fichier localisé social et fiscal) series at the commune level.

### Filosofi 1 — 2013–2021

| Year | Format | Income file | Poverty file | Source |
|------|--------|-------------|--------------|--------|
| 2013 | XLS | `FILO_DISP_COM.xls` | `FILO_DISP_Pauvres_COM.xls` | https://www.insee.fr/fr/statistiques/fichier/2388413/indic-struct-distrib-revenu-2013-COMMUNES.zip |
| 2014 | XLS | `base-cc-filosofi-2014.xls` (merged — income + poverty + breakdown in one file) | same | https://www.insee.fr/fr/statistiques/fichier/3126432/filo-revenu-pauvrete-menage-2014.zip |
| 2015 | XLS | `FILO_DISP_COM.xls` | `FILO_DISP_Pauvres_COM.xls` | https://www.insee.fr/fr/statistiques/fichier/3560118/indic-struct-distrib-revenu-2015-COMMUNES.zip |
| 2016 | XLS | `FILO2016_DISP_COM.xls` | `FILO2016_DISP_Pauvres_COM.xls` | https://www.insee.fr/fr/statistiques/fichier/4190006/indic-struct-distrib-revenu-2016-COMMUNES.zip |
| 2017 | CSV | `cc_filosofi_2017_COM.CSV` (merged — income + poverty + breakdown) | same | https://www.insee.fr/fr/statistiques/fichier/4507225/base-filosofi-2017_CSV.zip |
| 2018 | CSV | `FILO2018_DISP_COM.csv` | `FILO2018_DISP_Pauvres_COM.csv` | https://www.insee.fr/fr/statistiques/fichier/5009218/indic-struct-distrib-revenu-2018-COMMUNES_csv.zip |
| 2019 | CSV | `FILO2019_DISP_COM.csv` | `FILO2019_DISP_Pauvres_COM.csv` | https://www.insee.fr/fr/statistiques/fichier/6036907/indic-struct-distrib-revenu-2019-COMMUNES_csv.zip |
| 2020 | CSV | `FILO2020_DISP_COM.csv` | `FILO2020_DISP_PAUVRES_COM.csv` | https://www.insee.fr/fr/statistiques/fichier/6692220/indic-struct-distrib-revenu-2020-COMMUNES_csv.zip |
| 2021 | CSV | `FILO2021_DISP_COM.csv` | `FILO2021_DISP_PAUVRES_COM.csv` | https://www.insee.fr/fr/statistiques/fichier/7756855/indic-struct-distrib-revenu-2021-COMMUNES_csv.zip |

### Filosofi 2 — 2023

| Year | Format | File | Source |
|------|--------|------|--------|
| 2023 | CSV (long) | `DS_FILOSOFI_CC_2023_data.csv` | https://www.insee.fr/fr/statistiques/fichier/8984752/FILOSOFI_CC_csv.zip |

**Note:** 2022 is explicitly missing — INSEE did not publish Filosofi data for 2022 due to the taxe d'habitation abolition methodology change.

## Column mapping

The harmonised CSV has 21 columns. The mapping varies by year because of source format differences:

### Filosofi 1 — 2013–2021

| Harmonised column | 2013 | 2014 | 2015 | 2016 | 2017 | 2018–2019 | 2020–2021 |
|---|---|---|---|---|---|---|---|
| `id` | CODGEO | CODGEO | CODGEO | CODGEO | CODGEO | CODGEO | CODGEO |
| `nb_tax_households` | NBMEN13 | NBMENFISC14 | NBMEN15 | NBMEN16 | NBMENFISC17 | NBMEN{YY} | NBMEN{YY} |
| `nb_persons` | NBPERS13 | NBPERSMENFISC14 | NBPERS15 | NBPERS16 | NBPERSMENFISC17 | NBPERS{YY} | NBPERS{YY} |
| `median_income` | Q213 | MED14 | Q215 | Q216 | MED17 | Q2{YY} | Q2{YY} |
| `poverty_rate` | TP6013 | TP6014 | TP6015 | TP6016 | TP6017 | TP60{YY} | TP60{YY} |
| `decile1` | D113 | D114 | D115 | D116 | D117 | D1{YY} | D1{YY} |
| `decile9` | D913 | D914 | D915 | D916 | D917 | D9{YY} | D9{YY} |
| `gini` | GI13 | — | GI15 | GI16 | — | GI{YY} | GI{YY} |
| `s80_s20` | S80S2013 | — | S80S2015 | S80S2016 | — | S80S20{YY} | S80S20{YY} |
| `activity_income_share` | PTSAC13¹ | PRA14 | PACT15 | PACT16 | PACT17 | PACT{YY} | PACT{YY} |
| `salary_share` | PTSAC13¹ | PTSAC14¹ | PTSA15 | PTSA16 | PTSA17 | PTSA{YY} | NULL³ |
| `unemployment_share` | NULL¹ | NULL¹ | PCHO15 | PCHO16 | PCHO17 | PCHO{YY} | NULL³ |
| `pension_share` | PPEN13 | PPEN14 | PPEN15 | PPEN16 | PPEN17 | PPEN{YY} | NULL³ |
| `property_income_share` | PPAT13 | PPAT14 | PPAT15 | PPAT16 | PPAT17 | PPAT{YY} | NULL³ |
| `social_benefits_share` | PPSOC13 | PPSOC14 | PPSOC15 | PPSOC16 | PPSOC17 | PPSOC{YY} | NULL³ |
| `family_benefits_share` | PPFAM13 | PPFAM14 | PPFAM15 | PPFAM16 | PPFAM17 | PPFAM{YY} | NULL³ |
| `minimum_social_share` | PPMINI13 | PPMINI14 | PPMINI15 | PPMINI16 | PPMINI17 | PPMINI{YY} | NULL³ |
| `housing_benefits_share` | PPLOGT13 | PPLOGT14 | PPLOGT15 | PPLOGT16 | PPLOGT17 | PPLOGT{YY} | NULL³ |
| `tax_share` | PIMPOT13 | PIMPOT14 | PIMPOT15 | PIMPOT16 | PIMPOT17 | PIMPOT{YY} | NULL³ |

¹ 2013: PTSAC13 = salaries + unemployment combined (no separate PACT/PTSA/PCHO). Both `activity_income_share` and `salary_share` are populated from this one column; `unemployment_share` is NULL.
² 2014: PRA14 = total activity income; PTSAC14 = salaries + unemployment combined (same limitation as 2013).
³ 2020–2021: only PACT is published at commune level; all other composition columns are NULL.
— : Column not available in the source file for that year.

### Filosofi 2 — 2023 (long-format pivot)

| Harmonised column | Filosofi 2 measure |
|---|---|
| `median_income` | MED_SL |
| `poverty_rate` | PR_MD60 |
| All other measures | NULL (not available at commune level in Filosofi 2) |

## How it was built

1. `scripts/download_filosofi_income.py` downloads all ZIP archives, extracts the relevant files
2. XLS files (2013–2016) are parsed via `xlrd` (code row = row 5, data from row 6)
3. CSV files (2017–2021) are parsed as semicolon-delimited CSV (utf-8-sig encoding)
4. The 2023 long-format CSV is pivoted from key-value to wide format
5. All 10 years are deduplicated, written to `/tmp/filosofi_income_harmonized.csv`, and uploaded to MinIO staging (`s3://staging-current/income/`)

## Notes

- Temporally inconsistent data: INSEE changes the file format almost every year (XLS vs CSV, merged vs separate files, long vs wide).
- The XLS header structure is unusual: row 5 contains the short codes (CODGEO, NBMEN13, etc.) and row 4 the French labels. The script reads row 5.
- 2014 is a single XLS with both income and poverty data on the "COM" sheet (no separate DISP_Pauvres file).
- 2017 is the only year with a "base-filosofi" format CSV that uses different column naming (MED17 instead of Q217, NBMENFISC17 instead of NBMEN17, etc.).
- The detailed income composition breakdown (PTSA, PCHO, PPEN, PPAT, PPSOC, PPFAM, PPMINI, PPLOGT, PIMPOT) is only available for 2013–2019. From 2020 onwards, only the aggregate PACT (activity income share) is published.
- Filosofi 2 (2023) uses a different methodology and is explicitly non-comparable with Filosofi 1. Only MED_SL (median income) and PR_MD60 (poverty rate) are available at commune level.
- 2022 was skipped by INSEE (methodology change due to taxe d'habitation abolition).
