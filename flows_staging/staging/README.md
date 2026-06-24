# Seed-Driven Staging Flows

Some INSEE datasets are distributed as many small per-year ZIP files (e.g.
the *base-cc-emploi-pop-active* series with 34 files for 17 years x metro/COM).
Rather than bloating `config.yaml` with 34 individual download entries, we use a
**seed CSV** as the single source of truth for URLs.

## How it works

1. **Seed CSV** (`seeds/seed_*.csv`) — a checked-in CSV with columns
   `year,scope,url,format,status`. The `status` column allows disabling
   individual URLs without deleting them.
2. **Custom staging flow** — reads the seed, loops over active rows, downloads
   each ZIP, extracts columns matching the year-specific prefix
   (e.g. `P20_` for 2020), strips the prefix, injects the `year` column, and
   unions all rows into a single output CSV.
3. **Shared upload** — the final CSV is pushed through `_process_single_file()`
   (same MD5-check/archive/upload/metadata logic as regular downloads).

## When to use this pattern

- You have 5+ URLs for the same theme that produce compatible schemas.
- The URLs come from a predictable pattern (e.g. different INSEE year files).
- You want a single CSV in staging that a single dbt model reads.

## Adding a new seed-driven source

1. Create the seed CSV under `seeds/`.
2. Register the source in `data_sources/dim_source/sources.csv`.
3. Add the model to `config.yaml` `source_links:`.
4. Write the custom staging flow (see `staging_unemployment.py` for reference).
5. Register the external source in `french_towns_dbt/models/sources.yml`.
6. Write the dbt model and schema YAML.
7. Add transformation and integration flows.
8. Wire into the pipeline files.
