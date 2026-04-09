# Custom Parsers

This directory contains parsers for data that cannot be fetched programmatically and require manual retrieval.

## ville_sportive

Parses the "Villes Actives & Sportives" palmares PDF.

### Source

PDF must be manually downloaded from:
https://www.ville-active-et-sportive.com/villes#ville

### Input

Source files are stored in `data_sources/` at the project root.

Configure which PDF to use in `config.yaml`:

```yaml
custom_parsers:
  - name: "ville_sportive"
    module: "flows_staging.custom_parsers.parse_ville_sportive"
    enabled: true
    input_dir: "data_sources/ville_active_sportive"
    pdf_file: "2025-palmares.pdf"
```

### Output

CSV file saved to MinIO staging bucket with columns:
- `commune`: city name (lowercase)
- `dept_code`: department code (2-digit, e.g., "09", "971")
- `nb_lauriers`: number of "lauriers" (1-4)

### How to Update

1. Download the latest PDF from the website
2. Save it to `data_sources/ville_active_sportive/`
3. Update `pdf_file` in `config.yaml` to match the new filename
4. Run the pipeline - the parser will automatically process the new file
