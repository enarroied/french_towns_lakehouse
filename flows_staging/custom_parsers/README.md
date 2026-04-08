# Custom Parsers

This directory contains parsers for data that cannot be fetched programmatically and require manual retrieval.

## ville_sportive

Parses the "Villes Actives & Sportives" palmares PDF.

### Source

PDF must be manually downloaded from:
https://www.ville-active-et-sportive.com/villes#ville

### Input

Place the PDF file in `custom_parsers/data_for_parsers/`.

Configure which PDF to use in `config.yaml`:

```yaml
custom_parsers:
  - name: "ville_sportive"
    module: "custom_parsers.parse_ville_sportive"
    enabled: true
    input_dir: "custom_parsers/data_for_parsers"
    pdf_file: "2025-palmares.pdf"
```

### Output

CSV file saved to `data/custom/ville_sportive.csv` with columns:
- `commune`: city name (lowercase)
- `dept_code`: department code (2-digit, e.g., "09", "971")
- `nb_lauriers`: number of "lauriers" (1-4)

### How to Update

1. Download the latest PDF from the website
2. Save it to `custom_parsers/data_for_parsers/`
3. Update `pdf_file` in `config.yaml` to match the new filename
4. Run the pipeline - the parser will automatically process the new file
