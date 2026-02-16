COPY (
    SELECT
        GEO AS id,
        TIME_PERIOD::INTEGER AS year,
        OBS_VALUE::INTEGER AS population
    FROM read_csv_auto(
        '{{input_file}}',
        types={'GEO': 'CHAR(5)'}  -- Force GEO to be a CHAR(5) for Corsica
    )
    WHERE GEO_OBJECT = 'COM'
) TO '{{output_file}}';
