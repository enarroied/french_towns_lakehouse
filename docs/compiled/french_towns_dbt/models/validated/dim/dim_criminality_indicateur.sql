

SELECT
    indicateur_id::INTEGER          AS indicateur_id,
    name_fr::VARCHAR(100)           AS name_fr,
    name_en::VARCHAR(100)           AS name_en,
    unite_de_compte_fr::VARCHAR(50) AS unite_de_compte_fr,
    unite_de_compte_en::VARCHAR(50) AS unite_de_compte_en
FROM read_csv_auto('s3://staging-current/dim_criminality_indicateur/dim_criminality_indicateur_*.csv')
ORDER BY indicateur_id