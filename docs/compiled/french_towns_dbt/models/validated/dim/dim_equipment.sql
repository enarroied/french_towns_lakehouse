

SELECT
    ROW_NUMBER() OVER (ORDER BY equipment_code) AS equipment_type_id,
    equipment_code,
    equipment_name,
    subdomain_code,
    subdomain_name,
    domain_code,
    domain_name
FROM read_csv_auto('s3://staging-current/dim_equipment/dim_equipment_*.csv')
ORDER BY equipment_type_id