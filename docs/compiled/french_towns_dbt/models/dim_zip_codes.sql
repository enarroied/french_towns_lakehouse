

SELECT
    Code_postal::INTEGER AS id,
    Code_postal::CHAR(5) AS zip_code_char,
    SUBSTR(Code_postal, 1, 2) AS zip_code_main_department,
    COUNT(DISTINCT "#Code_commune_INSEE") AS number_of_communes,
    COUNT(DISTINCT SUBSTR("#Code_commune_INSEE", 1, 2)) AS number_of_departments
FROM read_csv_auto('../input/019HexaSmal.csv', sample_size=-1, encoding='latin-1', delim=';')
GROUP BY Code_postal