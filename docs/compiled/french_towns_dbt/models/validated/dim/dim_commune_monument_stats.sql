

WITH monument_domains AS (
    SELECT
        m.reference,
        m.domain,
        m.protection_level,
        m.protection_scope
    FROM "french_towns"."main"."dim_monument" m
),

commune_monuments AS (
    SELECT
        b.commune_code,
        b.monument_reference,
        d.domain,
        d.protection_level,
        d.protection_scope
    FROM "french_towns"."main"."bridge_monument_communes" b
    JOIN monument_domains d
        ON b.monument_reference = d.reference
)

SELECT
    c.id AS commune_id,
    COALESCE(COUNT(DISTINCT cm.monument_reference), 0) AS total_monuments,

    COUNT(DISTINCT CASE WHEN cm.domain = 'architecture religieuse'
        THEN cm.monument_reference END) AS religious_count,
    COUNT(DISTINCT CASE WHEN cm.domain = 'architecture domestique'
        THEN cm.monument_reference END) AS domestic_count,
    COUNT(DISTINCT CASE WHEN cm.domain = 'architecture administrative'
        THEN cm.monument_reference END) AS administrative_count,
    COUNT(DISTINCT CASE WHEN cm.domain = 'architecture militaire'
        THEN cm.monument_reference END) AS military_count,
    COUNT(DISTINCT CASE WHEN cm.domain = 'architecture funéraire et commémorative'
        OR cm.domain = 'architecture funéraire'
        THEN cm.monument_reference END) AS funerary_count,
    COUNT(DISTINCT CASE WHEN cm.domain = 'génie civil'
        THEN cm.monument_reference END) AS civil_engineering_count,
    COUNT(DISTINCT CASE WHEN cm.domain = 'industrie et artisanat'
        OR cm.domain = 'industrie'
        THEN cm.monument_reference END) AS industrial_count,
    COUNT(DISTINCT CASE WHEN cm.domain = 'architecture agricole'
        THEN cm.monument_reference END) AS agricultural_count,
    COUNT(DISTINCT CASE WHEN cm.domain = 'architecture scolaire et éducative'
        THEN cm.monument_reference END) AS educational_count,
    COUNT(DISTINCT CASE WHEN cm.domain NOT IN (
        'architecture religieuse', 'architecture domestique',
        'architecture administrative', 'architecture militaire',
        'architecture funéraire et commémorative', 'architecture funéraire',
        'génie civil', 'industrie et artisanat', 'industrie',
        'architecture agricole', 'architecture scolaire et éducative'
    ) OR cm.domain IS NULL THEN cm.monument_reference END) AS other_count,

    COUNT(DISTINCT CASE WHEN cm.protection_level = 'classé'
        THEN cm.monument_reference END) AS classified_count,
    COUNT(DISTINCT CASE WHEN cm.protection_level = 'inscrit'
        THEN cm.monument_reference END) AS inscribed_count,
    COUNT(DISTINCT CASE WHEN cm.protection_scope = 'partiellement'
        THEN cm.monument_reference END) AS partial_protection_count

FROM "french_towns"."main"."dim_communes" c
LEFT JOIN commune_monuments cm
    ON c.id = cm.commune_code
GROUP BY c.id