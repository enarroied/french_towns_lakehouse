{% macro bridge_source_links_simple(model_name, target_key_expr, source_names) %}

SELECT
    ROW_NUMBER() OVER () AS source_link_id,
    '{{ model_name }}' AS target_table,
    {{ target_key_expr }} AS target_key,
    s.source_id
FROM {{ ref(model_name) }} t
CROSS JOIN (
    SELECT source_id FROM {{ ref('dim_source') }}
    WHERE source_name IN ('{{ source_names | join("', '") }}')
) s

{% endmacro %}


{% macro bridge_source_links_mapped(model_name, target_key_expr, join_column, mapping) %}
{# mapping: dict of column_value → source_name #}

SELECT
    ROW_NUMBER() OVER () AS source_link_id,
    '{{ model_name }}' AS target_table,
    {{ target_key_expr }} AS target_key,
    m.source_id
FROM {{ ref(model_name) }} t
JOIN (
    {% for column_value, source_name in mapping.items() %}
    SELECT '{{ column_value }}' AS match_value, source_id FROM {{ ref('dim_source') }} WHERE source_name = '{{ source_name }}'
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
) m ON t.{{ join_column }} = m.match_value

{% endmacro %}
