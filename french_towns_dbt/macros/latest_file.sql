{% macro latest_file(pattern) %}
    {% set query %}
        SELECT max(file) AS file
        FROM glob('{{ pattern }}')
    {% endset %}

    {% set result = run_query(query) %}
    {% if execute %}
        {% set file = result.columns[0].values()[0] %}
        {% if file is none %}
            {% do exceptions.raise_compiler_error("No matching GeoJSON files found: " ~ pattern) %}
        {% endif %}
        '{{ file }}'
    {% else %}
        NULL
    {% endif %}
{% endmacro %}
