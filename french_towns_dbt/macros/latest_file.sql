{% macro latest_file(pattern) %}
    {# In CI (docs only), skip the S3 glob lookup and return the pattern as-is #}
    {% if env_var('CI', 'false') == 'true' %}
        '{{ pattern }}'
    {% else %}
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
    {% endif %}
{% endmacro %}
