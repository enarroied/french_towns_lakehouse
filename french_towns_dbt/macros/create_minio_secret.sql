{% macro create_minio_secret() %}
    CREATE SECRET IF NOT EXISTS minio_secret (
        TYPE s3,
        PROVIDER config,
        KEY_ID '{{ env_var('AWS_ACCESS_KEY_ID', 'eric') }}',
        SECRET '{{ env_var('AWS_SECRET_ACCESS_KEY', 'eric1234') }}',
        ENDPOINT '{{ env_var('AWS_ENDPOINT', 'localhost:19000') }}',
        REGION 'us-east-1',
        USE_SSL false,
        URL_STYLE 'path'
    );
{% endmacro %}
