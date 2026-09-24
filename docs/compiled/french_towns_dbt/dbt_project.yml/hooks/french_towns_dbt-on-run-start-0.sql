
    CREATE SECRET IF NOT EXISTS minio_secret (
        TYPE s3,
        PROVIDER config,
        KEY_ID 'eric',
        SECRET 'eric1234',
        ENDPOINT 'localhost:19000',
        REGION 'us-east-1',
        USE_SSL false,
        URL_STYLE 'path'
    );
