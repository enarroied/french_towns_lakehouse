-- migrations/002_add_file_metadata.sql
CREATE TABLE IF NOT EXISTS file_metadata (
    file_id UUID PRIMARY KEY,
    run_id UUID,
    filename TEXT,
    source_url TEXT,
    size_mb DOUBLE,
    md5_hash TEXT,
    bucket TEXT,
    upload_timestamp TIMESTAMP,
    is_latest INTEGER DEFAULT 1,
    FOREIGN KEY (run_id) REFERENCES flow_run_metadata(run_id)
);
