-- migrations/002_add_file_metadata.sql
CREATE TABLE IF NOT EXISTS file_metadata (
    file_id UUID PRIMARY KEY,
    run_id UUID NOT NULL,
    filename TEXT  NOT NULL,
    filename_timestamp TEXT  NOT NULL,
    file_location TEXT NOT NULL,
    source_url TEXT,
    size_mb DOUBLE NOT NULL,
    md5_hash TEXT NOT NULL,
    bucket TEXT,
    upload_timestamp TIMESTAMP,
    is_latest SMALLINT DEFAULT 1,
    FOREIGN KEY (run_id) REFERENCES flow_run_metadata(run_id)
);
