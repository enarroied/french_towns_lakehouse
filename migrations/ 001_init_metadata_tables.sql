CREATE TABLE IF NOT EXISTS flow_run_metadata (
    run_id UUID PRIMARY KEY,
    domain_name TEXT NOT NULL,
    layer TEXT NOT NULL, -- staging, transformation, integration
    status TEXT NOT NULL, -- STARTED, SUCCESS, FAILED
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    technical_type TEXT, -- scraper, download, api
    metrics_json JSON
);
