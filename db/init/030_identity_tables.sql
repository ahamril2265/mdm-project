CREATE TABLE IF NOT EXISTS identity.ingestion_watermarks (
    source_system TEXT PRIMARY KEY,
    last_ingested_ts TIMESTAMP NOT NULL
);

INSERT INTO identity.ingestion_watermarks (source_system, last_ingested_ts)
VALUES ('sales','1970-01-01'),('support','1970-01-01'),('marketing','1970-01-01')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS identity.customer_identity_map (
    global_customer_id UUID NOT NULL,
    source_system TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    confidence_score INT,
    decision TEXT,
    decided_at TIMESTAMP,
    PRIMARY KEY (source_system, source_record_id)
);
