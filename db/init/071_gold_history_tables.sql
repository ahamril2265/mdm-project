CREATE TABLE IF NOT EXISTS gold.dim_customers_history (
    surrogate_key BIGSERIAL PRIMARY KEY,
    global_customer_id UUID NOT NULL,
    canonical_email TEXT,
    canonical_phone TEXT,
    canonical_name TEXT,
    source_priority JSONB,
    record_count INT,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dim_hist_gcid
ON gold.dim_customers_history (global_customer_id);

CREATE INDEX IF NOT EXISTS idx_dim_hist_current
ON gold.dim_customers_history (global_customer_id)
WHERE is_current = true;
