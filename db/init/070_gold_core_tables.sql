CREATE TABLE IF NOT EXISTS gold.dim_customers (
    global_customer_id UUID PRIMARY KEY,
    canonical_email TEXT,
    canonical_phone TEXT,
    canonical_name TEXT,
    source_priority JSONB,
    record_count INT,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
