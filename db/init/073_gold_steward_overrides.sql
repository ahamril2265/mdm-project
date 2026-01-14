CREATE TABLE IF NOT EXISTS gold.customer_steward_overrides (
    override_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    global_customer_id UUID NOT NULL,
    attribute_name TEXT NOT NULL,
    override_value TEXT NOT NULL,
    reason TEXT,
    steward_id TEXT,
    valid_from TIMESTAMP DEFAULT now(),
    valid_to TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT now()
);
