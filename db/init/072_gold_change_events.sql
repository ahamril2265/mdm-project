CREATE TABLE IF NOT EXISTS gold.customer_change_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    global_customer_id UUID NOT NULL,
    change_type TEXT NOT NULL,
    old_record JSONB,
    new_record JSONB,
    changed_at TIMESTAMP NOT NULL,
    source TEXT DEFAULT 'mdm'
);
