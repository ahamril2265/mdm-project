CREATE TABLE IF NOT EXISTS gold.customer_change_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    global_customer_id UUID NOT NULL,
    change_type TEXT NOT NULL,            -- INSERT | UPDATE | DELETE

    old_record JSONB,
    new_record JSONB,

    changed_at TIMESTAMP NOT NULL,
    source TEXT DEFAULT 'mdm_phase_9a'
);

CREATE INDEX IF NOT EXISTS idx_change_events_customer
ON gold.customer_change_events (global_customer_id);

CREATE INDEX IF NOT EXISTS idx_change_events_time
ON gold.customer_change_events (changed_at);
