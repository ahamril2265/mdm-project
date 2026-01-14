CREATE TABLE IF NOT EXISTS staging.stg_sales_customers (
    order_id TEXT,
    normalized_email TEXT,
    normalized_phone TEXT,
    normalized_name TEXT,
    order_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_support_contacts (
    ticket_id TEXT,
    normalized_email TEXT,
    normalized_phone TEXT,
    ticket_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_marketing_leads (
    lead_id TEXT,
    normalized_email TEXT,
    normalized_phone TEXT,
    normalized_name TEXT,
    lead_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.identity_inputs (
    source_system TEXT,
    source_record_id TEXT,
    normalized_email TEXT,
    normalized_phone TEXT,
    normalized_name TEXT,
    event_ts TIMESTAMP
);
