CREATE TABLE IF NOT EXISTS raw.sales_orders (
    order_id TEXT PRIMARY KEY,
    email TEXT,
    name TEXT,
    zip_code TEXT,
    order_amount NUMERIC(10,2),
    order_ts TIMESTAMP,
    ingestion_ts TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.support_tickets (
    ticket_id TEXT PRIMARY KEY,
    contact_email TEXT,
    issue_type TEXT,
    ticket_ts TIMESTAMP,
    ingestion_ts TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.marketing_leads (
    lead_id TEXT PRIMARY KEY,
    full_name TEXT,
    phone TEXT,
    lead_ts TIMESTAMP,
    ingestion_ts TIMESTAMP DEFAULT now()
);
