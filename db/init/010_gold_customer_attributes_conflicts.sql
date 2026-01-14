CREATE TABLE IF NOT EXISTS gold.customer_attribute_conflicts (
    global_customer_id UUID,
    attribute_name TEXT,
    distinct_value_count INT,
    detected_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (global_customer_id, attribute_name)
);
