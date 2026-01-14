CREATE TABLE IF NOT EXISTS gold.match_confidence_metrics (
    metric_date DATE PRIMARY KEY,

    total_links INT NOT NULL,
    auto_merges INT NOT NULL,
    flagged_reviews INT NOT NULL,
    rejected_links INT NOT NULL,

    avg_confidence_score FLOAT,
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold.customer_attribute_conflicts (
    global_customer_id UUID,
    attribute_name TEXT,
    distinct_value_count INT,
    detected_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (global_customer_id, attribute_name)
);

CREATE OR REPLACE VIEW gold.steward_review_queue AS
SELECT
    c.global_customer_id,
    c.record_count,
    COUNT(DISTINCT conf.attribute_name) AS conflict_count,
    COUNT(DISTINCT o.override_id) AS active_overrides
FROM gold.dim_customers c
LEFT JOIN gold.customer_attribute_conflicts conf
  ON c.global_customer_id = conf.global_customer_id
LEFT JOIN gold.customer_steward_overrides o
  ON c.global_customer_id = o.global_customer_id
 AND o.is_active = true
GROUP BY c.global_customer_id, c.record_count
HAVING
    COUNT(DISTINCT conf.attribute_name) > 0
 OR COUNT(DISTINCT o.override_id) > 0
 OR c.record_count > 5;
