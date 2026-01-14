-- Phase 10D — Steward Review Queue (SAFE)

DROP VIEW IF EXISTS gold.steward_review_queue;

CREATE VIEW gold.steward_review_queue AS
SELECT
    c.global_customer_id,
    c.record_count,
    COUNT(DISTINCT conf.attribute_name) AS conflict_count,
    COUNT(DISTINCT o.override_id) AS override_count
FROM gold.dim_customers c
LEFT JOIN gold.customer_attribute_conflicts conf
  ON c.global_customer_id = conf.global_customer_id
LEFT JOIN gold.customer_steward_overrides o
  ON c.global_customer_id = o.global_customer_id
GROUP BY c.global_customer_id, c.record_count
HAVING
    COUNT(DISTINCT conf.attribute_name) > 0
 OR COUNT(DISTINCT o.override_id) > 0
 OR c.record_count > 5;
