-- Phase 10A — Match Confidence Metrics (IDEMPOTENT)

CREATE TABLE IF NOT EXISTS gold.match_confidence_metrics (
    metric_date DATE PRIMARY KEY,

    total_pairs INT NOT NULL,
    auto_merge_count INT NOT NULL,
    flag_review_count INT NOT NULL,
    reject_count INT NOT NULL,

    avg_confidence_score FLOAT,
    updated_at TIMESTAMP DEFAULT now()
);
