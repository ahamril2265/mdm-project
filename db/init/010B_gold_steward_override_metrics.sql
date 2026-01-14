-- Phase 10B — Steward Override Metrics (DDL)

CREATE TABLE IF NOT EXISTS gold.steward_override_metrics (
    metric_date DATE PRIMARY KEY,
    total_overrides INT NOT NULL,
    active_overrides INT NOT NULL,
    expired_overrides INT NOT NULL
);
