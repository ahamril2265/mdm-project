#!/usr/bin/env bash
set -euo pipefail

echo "======================================"
echo "MDM PIPELINE : PHASE 0 → PHASE 5"
echo "======================================"

# -----------------------
# CONFIG
# -----------------------
POSTGRES_CONTAINER="mdm_postgres"
DB_NAME="mdm_db"
DB_USER="mdm_user"

# -----------------------
# HELPERS
# -----------------------
psql_exec () {
  docker exec -i "$POSTGRES_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1
}

echo_step () {
  echo
  echo "▶ $1"
  echo "--------------------------------------"
}

# -----------------------
# PHASE 0 — INFRA & SCHEMA
# -----------------------
#echo_step "Phase 0 — Infrastructure & Base Schema"

#docker compose down -v
#docker compose up -d

#echo "Waiting for Postgres..."
#sleep 5

echo "Applying database initialization scripts..."

for file in db/init/*.sql; do
  echo "→ Applying $file"
  psql_exec < "$file"
done

#echo "Phase 0 completed ✅"

# -----------------------
# PHASE 1 — PRODUCERS
# -----------------------
echo_step "Phase 1 — Running Producers"

python producers/sales_producer.py
python producers/support_producer.py
python producers/marketing_producer.py

echo "Phase 1 completed ✅"

# -----------------------
# PHASE 2 — INGESTION
# -----------------------
echo_step "Phase 2 — MinIO → Raw Ingestion"

python ingestion/minio_to_raw.py

psql_exec <<'SQL'
SELECT COUNT(*) FROM raw.sales_orders;
SELECT COUNT(*) FROM raw.support_tickets;
SELECT COUNT(*) FROM raw.marketing_leads;
SQL

echo "Phase 2 completed ✅"

# -----------------------
# PHASE 3 — STAGING
# -----------------------
echo_step "Phase 3 — Staging Normalization"

python staging/run_staging.py

psql_exec <<'SQL'
SELECT COUNT(*) FROM staging.stg_sales_customers;
SELECT COUNT(*) FROM staging.stg_support_contacts;
SELECT COUNT(*) FROM staging.stg_marketing_leads;
SQL

echo "Phase 3 completed ✅"

# -----------------------
# PHASE 4 — IDENTITY INPUTS
# -----------------------
echo_step "Phase 4 — Identity Inputs"

python staging/run_identity_inputs.py

psql_exec <<'SQL'
SELECT COUNT(*) FROM staging.identity_inputs;
SQL

echo "Phase 4 completed ✅"

# -----------------------
# PHASE 4.5 — BLOCKING
# -----------------------
echo_step "Phase 4.5 — Blocking Candidates"

psql_exec <<'SQL'
DROP TABLE IF EXISTS staging.identity_match_candidates_blocked;

CREATE TABLE staging.identity_match_candidates_blocked AS
SELECT
    a.source_system        AS left_source_system,
    a.source_record_id     AS left_record_id,
    a.normalized_email     AS left_email,
    a.normalized_phone     AS left_phone,
    a.normalized_name      AS left_name,

    b.source_system        AS right_source_system,
    b.source_record_id     AS right_record_id,
    b.normalized_email     AS right_email,
    b.normalized_phone     AS right_phone,
    b.normalized_name      AS right_name
FROM staging.identity_inputs a
JOIN staging.identity_inputs b
  ON a.source_system <> b.source_system
 AND (
        (a.normalized_email IS NOT NULL AND a.normalized_email = b.normalized_email)
     OR (a.normalized_phone IS NOT NULL AND a.normalized_phone = b.normalized_phone)
    )
 -- prevent mirrored duplicates
 AND a.source_record_id < b.source_record_id;

CREATE INDEX idx_blocked_left
  ON staging.identity_match_candidates_blocked (left_source_system, left_record_id);

CREATE INDEX idx_blocked_right
  ON staging.identity_match_candidates_blocked (right_source_system, right_record_id);
SQL

psql_exec <<'SQL'
SELECT COUNT(*) FROM staging.identity_match_candidates_blocked;
SQL

echo "Phase 4.5 completed ✅"

# -----------------------
# PHASE 5 — MATCHING
# -----------------------
echo_step "Phase 5 — Identity Matching Engine"

python matching/run_matching_engine.py

psql_exec <<'SQL'
SELECT match_decision, COUNT(*)
FROM staging.identity_match_candidates
GROUP BY match_decision;
SQL

echo "Phase 5 completed ✅"

echo
echo "======================================"
echo "MDM PIPELINE COMPLETED SUCCESSFULLY 🎉"
echo "======================================"

# -----------------------
# PHASE 6 — IDENTITY RESOLUTION (GLOBAL CUSTOMER ID)
# -----------------------
echo_step "Phase 6 — Global Customer ID Resolution"

python identity/run_identity_resolution.py

psql_exec <<'SQL'
SELECT COUNT(*) AS total_identity_mappings
FROM identity.customer_identity_map;
SQL

psql_exec <<'SQL'
SELECT global_customer_id, COUNT(*) AS linked_records
FROM identity.customer_identity_map
GROUP BY global_customer_id
HAVING COUNT(*) > 1
ORDER BY linked_records DESC
LIMIT 5;
SQL

echo "Phase 6 completed ✅"

# -----------------------
# PHASE 7 — GOLDEN RECORD
# -----------------------
echo_step "Phase 7 — Golden Record Construction"

python gold/run_golden_customers.py

psql_exec <<'SQL'
SELECT COUNT(*) FROM gold.dim_customers;

SELECT
  record_count,
  COUNT(*) AS customers
FROM gold.dim_customers
GROUP BY record_count
ORDER BY record_count DESC;
SQL

echo "Phase 7 completed ✅"

# -----------------------
# PHASE 8 — GOLDEN HISTORY
# -----------------------
echo_step "Phase 8 — Golden Record History (SCD2)"

python gold/run_golden_history.py

psql_exec <<'SQL'
SELECT COUNT(*) FROM gold.dim_customers_history;
SELECT COUNT(*) FROM gold.dim_customers_history WHERE is_current = true;
SQL

echo "Phase 8 completed ✅"

# -----------------------
# PHASE 9 — Golden Change Events (CDC)
# -----------------------
echo_step "Phase 9 — Golden Change Events (CDC)"

python gold/run_golden_cdc.py

psql_exec <<'SQL'
SELECT COUNT(*) FROM gold.customer_change_events;
SELECT change_type, COUNT(*) FROM gold.customer_change_events GROUP BY change_type;
SELECT * FROM gold.customer_change_events ORDER BY changed_at DESC LIMIT 5;
SQL

echo "Phase 9A completed ✅"

# -----------------------
# PHASE 9B — STEWARD OVERRIDES
# -----------------------
echo_step "Phase 9B — Steward Overrides"

python gold/run_steward_overrides.py

psql_exec <<'SQL'
SELECT COUNT(*) AS active_overrides
FROM gold.customer_steward_overrides
WHERE is_active = true;
SQL

echo "Phase 9B completed ✅"

# -----------------------
# PHASE 10 — DATA QUALKLITY & GOVERNANCE
# -----------------------
echo_step "Phase 10 — Data Quality & Governance"

apply_sql db/init/010A_gold_match_confidence_metrics.sql
apply_sql db/init/010B_gold_steward_override_metrics.sql
apply_sql db/init/010C_gold_customer_attribute_conflicts.sql
apply_sql db/init/010D_gold_steward_review_queue.sql

python gold/run_conflict_detection.py
python gold/run_quality_metrics.py

echo "Phase 10 completed ✅"