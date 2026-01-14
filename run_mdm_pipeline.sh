#!/usr/bin/env bash
set -euo pipefail

echo "======================================"
echo "MDM PIPELINE : PHASE 0 → PHASE 10"
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
  docker exec -i "$POSTGRES_CONTAINER" \
    psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1
}

apply_sql () {
  local file="$1"
  echo "→ Applying $file"
  psql_exec < "$file"
}

apply_sql () {
  local file="$1"

  if [[ ! -f "$file" ]]; then
    echo "❌ SQL file not found: $file"
    exit 1
  fi

  echo "→ Applying $file"
  psql_exec < "$file"
}

echo_step () {
  echo
  echo "▶ $1"
  echo "--------------------------------------"
}

# -----------------------
# PHASE 0 — INFRA & SCHEMA
# -----------------------
echo_step "Phase 0 — Infrastructure & Base Schema"

docker compose down -v
docker compose up -d

echo "Waiting for Postgres..."
sleep 5

for file in db/init/*.sql; do
  apply_sql "$file"
done

echo "Phase 0 completed ✅"

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

apply_sql db/init/040_blocking_tables.sql

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

# -----------------------
# PHASE 6 — IDENTITY RESOLUTION
# -----------------------
echo_step "Phase 6 — Global Customer ID Resolution"

python identity/run_identity_resolution.py

psql_exec <<'SQL'
SELECT COUNT(*) FROM identity.customer_identity_map;
SQL

echo "Phase 6 completed ✅"

# -----------------------
# PHASE 7 — GOLDEN RECORD
# -----------------------
echo_step "Phase 7 — Golden Record Construction"

python gold/run_golden_customers.py

psql_exec <<'SQL'
SELECT COUNT(*) FROM gold.dim_customers;
SQL

echo "Phase 7 completed ✅"

# -----------------------
# PHASE 8 — GOLDEN HISTORY (SCD2)
# -----------------------
echo_step "Phase 8 — Golden Record History (SCD2)"

python gold/run_golden_history.py

psql_exec <<'SQL'
SELECT COUNT(*) FROM gold.dim_customers_history;
SELECT COUNT(*) FROM gold.dim_customers_history WHERE is_current = true;
SQL

echo "Phase 8 completed ✅"

# -----------------------
# PHASE 9A — CDC
# -----------------------
echo_step "Phase 9A — Golden Change Events (CDC)"

python gold/run_golden_cdc.py

psql_exec <<'SQL'
SELECT change_type, COUNT(*) 
FROM gold.customer_change_events 
GROUP BY change_type;
SQL

echo "Phase 9A completed ✅"

# -----------------------
# PHASE 9B — STEWARD OVERRIDES
# -----------------------
echo_step "Phase 9B — Steward Overrides"

python gold/run_steward_overrides.py

psql_exec <<'SQL'
SELECT COUNT(*) 
FROM gold.customer_steward_overrides 
WHERE is_active = true;
SQL

echo "Phase 9B completed ✅"

# -----------------------
# PHASE 10 — DATA QUALITY & GOVERNANCE
# -----------------------
echo_step "Phase 10 — Data Quality & Governance"

python gold/run_conflict_detection.py
python gold/run_quality_metrics.py

psql_exec <<'SQL'
SELECT * FROM gold.match_confidence_metrics ORDER BY metric_date DESC;
SELECT COUNT(*) FROM gold.steward_review_queue;
SQL

echo "Phase 10 completed ✅"

echo
echo "======================================"
echo "MDM PIPELINE COMPLETED SUCCESSFULLY 🎉"
echo "======================================"
