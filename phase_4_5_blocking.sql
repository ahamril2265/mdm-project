-- ============================================================
-- Phase 4.5 : Identity Blocking (Pair Generation)
-- Purpose:
--   Generate candidate identity pairs using deterministic
--   blocking rules (email / phone).
--
-- Output:
--   staging.identity_match_candidates_blocked
--
-- Guarantees:
--   - Cross-source only
--   - No self-pairs
--   - No duplicate mirrored pairs
--   - Safe to re-run (idempotent)
-- ============================================================

BEGIN;

-- Drop existing blocked candidates
DROP TABLE IF EXISTS staging.identity_match_candidates_blocked;

-- Create blocked candidate pairs
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
 -- Prevent mirrored duplicates (A,B) vs (B,A)
 AND a.source_record_id < b.source_record_id;

-- Optional but recommended: add indexes for Phase 5 speed
CREATE INDEX idx_blocked_left_record
    ON staging.identity_match_candidates_blocked (left_source_system, left_record_id);

CREATE INDEX idx_blocked_right_record
    ON staging.identity_match_candidates_blocked (right_source_system, right_record_id);

COMMIT;

-- ============================================================
-- Sanity check (safe to leave here)
-- ============================================================

SELECT COUNT(*) AS blocked_pair_count
FROM staging.identity_match_candidates_blocked;
