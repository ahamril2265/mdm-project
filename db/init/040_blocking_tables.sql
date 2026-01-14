CREATE TABLE IF NOT EXISTS staging.identity_match_candidates_blocked (
    left_source_system TEXT,
    left_record_id TEXT,
    left_email TEXT,
    left_phone TEXT,
    left_name TEXT,

    right_source_system TEXT,
    right_record_id TEXT,
    right_email TEXT,
    right_phone TEXT,
    right_name TEXT
);
