CREATE TABLE IF NOT EXISTS staging.identity_match_candidates (
    left_source_system TEXT,
    left_record_id TEXT,
    right_source_system TEXT,
    right_record_id TEXT,
    email_match_score INT,
    phone_match_score INT,
    name_match_score INT,
    total_confidence_score INT,
    match_decision TEXT,
    evaluated_at TIMESTAMP
);
