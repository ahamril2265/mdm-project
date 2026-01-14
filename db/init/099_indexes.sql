CREATE INDEX IF NOT EXISTS idx_identity_inputs_email
ON staging.identity_inputs (normalized_email);

CREATE INDEX IF NOT EXISTS idx_identity_inputs_phone
ON staging.identity_inputs (normalized_phone);

CREATE INDEX IF NOT EXISTS idx_identity_map_gcid
ON identity.customer_identity_map (global_customer_id);
