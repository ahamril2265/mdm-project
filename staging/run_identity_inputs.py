from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from tqdm import tqdm

# =====================
# ENV & ENGINE
# =====================

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@localhost:"
    f"{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}",
    future=True,
)

# =====================
# PHASE 4 SQL (NO DDL)
# =====================

IDENTITY_INPUTS_SQL = """
TRUNCATE staging.identity_inputs;

INSERT INTO staging.identity_inputs (
    source_system,
    source_record_id,
    normalized_email,
    normalized_phone,
    normalized_name,
    event_ts
)

SELECT
    'sales' AS source_system,
    order_id AS source_record_id,
    normalized_email,
    NULL AS normalized_phone,
    normalized_name,
    order_ts AS event_ts
FROM staging.stg_sales_customers

UNION ALL

SELECT
    'support' AS source_system,
    ticket_id AS source_record_id,
    normalized_email,
    NULL AS normalized_phone,
    NULL AS normalized_name,
    ticket_ts AS event_ts
FROM staging.stg_support_contacts

UNION ALL

SELECT
    'marketing' AS source_system,
    lead_id AS source_record_id,
    NULL AS normalized_email,
    normalized_phone,
    normalized_name,
    lead_ts AS event_ts
FROM staging.stg_marketing_leads;
"""

# =====================
# RUNNER
# =====================

def run_identity_inputs():
    print("▶ Phase 4: Identity Inputs")

    with engine.begin() as conn:
        for _ in tqdm(
            range(1),
            desc="Building identity inputs",
            unit="step"
        ):
            conn.execute(text(IDENTITY_INPUTS_SQL))

    print("✅ Phase 4 completed — staging.identity_inputs refreshed")

if __name__ == "__main__":
    run_identity_inputs()
