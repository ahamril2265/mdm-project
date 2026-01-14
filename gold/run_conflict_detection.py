from sqlalchemy import create_engine, text
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@localhost:"
    f"{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}",
    future=True,
)

ATTRIBUTES = {
    "normalized_email": "normalized_email",
    "normalized_phone": "normalized_phone",
    "normalized_name": "normalized_name",
}

def run_phase_10_conflicts():
    print("▶ Phase 10 — Attribute Conflict Detection")

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE gold.customer_attribute_conflicts"))

        for attr_name, col in ATTRIBUTES.items():
            conn.execute(text(f"""
                INSERT INTO gold.customer_attribute_conflicts
                SELECT
                    m.global_customer_id,
                    '{attr_name}',
                    COUNT(DISTINCT i.{col}),
                    now()
                FROM identity.customer_identity_map m
                JOIN staging.identity_inputs i
                  ON m.source_system = i.source_system
                 AND m.source_record_id = i.source_record_id
                WHERE i.{col} IS NOT NULL
                GROUP BY m.global_customer_id
                HAVING COUNT(DISTINCT i.{col}) > 1
            """))

    print("✅ Phase 10 — Conflict detection completed")

if __name__ == "__main__":
    run_phase_10_conflicts()
