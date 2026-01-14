from sqlalchemy import create_engine, text
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@localhost:"
    f"{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}",
    future=True,
)

def run_phase_9b():
    print("▶ Phase 9B — Applying Steward Overrides")

    now = datetime.now(timezone.utc)

    with engine.begin() as conn:

        overrides = conn.execute(text("""
            SELECT
                global_customer_id,
                attribute_name,
                override_value
            FROM gold.customer_steward_overrides
            WHERE is_active = true
              AND valid_from <= :now
              AND (valid_to IS NULL OR valid_to > :now)
        """), {"now": now}).mappings().all()

        if not overrides:
            print("No active steward overrides found")
            return

        for o in overrides:
            if o["attribute_name"] not in {
                "canonical_email",
                "canonical_phone",
                "canonical_name"
            }:
                continue

            conn.execute(text(f"""
                UPDATE gold.dim_customers
                SET {o["attribute_name"]} = :value,
                    updated_at = :now
                WHERE global_customer_id = :gcid
            """), {
                "value": o["override_value"],
                "gcid": o["global_customer_id"],
                "now": now
            })

    print(f"✅ Phase 9B completed — {len(overrides)} overrides applied")

if __name__ == "__main__":
    run_phase_9b()
