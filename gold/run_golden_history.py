import json
from sqlalchemy import create_engine, text
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@localhost:"
    f"{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}",
    future=True,
)

def run_phase_8():
    print("▶ Phase 8 — Golden Record History (SCD2)")

    now = datetime.now(timezone.utc)

    with engine.begin() as conn:

        current = conn.execute(text("""
            SELECT *
            FROM gold.dim_customers
        """)).mappings().all()

        history = conn.execute(text("""
            SELECT *
            FROM gold.dim_customers_history
            WHERE is_current = true
        """)).mappings().all()

        hist_map = {
            h["global_customer_id"]: h
            for h in history
        }

        inserts = []
        expirations = []

        for row in tqdm(current, desc="SCD2 processing"):

            prev = hist_map.get(row["global_customer_id"])

            if not prev:
                inserts.append({
                    **row,
                    "valid_from": now
                })
                continue

            if (
                row["canonical_email"] != prev["canonical_email"] or
                row["canonical_phone"] != prev["canonical_phone"] or
                row["canonical_name"]  != prev["canonical_name"]  or
                row["record_count"]    != prev["record_count"]
            ):
                expirations.append(prev["history_id"])
                inserts.append({
                    **row,
                    "valid_from": now
                })

        # 🔻 Expire old versions
        if expirations:
            conn.execute(
                text("""
                    UPDATE gold.dim_customers_history
                    SET valid_to = :now,
                        is_current = false
                    WHERE history_id IN :ids
                """).bindparams(
                    text("ids").bindparam(expanding=True)
                ),
                {"now": now, "ids": expirations}
            )

        # 🔺 Insert new versions
        if inserts:
            conn.execute(text("""
                INSERT INTO gold.dim_customers_history (
                    global_customer_id,
                    canonical_email,
                    canonical_phone,
                    canonical_name,
                    source_priority,
                    record_count,
                    valid_from,
                    is_current
                ) VALUES (
                    :global_customer_id,
                    :canonical_email,
                    :canonical_phone,
                    :canonical_name,
                    :source_priority,
                    :record_count,
                    :valid_from,
                    true
                )
            """), [
                {
                    **r,
                    # ✅ JSON must be string
                    "source_priority": json.dumps(r["source_priority"])
                }
                for r in inserts
            ])

    print(f"✅ Phase 8 completed — {len(inserts)} history records written")

if __name__ == "__main__":
    run_phase_8()
