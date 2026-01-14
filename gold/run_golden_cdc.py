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

def run_phase_9a():
    print("▶ Phase 9A — Golden Change Events (CDC)")

    now = datetime.now(timezone.utc)

    with engine.begin() as conn:

        # 1️⃣ Newly activated records
        new_rows = conn.execute(text("""
            SELECT *
            FROM gold.dim_customers_history
            WHERE is_current = true
        """)).mappings().all()

        # 2️⃣ Previous versions
        old_rows = conn.execute(text("""
            SELECT *
            FROM gold.dim_customers_history
            WHERE is_current = false
        """)).mappings().all()

        old_map = {}
        for r in old_rows:
            old_map.setdefault(r["global_customer_id"], []).append(r)

        events = []

        for new in tqdm(new_rows, desc="Detecting changes"):
            candidates = old_map.get(new["global_customer_id"], [])

            # Find immediately previous version
            prev = next(
                (
                    r for r in candidates
                    if r["valid_to"] == new["valid_from"]
                ),
                None
            )

            if not prev:
                change_type = "INSERT"
                old_payload = None
            else:
                change_type = "UPDATE"
                old_payload = {
                    "canonical_email": prev["canonical_email"],
                    "canonical_phone": prev["canonical_phone"],
                    "canonical_name": prev["canonical_name"],
                    "record_count": prev["record_count"],
                    "source_priority": prev["source_priority"],
                }

            new_payload = {
                "canonical_email": new["canonical_email"],
                "canonical_phone": new["canonical_phone"],
                "canonical_name": new["canonical_name"],
                "record_count": new["record_count"],
                "source_priority": new["source_priority"],
            }

            events.append({
                "global_customer_id": new["global_customer_id"],
                "change_type": change_type,
                "old_record": json.dumps(old_payload) if old_payload else None,
                "new_record": json.dumps(new_payload),
                "changed_at": now,
            })

        if events:
            conn.execute(text("""
                INSERT INTO gold.customer_change_events (
                    global_customer_id,
                    change_type,
                    old_record,
                    new_record,
                    changed_at
                ) VALUES (
                    :global_customer_id,
                    :change_type,
                    :old_record,
                    :new_record,
                    :changed_at
                )
            """), events)

    print(f"✅ Phase 9A completed — {len(events)} change events emitted")

if __name__ == "__main__":
    run_phase_9a()
