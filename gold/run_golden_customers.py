import json
from sqlalchemy import create_engine, text
from collections import Counter
from datetime import datetime, timezone
import os
from dotenv import load_dotenv


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
# CANONICAL RULES
# =====================

EMAIL_PRIORITY = ["sales", "support", "marketing"]
PHONE_PRIORITY = ["marketing", "sales"]

def choose_by_priority(rows, field, priority):
    for src in priority:
        candidates = [
            r for r in rows
            if r["source_system"] == src and r[field] is not None
        ]
        if candidates:
            return max(candidates, key=lambda r: r["event_ts"])[field]
    return None

def most_frequent(rows, field):
    values = [r[field] for r in rows if r[field] is not None]
    if not values:
        return None
    return Counter(values).most_common(1)[0][0]

# =====================
# PHASE 7 RUNNER
# =====================

def run_phase_7():
    print("▶ Phase 7 — Golden Record Construction")

    with engine.begin() as conn:
        identity_rows = conn.execute(text("""
            SELECT
                m.global_customer_id,
                i.source_system,
                i.normalized_email,
                i.normalized_phone,
                i.normalized_name,
                i.event_ts
            FROM identity.customer_identity_map m
            JOIN staging.identity_inputs i
              ON m.source_system = i.source_system
             AND m.source_record_id = i.source_record_id
        """)).mappings().all()

        grouped = {}
        for row in identity_rows:
            grouped.setdefault(row["global_customer_id"], []).append(row)

        now_utc = datetime.now(timezone.utc)
        results = []

        for gcid, rows in grouped.items():
            results.append({
                "global_customer_id": gcid,
                "canonical_email": choose_by_priority(rows, "normalized_email", EMAIL_PRIORITY),
                "canonical_phone": choose_by_priority(rows, "normalized_phone", PHONE_PRIORITY),
                "canonical_name": most_frequent(rows, "normalized_name"),
                # ✅ FIX 1: serialize to JSON string
                "source_priority": json.dumps({
                    "email": EMAIL_PRIORITY,
                    "phone": PHONE_PRIORITY,
                    "name": "most_frequent"
                }),
                "record_count": len(rows),
                "updated_at": now_utc
            })

        # Safe rebuild
        conn.execute(text("TRUNCATE gold.dim_customers"))

        if results:
            conn.execute(text("""
                INSERT INTO gold.dim_customers (
                    global_customer_id,
                    canonical_email,
                    canonical_phone,
                    canonical_name,
                    source_priority,
                    record_count,
                    updated_at
                ) VALUES (
                    :global_customer_id,
                    :canonical_email,
                    :canonical_phone,
                    :canonical_name,
                    :source_priority,   -- ✅ FIX 2: NO CAST
                    :record_count,
                    :updated_at
                )
            """), results)

    print(f"✅ Phase 7 completed — {len(results)} golden customers created")

if __name__ == "__main__":
    run_phase_7()
