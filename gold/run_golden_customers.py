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
# SURVIVORSHIP CONFIG
# =====================

def load_survivorship_config(conn):
    rules = conn.execute(text("""
        SELECT attribute_name, source_system, priority_rank
        FROM gold.survivorship_rules
        ORDER BY attribute_name, priority_rank
    """)).mappings().all()

    strategies = conn.execute(text("""
        SELECT attribute_name, strategy
        FROM gold.survivorship_strategies
    """)).mappings().all()

    priority_map = {}
    for r in rules:
        priority_map.setdefault(r["attribute_name"], []).append(r["source_system"])

    strategy_map = {
        s["attribute_name"]: s["strategy"]
        for s in strategies
    }

    return priority_map, strategy_map


def resolve_attribute(rows, field, strategy, priority_map):
    """
    Generic survivorship resolver
    """
    if strategy == "most_frequent":
        values = [r[field] for r in rows if r[field]]
        return Counter(values).most_common(1)[0][0] if values else None

    if strategy == "priority":
        attr = field.replace("normalized_", "")
        for src in priority_map.get(attr, []):
            candidates = [
                r for r in rows
                if r["source_system"] == src and r[field]
            ]
            if candidates:
                return max(candidates, key=lambda r: r["event_ts"])[field]

    return None

# =====================
# PHASE 7 RUNNER
# =====================

def run_phase_7():
    print("▶ Phase 7 — Golden Record Construction")

    with engine.begin() as conn:
        priority_map, strategy_map = load_survivorship_config(conn)

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
                "canonical_email": resolve_attribute(
                    rows,
                    "normalized_email",
                    strategy_map["email"],
                    priority_map
                ),
                "canonical_phone": resolve_attribute(
                    rows,
                    "normalized_phone",
                    strategy_map["phone"],
                    priority_map
                ),
                "canonical_name": resolve_attribute(
                    rows,
                    "normalized_name",
                    strategy_map["name"],
                    priority_map
                ),
                "source_priority": json.dumps(strategy_map),
                "record_count": len(rows),
                "updated_at": now_utc
            })

        # rebuild safely
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
                    :source_priority,
                    :record_count,
                    :updated_at
                )
            """), results)

    print(f"✅ Phase 7 completed — {len(results)} golden customers created")


if __name__ == "__main__":
    run_phase_7()
