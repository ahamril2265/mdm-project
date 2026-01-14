from sqlalchemy import create_engine, text
from datetime import datetime, timezone
import uuid
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
# SQL
# =====================

FETCH_MATCHES = """
SELECT *
FROM staging.identity_match_candidates
WHERE match_decision IN ('AUTO_MERGE', 'FLAG_REVIEW')
"""

FETCH_EXISTING_ID = """
SELECT global_customer_id
FROM identity.customer_identity_map
WHERE source_system = :source_system
  AND source_record_id = :source_record_id
"""

REASSIGN_GLOBAL_ID = """
UPDATE identity.customer_identity_map
SET global_customer_id = :new_global_id
WHERE global_customer_id = :old_global_id
"""

INSERT_IDENTITY_MAP = """
INSERT INTO identity.customer_identity_map (
    global_customer_id,
    source_system,
    source_record_id,
    confidence_score,
    decision,
    decided_at
)
VALUES (
    :global_customer_id,
    :source_system,
    :source_record_id,
    :confidence_score,
    :decision,
    :decided_at
)
ON CONFLICT DO NOTHING
"""

# =====================
# RUNNER
# =====================

def run_identity_resolution():
    print("▶ Phase 6: Global Customer ID Resolution")

    with engine.begin() as conn:
        matches = conn.execute(text(FETCH_MATCHES)).mappings().all()

        for row in tqdm(matches, desc="Resolving identities", unit="pair"):

            left = {
                "source_system": row["left_source_system"],
                "source_record_id": row["left_record_id"],
            }
            right = {
                "source_system": row["right_source_system"],
                "source_record_id": row["right_record_id"],
            }

            left_id = conn.execute(text(FETCH_EXISTING_ID), left).scalar()
            right_id = conn.execute(text(FETCH_EXISTING_ID), right).scalar()

            # -----------------------
            # GLOBAL ID RESOLUTION
            # -----------------------

            if left_id and right_id and left_id != right_id:
                # Collapse identity graphs
                global_id = left_id
                conn.execute(
                    text(REASSIGN_GLOBAL_ID),
                    {"new_global_id": global_id, "old_global_id": right_id},
                )

            elif left_id:
                global_id = left_id

            elif right_id:
                global_id = right_id

            else:
                global_id = str(uuid.uuid4())

            # -----------------------
            # INSERT BOTH RECORDS
            # -----------------------

            for record in (left, right):
                conn.execute(
                    text(INSERT_IDENTITY_MAP),
                    {
                        "global_customer_id": global_id,
                        "source_system": record["source_system"],
                        "source_record_id": record["source_record_id"],
                        "confidence_score": row["total_confidence_score"],
                        "decision": row["match_decision"],
                        "decided_at": datetime.now(timezone.utc),
                    },
                )

    print("✅ Phase 6 completed — Global Customer IDs assigned")

if __name__ == "__main__":
    run_identity_resolution()
