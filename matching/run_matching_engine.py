from sqlalchemy import create_engine, text
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
from difflib import SequenceMatcher
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
# SCORING FUNCTIONS
# =====================

def name_similarity(a, b):
    if not a or not b:
        return 0
    return SequenceMatcher(None, a, b).ratio()

def score_pair(left, right):
    score = 0
    email_score = 0
    phone_score = 0
    name_score = 0

    if left["email"] and left["email"] == right["email"]:
        email_score = 70
        score += email_score

    if left["phone"] and left["phone"] == right["phone"]:
        phone_score = 70
        score += phone_score

    if name_similarity(left["name"], right["name"]) >= 0.85:
        name_score = 30
        score += name_score

    if email_score == 70:
        decision = "AUTO_MERGE"
    elif score >= 65:
        decision = "FLAG_REVIEW"
    else:
        decision = "REJECT"

    return email_score, phone_score, name_score, score, decision

# =====================
# SQL (NO DDL)
# =====================

MATCH_QUERY = """
SELECT *
FROM staging.identity_match_candidates_blocked
"""

INSERT_SQL = """
TRUNCATE staging.identity_match_candidates;

INSERT INTO staging.identity_match_candidates (
    left_source_system,
    left_record_id,
    right_source_system,
    right_record_id,
    email_match_score,
    phone_match_score,
    name_match_score,
    total_confidence_score,
    match_decision,
    evaluated_at
) VALUES (
    :left_source_system,
    :left_record_id,
    :right_source_system,
    :right_record_id,
    :email_match_score,
    :phone_match_score,
    :name_match_score,
    :total_confidence_score,
    :match_decision,
    :evaluated_at
);
"""

# =====================
# RUNNER
# =====================

def run_matching_engine():
    print("▶ Phase 5: Matching Engine (Python Scoring)")

    with engine.begin() as conn:
        rows = conn.execute(text(MATCH_QUERY)).mappings().all()

        results = []

        for row in tqdm(rows, desc="Scoring identity pairs", unit="pair"):
            left = {
                "email": row["left_email"],
                "phone": row["left_phone"],
                "name": row["left_name"],
            }
            right = {
                "email": row["right_email"],
                "phone": row["right_phone"],
                "name": row["right_name"],
            }

            email_s, phone_s, name_s, total_s, decision = score_pair(left, right)

            results.append({
                "left_source_system": row["left_source_system"],
                "left_record_id": row["left_record_id"],
                "right_source_system": row["right_source_system"],
                "right_record_id": row["right_record_id"],
                "email_match_score": email_s,
                "phone_match_score": phone_s,
                "name_match_score": name_s,
                "total_confidence_score": total_s,
                "match_decision": decision,
                "evaluated_at": datetime.now(timezone.utc),
            })

        if results:
            conn.execute(text(INSERT_SQL), results)

    print(f"✅ Phase 5 completed — {len(results)} match candidates generated")

if __name__ == "__main__":
    run_matching_engine()
