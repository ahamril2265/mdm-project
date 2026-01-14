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

def run_phase_10a():
    print("▶ Phase 10A — Match Confidence Metrics")

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO gold.match_confidence_metrics (
                metric_date,
                total_links,
                auto_merges,
                flagged_reviews,
                rejected_links,
                avg_confidence_score,
                updated_at
            )
            SELECT
                CURRENT_DATE,
                COUNT(*)                                  AS total_links,
                COUNT(*) FILTER (WHERE match_decision='AUTO_MERGE'),
                COUNT(*) FILTER (WHERE match_decision='FLAG_REVIEW'),
                COUNT(*) FILTER (WHERE match_decision='REJECT'),
                AVG(total_confidence_score),
                now()
            FROM staging.identity_match_candidates
            ON CONFLICT (metric_date)
            DO UPDATE SET
                total_links          = EXCLUDED.total_links,
                auto_merges          = EXCLUDED.auto_merges,
                flagged_reviews      = EXCLUDED.flagged_reviews,
                rejected_links       = EXCLUDED.rejected_links,
                avg_confidence_score = EXCLUDED.avg_confidence_score,
                updated_at           = now();
        """))

    print("✅ Phase 10A completed")

if __name__ == "__main__":
    run_phase_10a()
