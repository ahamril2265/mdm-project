from sqlalchemy import create_engine, text
from tqdm import tqdm
import os
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@localhost:"
    f"{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}",
    future=True,
)

STAGING_SQL = [
    # -------------------------
    # SALES
    # -------------------------
    ("stg_sales_customers", """
        TRUNCATE staging.stg_sales_customers;

        INSERT INTO staging.stg_sales_customers (
            order_id,
            normalized_email,
            normalized_phone,
            normalized_name,
            order_ts
        )
        SELECT
            order_id,
            LOWER(TRIM(email))      AS normalized_email,
            NULL                    AS normalized_phone,
            LOWER(TRIM(name))       AS normalized_name,
            order_ts::timestamp
        FROM raw.sales_orders;
    """),

    # -------------------------
    # SUPPORT
    # -------------------------
    ("stg_support_contacts", """
        TRUNCATE staging.stg_support_contacts;

        INSERT INTO staging.stg_support_contacts (
            ticket_id,
            normalized_email,
            normalized_phone,
            ticket_ts
        )
        SELECT
            ticket_id,
            LOWER(TRIM(contact_email)) AS normalized_email,
            NULL                       AS normalized_phone,
            ticket_ts::timestamp
        FROM raw.support_tickets;
    """),

    # -------------------------
    # MARKETING
    # -------------------------
    ("stg_marketing_leads", """
        TRUNCATE staging.stg_marketing_leads;

        INSERT INTO staging.stg_marketing_leads (
            lead_id,
            normalized_name,
            normalized_phone,
            lead_ts
        )
        SELECT
            lead_id,
            LOWER(TRIM(full_name))          AS normalized_name,
            REGEXP_REPLACE(phone, '\\D', '', 'g') AS normalized_phone,
            lead_ts::timestamp
        FROM raw.marketing_leads;
    """),
]

def run_staging():
    print("▶ Phase 3: Staging normalization")

    with engine.begin() as conn:
        for table_name, stmt in tqdm(
            STAGING_SQL,
            desc="Building staging tables",
            unit="table"
        ):
            conn.execute(text(stmt))

    print("✅ Phase 3 completed")

if __name__ == "__main__":
    run_staging()
