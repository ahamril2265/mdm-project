import json
import re
from datetime import datetime
from minio import Minio
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# =====================
# ENV
# =====================

load_dotenv()

MINIO_BUCKET = "mdm-ingestion"

# =====================
# CLIENTS
# =====================

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@localhost:"
    f"{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}",
    future=True
)

with engine.connect() as conn:
    print("CONNECTED DATABASE:",
          conn.execute(text("SELECT current_database()")).scalar())
    print("CONNECTED USER:",
          conn.execute(text("SELECT current_user")).scalar())
    print("SEARCH PATH:",
          conn.execute(text("SHOW search_path")).scalar())

    tables = conn.execute(text("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_name = 'ingestion_watermarks'
    """)).fetchall()

    print("FOUND ingestion_watermarks:", tables)
    
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False,
)

# =====================
# CONSTANTS
# =====================

PATH_RE = re.compile(r"dt=(\d{4}-\d{2}-\d{2})/hr=(\d{2})")

# --------------------
# ENSURE BUCKET
# --------------------

def ensure_bucket(bucket):
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

# =====================
# WATERMARKS
# =====================

def get_watermark(source):
    sql = text("""
        SELECT last_ingested_ts
        FROM identity.ingestion_watermarks
        WHERE source_system = :source
    """)
    with engine.begin() as conn:
        return conn.execute(sql, {"source": source}).scalar()

def update_watermark(source, ts):
    sql = text("""
        UPDATE identity.ingestion_watermarks
        SET last_ingested_ts = :ts
        WHERE source_system = :source
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"source": source, "ts": ts})

# =====================
# DISCOVERY
# =====================

def list_new_objects(source, last_ts):
    objects = minio_client.list_objects(
        MINIO_BUCKET,
        prefix=f"{source}/",
        recursive=True
    )

    candidates = []
    for obj in objects:
        match = PATH_RE.search(obj.object_name)
        if not match:
            continue

        dt, hr = match.groups()
        obj_ts = datetime.strptime(f"{dt} {hr}", "%Y-%m-%d %H")

        if obj_ts > last_ts:
            candidates.append((obj_ts, obj.object_name))

    return sorted(candidates, key=lambda x: x[0])

# =====================
# STREAM JSONL
# =====================

def stream_jsonl(object_name):
    response = minio_client.get_object(MINIO_BUCKET, object_name)

    buffer = b""
    for chunk in response.stream(1024 * 1024):
        buffer += chunk

        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            if line.strip():
                yield json.loads(line.decode("utf-8"))

    # flush last line (if file doesn't end with newline)
    if buffer.strip():
        yield json.loads(buffer.decode("utf-8"))


# =====================
# INGESTORS
# =====================

def ingest_sales(obj):
    rows = [{
        "order_id": e["order_id"],
        "email": e["customer_email"],
        "name": e["customer_name"],
        "zip_code": e["zip_code"],
        "order_amount": e["order_amount"],
        "order_ts": e["event_ts"]
    } for e in stream_jsonl(obj)]

    sql = text("""
        INSERT INTO raw.sales_orders
        (order_id, email, name, zip_code, order_amount, order_ts)
        VALUES
        (:order_id, :email, :name, :zip_code, :order_amount, :order_ts)
        ON CONFLICT (order_id) DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(sql, rows)

def ingest_support(obj):
    rows = [{
        "ticket_id": e["ticket_id"],
        "contact_email": e["contact_email"],
        "issue_type": e["issue_type"],
        "ticket_ts": e["event_ts"]
    } for e in stream_jsonl(obj)]

    sql = text("""
        INSERT INTO raw.support_tickets
        (ticket_id, contact_email, issue_type, ticket_ts)
        VALUES
        (:ticket_id, :contact_email, :issue_type, :ticket_ts)
        ON CONFLICT (ticket_id) DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(sql, rows)

def ingest_marketing(obj):
    rows = [{
        "lead_id": e["lead_id"],
        "full_name": e["full_name"],
        "phone": e["phone"],
        "lead_ts": e["event_ts"]
    } for e in stream_jsonl(obj)]

    sql = text("""
        INSERT INTO raw.marketing_leads
        (lead_id, full_name, phone, lead_ts)
        VALUES
        (:lead_id, :full_name, :phone, :lead_ts)
        ON CONFLICT (lead_id) DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(sql, rows)

# =====================
# ORCHESTRATOR
# =====================

def ingest_source(source):
    print(f"\n▶ Ingesting {source}")

    ensure_bucket(MINIO_BUCKET)

    last_ts = get_watermark(source)
    objects = list_new_objects(source, last_ts)

    if not objects:
        print("No new data")
        return

    max_ts = last_ts

    for obj_ts, obj_name in objects:
        print(f"  → {obj_name}")

        if source == "sales":
            ingest_sales(obj_name)
        elif source == "support":
            ingest_support(obj_name)
        else:
            ingest_marketing(obj_name)

        max_ts = max(max_ts, obj_ts)

    update_watermark(source, max_ts)
    print(f"✔ Watermark updated to {max_ts}")

# =====================
# ENTRY
# =====================

if __name__ == "__main__":
    for src in ["sales", "support", "marketing"]:
        ingest_source(src)
