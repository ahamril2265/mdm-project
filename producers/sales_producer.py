import json, uuid, random, io
from datetime import datetime, timedelta

from minio_client import get_minio_client
from shared_identities import get_identity

RECORDS_PER_FILE = 50_000
BUCKET = "mdm-ingestion"

def ensure_bucket(client):
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

def upload(client, ts, records):
    dt = ts.strftime("%Y-%m-%d")
    hr = ts.strftime("%H")
    path = f"sales/dt={dt}/hr={hr}/sales_{uuid.uuid4().hex}.jsonl"
    data = "\n".join(records).encode()
    client.put_object(BUCKET, path, io.BytesIO(data), len(data))

def run(start_ts, hours, total_records):
    client = get_minio_client()
    ensure_bucket(client)

    per_hour = total_records // hours
    ts = start_ts

    for _ in range(hours):
        buffer = []

        for _ in range(per_hour):
            name, email, phone = get_identity()

            buffer.append(json.dumps({
                "event_type": "order_created",
                "order_id": str(uuid.uuid4()),
                "customer_name": name,
                "customer_email": email,
                "customer_phone": phone,
                "zip_code": str(random.randint(110000, 560999)),
                "order_amount": round(random.uniform(500, 5000), 2),
                "event_ts": ts.isoformat(),
                "producer_ts": datetime.utcnow().isoformat()
            }))

            if len(buffer) >= RECORDS_PER_FILE:
                upload(client, ts, buffer)
                buffer.clear()

        if buffer:
            upload(client, ts, buffer)

        ts += timedelta(hours=1)

if __name__ == "__main__":
    run(
        start_ts=datetime.utcnow(),
        hours=6,
        total_records=300
    )
