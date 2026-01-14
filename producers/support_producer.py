import json, uuid, random, io
from datetime import datetime, timedelta

from minio_client import get_minio_client
from shared_identities import get_identity

RECORDS_PER_FILE = 50_000
ISSUES = ["payment", "delivery", "refund", "login", "account"]
BUCKET = "mdm-ingestion"

def run(start_ts, hours, total_records):
    client = get_minio_client()

    per_hour = total_records // hours
    ts = start_ts

    for _ in range(hours):
        buffer = []

        for _ in range(per_hour):
            name, email, phone = get_identity()

            buffer.append(json.dumps({
                "event_type": "ticket_created",
                "ticket_id": str(uuid.uuid4()),
                "contact_email": email,
                "contact_phone": phone,
                "issue_type": random.choice(ISSUES),
                "event_ts": ts.isoformat(),
                "producer_ts": datetime.utcnow().isoformat()
            }))

            if len(buffer) >= RECORDS_PER_FILE:
                _upload(client, ts, buffer)
                buffer.clear()

        if buffer:
            _upload(client, ts, buffer)

        ts += timedelta(hours=1)

def _upload(client, ts, records):
    dt = ts.strftime("%Y-%m-%d")
    hr = ts.strftime("%H")
    path = f"support/dt={dt}/hr={hr}/support_{uuid.uuid4().hex}.jsonl"
    data = "\n".join(records).encode()
    client.put_object(BUCKET, path, io.BytesIO(data), len(data))

if __name__ == "__main__":
    run(
        start_ts=datetime.utcnow(),
        hours=6,
        total_records=300
    )
