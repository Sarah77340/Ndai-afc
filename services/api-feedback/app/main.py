import base64
import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import boto3
from confluent_kafka import Producer
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

app = FastAPI(title="NDAI - Private Feedback API", version="0.1.0")

TOPIC = os.getenv("KAFKA_TOPIC", "feedback.created")
API_KEY = os.getenv("API_KEY", "")

BASIC_USER = os.getenv("BASIC_USER", "")
BASIC_PASS = os.getenv("BASIC_PASS", "")


class FeedbackIn(BaseModel):
    username: str = Field(min_length=1)
    feedback_date: str = Field(min_length=10)
    campaign_id: str = Field(min_length=4)
    comment: str = Field(min_length=1)


def _basic_auth_ok(auth_header: str) -> bool:
    """
    Validate HTTP Basic Auth header against BASIC_USER/BASIC_PASS.
    Expected header format: "Basic base64(user:pass)".
    """
    if not (BASIC_USER and BASIC_PASS):
        return False
    if not auth_header or not auth_header.startswith("Basic "):
        return False

    try:
        b64 = auth_header.split(" ", 1)[1].strip()
        decoded = base64.b64decode(b64).decode("utf-8")
        user, pwd = decoded.split(":", 1)
        return user == BASIC_USER and pwd == BASIC_PASS
    except Exception:
        return False


def authorize(request: Request, x_api_key: str | None):
    """
    Authorize request if:
    - X-API-Key matches API_KEY (manual tests / internal clients)
    OR
    - Basic Auth matches BASIC_USER/BASIC_PASS (api_pusher)
    """
    # 1) API Key path
    if API_KEY and x_api_key == API_KEY:
        return

    # 2) Basic Auth path
    auth_header = request.headers.get("Authorization", "")
    if _basic_auth_ok(auth_header):
        return

    # (Use for setup) => if server has not configured, it return explicit error
    if not API_KEY and not (BASIC_USER and BASIC_PASS):
        raise HTTPException(status_code=500, detail="No auth configured on server (API_KEY or BASIC_USER/BASIC_PASS)")

    raise HTTPException(status_code=401, detail="Unauthorized")


def kafka_producer() -> Producer:
    return Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")})


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "minio"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "minio12345"),
        region_name="us-east-1",
    )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/feedback")
def create_feedback(payload: FeedbackIn, request: Request, x_api_key: str | None = Header(default=None)):
    authorize(request, x_api_key)

    feedback_id = str(uuid4())
    event = payload.model_dump()
    event["feedback_id"] = feedback_id
    event["ingested_at"] = datetime.now(timezone.utc).isoformat()

    # Store RAW in MinIO
    bucket = os.getenv("S3_BUCKET_RAW", "raw-feedback")
    key = f"dt={event['feedback_date']}/{feedback_id}.json"

    s3 = s3_client()
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(event).encode("utf-8"),
        ContentType="application/json",
    )

    # Publish to Kafka
    producer = kafka_producer()
    producer.produce(TOPIC, json.dumps(event).encode("utf-8"))
    producer.flush(5)

    return {"feedback_id": feedback_id, "stored": f"s3://{bucket}/{key}", "topic": TOPIC}