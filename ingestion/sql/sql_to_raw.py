import pandas as pd
from sqlalchemy import create_engine
import boto3
from botocore.client import Config
from datetime import datetime
import io
import os

# ======================================================
# DB & MINIO DARI ENV (SUPPORT DOCKER)
# ======================================================
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://neondb_owner:npg_B7ysWvCoLix2@ep-still-surf-ad9wrml7-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require"
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

try:
    engine = create_engine(DATABASE_URL)

    # ======================================================
    # AMBIL DATA
    # ======================================================
    query = """
    SELECT
        waktu_mandi,
        tingkat_bau_badan
    FROM log_mandi
    ORDER BY waktu_mandi
    """
    df = pd.read_sql(query, engine)

    # ======================================================
    # UPLOAD
    # ======================================================
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)

    object_key = (
        f"sql/log_mandi/"
        f"log_mandi_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    s3.put_object(
        Bucket="raw-zone",
        Key=object_key,
        Body=buffer.getvalue()
    )

    print(f"[RAW SQL] berhasil disimpan → {object_key}")

except Exception as e:
    print(f"❌ Error SQL Ingest: {e}")
    raise e