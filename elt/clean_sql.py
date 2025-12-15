import pandas as pd
import boto3
from botocore.client import Config
import io
import os
from deltalake.writer import write_deltalake

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin123"

RAW_BUCKET = "raw-zone"
CLEAN_BUCKET = "clean-zone"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

storage_options = {
    "AWS_ACCESS_KEY_ID": ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
}

def get_latest_csv(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]
    if not files:
        raise FileNotFoundError("CSV SQL tidak ditemukan")
    return sorted(files, key=lambda x: x["LastModified"], reverse=True)[0]["Key"]

# ===============================
# PROCESS
# ===============================
try:
    sql_key = get_latest_csv(RAW_BUCKET, "sql/log_mandi/")
    print(f"[RAW SQL] pakai → {sql_key}")

    obj = s3.get_object(Bucket=RAW_BUCKET, Key=sql_key)
    df = pd.read_csv(obj["Body"])

    df["waktu_mandi"] = pd.to_datetime(df["waktu_mandi"])
    df["tingkat_bau_badan"] = pd.to_numeric(df["tingkat_bau_badan"], errors="coerce")

    # ===============================
    # SIMPAN CLEAN (DELTA)
    # ===============================
    path = f"s3://{CLEAN_BUCKET}/sql/log_mandi"
    print(f"   💾 Menyimpan Delta: {path}")

    write_deltalake(
        path,
        df,
        mode="overwrite", 
        storage_options=storage_options
    )

    print("✅ CLEAN SQL SELESAI")

except Exception as e:
    print(f"❌ Gagal Clean SQL: {e}")