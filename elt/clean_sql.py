import pandas as pd
import boto3
from botocore.client import Config
import io

MINIO_ENDPOINT = "http://localhost:9000"
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

def get_latest_csv(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]
    if not files:
        raise FileNotFoundError("CSV SQL tidak ditemukan")
    return sorted(files, key=lambda x: x["LastModified"], reverse=True)[0]["Key"]

sql_key = get_latest_csv(RAW_BUCKET, "sql/log_mandi/")
print(f"[RAW SQL] pakai → {sql_key}")

obj = s3.get_object(Bucket=RAW_BUCKET, Key=sql_key)
df = pd.read_csv(obj["Body"])

df["waktu_mandi"] = pd.to_datetime(df["waktu_mandi"])
df["tingkat_bau_badan"] = pd.to_numeric(df["tingkat_bau_badan"], errors="coerce")

buffer = io.StringIO()
df.to_csv(buffer, index=False)

s3.put_object(
    Bucket=CLEAN_BUCKET,
    Key="sql/log_mandi.csv",
    Body=buffer.getvalue()
)

print("[CLEAN SQL] sql/log_mandi.csv berhasil dibuat")
