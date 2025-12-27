import json
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

def get_latest_json(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:
        raise FileNotFoundError(f"Tidak ada file di {prefix}")

    files = [obj for obj in response["Contents"] if obj["Key"].endswith(".json")]
    if not files:
        raise FileNotFoundError(f"Tidak ada JSON di {prefix}")

    latest = sorted(files, key=lambda x: x["LastModified"], reverse=True)[0]
    return latest["Key"]

# ===============================
# AMBIL RAW TERBARU
# ===============================
try:
    aqi_key = get_latest_json(RAW_BUCKET, "api/aqicn/")
    print(f"[RAW AQICN] pakai → {aqi_key}")

    obj = s3.get_object(Bucket=RAW_BUCKET, Key=aqi_key)
    raw = json.loads(obj["Body"].read())

    if raw.get("status") != "ok":
        raise ValueError("Status AQICN tidak OK")

    data = raw.get("data", {})

    # ===============================
    # NORMALISASI
    # ===============================
    df = pd.DataFrame([{
        "datetime": data.get("time", {}).get("s"),
        "aqi": data.get("aqi"),
        "pm25": data.get("iaqi", {}).get("pm25", {}).get("v"),
        "pm10": data.get("iaqi", {}).get("pm10", {}).get("v"),
        "dominant_pollutant": data.get("dominentpol")
    }])

    df["datetime"] = pd.to_datetime(df["datetime"])

    # ===============================
    # SIMPAN CLEAN (DELTA)
    # ===============================
    path = f"s3://{CLEAN_BUCKET}/api/aqi"
    print(f"   💾 Menyimpan Delta: {path}")
    
    write_deltalake(
        path,
        df,
        mode="overwrite", 
        storage_options=storage_options
    )
    print("✅ CLEAN AQICN SELESAI")

except Exception as e:
    print(f"❌ Gagal Clean AQICN: {e}")