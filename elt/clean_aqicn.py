import json
import pandas as pd
import boto3
from botocore.client import Config
import io
import os
from deltalake.writer import write_deltalake
from datetime import datetime

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

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

def find_latest_by_metadata(bucket, prefix, meta_key, meta_value):
    """Mencari file terbaru berdasarkan metadata 'waktu_ingest'"""
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:
        return None

    valid_files = []
    for obj in response["Contents"]:
        head = s3.head_object(Bucket=bucket, Key=obj["Key"])
        metadata = head.get("Metadata", {})
        
        # Cek apakah kategori/nama sesuai (Boto3 mengembalikan key dalam lowercase)
        if metadata.get(meta_key.lower()) == meta_value:
            waktu_str = metadata.get("waktu_ingest")
            if waktu_str:
                valid_files.append({
                    "key": obj["Key"],
                    "waktu": datetime.strptime(waktu_str, '%Y-%m-%d %H:%M:%S')
                })
    
    if not valid_files:
        return None
    
    # Sort berdasarkan waktu_ingest terbaru
    latest = sorted(valid_files, key=lambda x: x["waktu"], reverse=True)[0]
    return latest["key"]

try:
    # Mengambil file berdasarkan metadata
    aqi_key = find_latest_by_metadata(RAW_BUCKET, "api/aqicn/", "nama_data", "aqicn_air_quality")
    
    if not aqi_key:
        raise FileNotFoundError("File AQICN dengan metadata yang sesuai tidak ditemukan")

    print(f"[CLEAN AQICN] Memproses data terbaru: {aqi_key}")

    obj = s3.get_object(Bucket=RAW_BUCKET, Key=aqi_key)
    raw = json.loads(obj["Body"].read())

    if raw.get("status") != "ok":
        raise ValueError("Status AQICN tidak OK")

    data = raw.get("data", {})

    df = pd.DataFrame([{
        "datetime": data.get("time", {}).get("s"),
        "aqi": data.get("aqi"),
        "pm25": data.get("iaqi", {}).get("pm25", {}).get("v"),
        "pm10": data.get("iaqi", {}).get("pm10", {}).get("v"),
        "dominant_pollutant": data.get("dominentpol")
    }])

    df["datetime"] = pd.to_datetime(df["datetime"])

    path = f"s3://{CLEAN_BUCKET}/api/aqi"
    write_deltalake(path, df, mode="overwrite", storage_options=storage_options)
    print("✅ CLEAN AQICN SELESAI")

except Exception as e:
    print(f"❌ Gagal Clean AQICN: {e}")