import json
import pandas as pd
import boto3
from botocore.client import Config
import os
from deltalake.writer import write_deltalake
from datetime import datetime

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
RAW_BUCKET = "raw-zone"
CLEAN_BUCKET = "clean-zone"

s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT, aws_access_key_id=ACCESS_KEY, 
                  aws_secret_access_key=SECRET_KEY, config=Config(signature_version="s3v4"), region_name="us-east-1")

storage_options = {
    "AWS_ACCESS_KEY_ID": ACCESS_KEY, "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT, "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true",
}

def find_latest_by_metadata(bucket, prefix, meta_key, meta_value):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response: return None
    
    valid_files = []
    for obj in response["Contents"]:
        metadata = s3.head_object(Bucket=bucket, Key=obj["Key"]).get("Metadata", {})
        if metadata.get(meta_key.lower()) == meta_value:
            waktu_str = metadata.get("waktu_ingest")
            if waktu_str:
                valid_files.append({"key": obj["Key"], "waktu": datetime.strptime(waktu_str, '%Y-%m-%d %H:%M:%S')})
    
    return sorted(valid_files, key=lambda x: x["waktu"], reverse=True)[0]["key"] if valid_files else None

try:
    bmkg_key = find_latest_by_metadata(RAW_BUCKET, "api/bmkg/", "nama_data", "prakiraan_cuaca")
    
    if not bmkg_key:
        raise FileNotFoundError("Data BMKG tidak ditemukan via metadata")

    print(f"[CLEAN BMKG] Memproses data terbaru: {bmkg_key}")

    obj = s3.get_object(Bucket=RAW_BUCKET, Key=bmkg_key)
    raw = json.loads(obj["Body"].read())

    rows = []
    for lokasi in raw.get("data", []):
        for cuaca_harian in lokasi.get("cuaca", []):
            for item in cuaca_harian:
                rows.append({
                    "datetime": item.get("local_datetime"),
                    "temperature": item.get("t"),
                    "humidity": item.get("hu"),
                    "weather_desc": item.get("weather_desc"),
                    "wind_speed": item.get("ws")
                })

    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["datetime"])

    write_deltalake(f"s3://{CLEAN_BUCKET}/api/bmkg", df, mode="overwrite", storage_options=storage_options)
    print("✅ CLEAN BMKG SELESAI")

except Exception as e:
    print(f"❌ Gagal Clean BMKG: {e}")