import pandas as pd
import boto3
from botocore.client import Config
import io
import os
from datetime import datetime
from deltalake.writer import write_deltalake

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
    path_aktivitas = find_latest_by_metadata(RAW_BUCKET, "sheets/", "nama_sheet", "aktivitas_manual")
    path_mandi = find_latest_by_metadata(RAW_BUCKET, "sheets/", "nama_sheet", "log_mandi")

    # Proses Data
    for path, table_name in [(path_aktivitas, "catatan_aktivitas"), (path_mandi, "log_mandi")]:
        obj = s3.get_object(Bucket=RAW_BUCKET, Key=path)
        content = obj["Body"].read().decode("utf-8").replace("\ufeff", "")
        df = pd.read_csv(io.StringIO(content))
        
        # Normalisasi Kolom
        df.columns = df.columns.astype(str).str.strip().str.lower().str.replace(" ", "_")
        if "waktu_mandi" in df.columns: df = df.rename(columns={"waktu_mandi": "timestamp"})
        
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])

        write_deltalake(f"s3://{CLEAN_BUCKET}/sheets/{table_name}", df, mode="overwrite", schema_mode="overwrite", storage_options=storage_options)

    print("✅ CLEAN SHEETS SELESAI")
except Exception as e:
    print(f"❌ Error Clean Sheets: {e}")