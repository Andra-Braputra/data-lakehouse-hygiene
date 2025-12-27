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
    key_akt = find_latest_by_metadata(RAW_BUCKET, "sql/", "nama_tabel", "aktivitas")
    key_kat = find_latest_by_metadata(RAW_BUCKET, "sql/", "nama_tabel", "kategori")

    df_akt = pd.read_csv(s3.get_object(Bucket=RAW_BUCKET, Key=key_akt)["Body"])
    df_kat = pd.read_csv(s3.get_object(Bucket=RAW_BUCKET, Key=key_kat)["Body"])

    df_clean = pd.merge(df_akt, df_kat, on="id_kategori", how="left")
    df_clean["skor_met"] = pd.to_numeric(df_clean["skor_met"], errors="coerce")

    write_deltalake(f"s3://{CLEAN_BUCKET}/sql/aktivitas_joined_master", df_clean, mode="overwrite", schema_mode="overwrite", storage_options=storage_options)
    print("✅ CLEAN SQL SELESAI")
except Exception as e:
    print(f"❌ Gagal Clean SQL: {e}")