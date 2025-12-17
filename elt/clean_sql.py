import pandas as pd
import boto3
from botocore.client import Config
import io
import os
from deltalake.writer import write_deltalake

# ======================================================
# CONFIGURASI
# ======================================================
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

def get_latest_csv(bucket, table_name):
    """Mencari file CSV terbaru di folder tabel terkait"""
    prefix = f"sql/{table_name}/"
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]
    if not files:
        raise FileNotFoundError(f"Data mentah untuk folder {table_name} tidak ditemukan")
    return sorted(files, key=lambda x: x["LastModified"], reverse=True)[0]["Key"]

# ======================================================
# PROCESS: JOIN DATA
# ======================================================
try:
    # 1. Ambil path file terbaru dari raw-zone
    key_aktivitas = get_latest_csv(RAW_BUCKET, "aktivitas")
    key_kategori = get_latest_csv(RAW_BUCKET, "kategori")
    
    print(f"[RAW] Menggabungkan: {key_aktivitas} dan {key_kategori}")

    # 2. Baca data dari Minio
    obj_akt = s3.get_object(Bucket=RAW_BUCKET, Key=key_aktivitas)
    obj_kat = s3.get_object(Bucket=RAW_BUCKET, Key=key_kategori)
    
    df_aktivitas = pd.read_csv(obj_akt["Body"])
    df_kategori = pd.read_csv(obj_kat["Body"])

    # 3. Join Tabel berdasarkan id_kategori
    # Ini akan menggabungkan nama_aktivitas dengan nama_kategori (Indoor/Outdoor)
    df_clean = pd.merge(
        df_aktivitas, 
        df_kategori, 
        on="id_kategori", 
        how="left"
    )

    # 4. Pembersihan Tipe Data (Ensuring MET is numeric) [cite: 5, 25]
    df_clean["skor_met"] = pd.to_numeric(df_clean["skor_met"], errors="coerce") 

    # ======================================================
    # SIMPAN KE CLEAN-ZONE (DELTA LAKE)
    # ======================================================
    path = f"s3://{CLEAN_BUCKET}/sql/aktivitas_joined_master"
    print(f"💾 Menyimpan Tabel Gabungan ke Delta: {path}")

    write_deltalake(
        path,
        df_clean,
        mode="overwrite", 
        schema_mode="overwrite",  
        storage_options=storage_options
    )

    print("✅ CLEAN LAYER SELESAI: Data sudah digabung")
    print(df_clean.head())

except Exception as e:
    print(f"❌ Gagal Transformasi Clean: {e}")
    raise e