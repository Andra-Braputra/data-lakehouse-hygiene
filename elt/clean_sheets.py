import pandas as pd
import boto3
from botocore.client import Config
import io
import os
from datetime import datetime
from deltalake.writer import write_deltalake

# ======================================================
# KONFIGURASI DINAMIS (Support Docker & Laptop)
# ======================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

RAW_BUCKET = "raw-zone"
CLEAN_BUCKET = "clean-zone"

# Client Boto3
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# Config Delta Lake (Wajib untuk MinIO/S3 via HTTP)
storage_options = {
    "AWS_ACCESS_KEY_ID": ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT, 
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",           
}

# ======================================================
# HELPER FUNCTIONS
# ======================================================
def get_latest_csv(bucket, prefix):
    """Mencari file CSV terbaru berdasarkan LastModified di S3/MinIO"""
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [f for f in resp.get("Contents", []) if f["Key"].endswith(".csv")]
    if not files:
        raise FileNotFoundError(f"❌ Tidak ada file CSV di prefix: {prefix}")
    
    # Sort berdasarkan waktu modifikasi terbaru agar data yang diproses adalah yang paling fresh
    latest_file = sorted(files, key=lambda x: x["LastModified"], reverse=True)[0]["Key"]
    return latest_file

def read_csv_from_s3(bucket, key):
    """Membaca CSV dari S3 dan menangani Byte Order Mark (BOM) agar nama kolom bersih"""
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8").replace("\ufeff", "")
    return pd.read_csv(io.StringIO(content))

def normalize_columns(df):
    """Standarisasi nama kolom: huruf kecil, tanpa spasi (wajib untuk Delta Lake)"""
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("\ufeff", "", regex=False)
    )
    return df

def save_to_delta(df, bucket, table_path):
    """Menyimpan DataFrame ke format Delta Lake dengan mode overwrite skema"""
    path = f"s3://{bucket}/{table_path}"
    print(f"    💾 Menyimpan Delta Table ke: {path}")
    
    # Perbaikan: Menggunakan schema_mode="overwrite" untuk menangani perubahan jumlah kolom
    write_deltalake(
        path,
        df,
        mode="overwrite",
        schema_mode="overwrite", 
        storage_options=storage_options
    )

# ======================================================
# MAIN PROCESSING
# ======================================================
print("==============================")
print("▶ START: Cleaning Google Sheets Data")
print("==============================")

try:
    # 1. Identifikasi File Terbaru dari Raw Zone
    path_aktivitas = get_latest_csv(RAW_BUCKET, "sheets/aktivitas_manual/")
    path_mandi = get_latest_csv(RAW_BUCKET, "sheets/log_mandi/")

    print(f"🔍 File ditemukan: \n   - {path_aktivitas} \n   - {path_mandi}")

    # 2. Load Data & Normalisasi Nama Kolom
    df_aktivitas = normalize_columns(read_csv_from_s3(RAW_BUCKET, path_aktivitas))
    df_mandi = normalize_columns(read_csv_from_s3(RAW_BUCKET, path_mandi))

    # --- PROSES CLEANING: Aktivitas Manual ---
    print("🧹 Cleaning: aktivitas_manual...")
    if "timestamp" in df_aktivitas.columns:
        df_aktivitas["timestamp"] = pd.to_datetime(df_aktivitas["timestamp"], errors="coerce")
    
    if "durasi_menit" in df_aktivitas.columns:
        df_aktivitas["durasi_menit"] = pd.to_numeric(df_aktivitas["durasi_menit"], errors="coerce")
    
    # Hapus baris yang tidak memiliki timestamp valid agar data Silver bersih
    df_aktivitas = df_aktivitas.dropna(subset=["timestamp"])

    # --- PROSES CLEANING: Log Mandi ---
    print("🧹 Cleaning: log_mandi...")
    # Menangani kolom 'waktu_mandi' dari source dan meragamkannya menjadi 'timestamp'
    if "waktu_mandi" in df_mandi.columns:
        df_mandi["timestamp"] = pd.to_datetime(df_mandi["waktu_mandi"], errors="coerce")
    
    for col in ["tingkat_kekotoran", "tingkat_bau_badan"]:
        if col in df_mandi.columns:
            df_mandi[col] = pd.to_numeric(df_mandi[col], errors="coerce")

    df_mandi = df_mandi.dropna(subset=["timestamp"])

    # 3. Simpan ke Clean Zone (Delta Lake)
    save_to_delta(df_aktivitas, CLEAN_BUCKET, "sheets/catatan_aktivitas")
    save_to_delta(df_mandi, CLEAN_BUCKET, "sheets/log_mandi")

    print("✅ SEMUA PROSES SELESAI: Data berhasil disimpan ke Clean Zone.")

except Exception as e:
    print(f"❌ Error saat proses Clean: {e}")
    raise e