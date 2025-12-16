import pandas as pd
from sqlalchemy import create_engine
from deltalake import DeltaTable
import os

# ======================
# KONFIGURASI DINAMIS (PENTING!)
# ======================
# 1. Database URL: Otomatis ambil dari Env (Docker) atau fallback ke string hardcode (Laptop)
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://neondb_owner:npg_B7ysWvCoLix2@ep-still-surf-ad9wrml7-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require"
)

# 2. MinIO Config: Otomatis 'minio' (Docker) atau 'localhost' (Laptop)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

# 3. Storage Options untuk Delta Lake
storage_options = {
    "AWS_ACCESS_KEY_ID": ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true", # <--- WAJIB: Agar tidak error 'builder error'
}

def read_delta(bucket, path):
    """Membaca Delta Table dari MinIO"""
    full_path = f"s3://{bucket}/{path}"
    print(f"📂 Reading Delta: {full_path}")
    dt = DeltaTable(full_path, storage_options=storage_options)
    return dt.to_pandas()

# ======================
# MAIN PROCESS
# ======================
print("--- START LOAD TO NEON ---")
print(f"Target MinIO: {MINIO_ENDPOINT}")

try:
    # 1. Baca Data dari Clean Zone (Delta Lake)
    print("⏳ Membaca data dari Clean Zone...")
    catatan = read_delta("clean-zone", "sheets/catatan_aktivitas")
    master = read_delta("clean-zone", "sheets/master_aktivitas")
    
    # 2. Transformasi & Join
    catatan["timestamp"] = pd.to_datetime(catatan["timestamp"])

    # Left join: menggabungkan log aktivitas dengan master datanya (bobot, nama, dll)
    df = catatan.merge(
        master,
        on="id_aktivitas",
        how="left"
    )

    # Handle jika ada aktivitas yang tidak ketemu di master
    if "nama_aktivitas" not in df.columns:
        df["nama_aktivitas"] = "Unknown"
    else:
        df["nama_aktivitas"] = df["nama_aktivitas"].fillna("Unknown")

    # Pilih kolom final untuk dashboard
    df = df[[
        "timestamp",
        "id_aktivitas",
        "nama_aktivitas",
        "durasi_menit",
        "bobot_kotor",
        "bobot_bau"
    ]]

    print(f"📊 Data siap load: {len(df)} baris")

    # 3. Load ke Neon (Postgres)
    print("🔌 Connecting to Neon DB...")
    engine = create_engine(DATABASE_URL)
    
    # Simpan ke tabel 'aktivitas_setelah_mandi'
    df.to_sql(
        "aktivitas_setelah_mandi",
        engine,
        if_exists="append",
        index=False
    )
    
    print("✅ SUKSES: Data Aktivitas berhasil dimuat ke Neon")
    print(df.head())

except Exception as e:
    print(f"❌ GAGAL Load to Neon: {e}")
    # Raise error agar Airflow sadar task ini gagal (jadi Merah)
    raise e