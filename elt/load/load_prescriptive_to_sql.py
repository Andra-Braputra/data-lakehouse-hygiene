import pandas as pd
from sqlalchemy import create_engine
from deltalake import DeltaTable
import os
import sys

# ======================================================
# KONFIGURASI
# ======================================================
# Menggunakan default URL agar tidak bernilai None jika env var tidak ada
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://neondb_owner:npg_B7ysWvCoLix2@ep-still-surf-ad9wrml7-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require"
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin123"
CURATED_BUCKET = "curated-zone"

storage_options = {
    "AWS_ACCESS_KEY_ID": ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
}

# ======================================================
# LOADING HASIL PRESKRIPTIF KE TABEL BARU
# ======================================================
try:
    print("🚀 Memuat hasil rekomendasi terbaru ke Neon...")

    # Path sumber data di Curated Zone
    path_curated = f"s3://{CURATED_BUCKET}/prescriptive_hygiene"
    
    dt = DeltaTable(path_curated, storage_options=storage_options)
    df_hasil = dt.to_pandas()

    if not df_hasil.empty:
        # Pastikan kolom waktu valid
        if "generated_at" in df_hasil.columns:
            df_hasil["generated_at"] = pd.to_datetime(df_hasil["generated_at"])

        engine = create_engine(DATABASE_URL)
        
        # Nama tabel baru: rekomendasi_mandi_preskriptif
        # Menggunakan 'append' agar histori skor tersimpan terus ke bawah
        df_hasil.to_sql(
            "rekomendasi_mandi_preskriptif", 
            engine, 
            if_exists="append", 
            index=False
        )
        print("✅ Berhasil: Data dimuat ke tabel [rekomendasi_mandi_preskriptif].")
    else:
        print("⚠️ Data di Curated Zone kosong.")

except Exception as e:
    print(f"❌ Gagal memuat rekomendasi: {e}")
    # Penting: raise e agar run_pipeline.py bisa menangkap error ini
    raise e