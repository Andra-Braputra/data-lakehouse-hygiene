import pandas as pd
from sqlalchemy import create_engine
from deltalake import DeltaTable
import os

# ======================================================
# CONFIG ENV
# ======================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
CURATED_BUCKET = "curated-zone"

# Database URL Neon
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://neondb_owner:npg_B7ysWvCoLix2@ep-still-surf-ad9wrml7-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require"
)

# Config Delta
storage_options = {
    "AWS_ACCESS_KEY_ID": ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
}

# ======================================================
# LOAD DATA DARI CURATED (DELTA LAKE)
# ======================================================
print("--- LOAD PRESCRIPTIVE TO SQL ---")

try:
    path = f"s3://{CURATED_BUCKET}/prescriptive/hasil_preskriptif"
    print(f"📂 Membaca Delta Table: {path}")
    
    # Baca Delta Table langsung jadi Pandas DataFrame
    dt = DeltaTable(path, storage_options=storage_options)
    df = dt.to_pandas()
    
    if df.empty:
        print("⚠️ Data Curated kosong, skip loading.")
        exit(0)

    # Pastikan kolom waktu dikenali Pandas
    df["generated_at"] = pd.to_datetime(df["generated_at"])

    print(f"📊 Data ditemukan: {len(df)} baris")
    print(df.tail(3))

    # ======================================================
    # LOAD KE NEON DB
    # ======================================================
    print("🔌 Connecting to Neon DB...")
    engine = create_engine(DATABASE_URL)
    
    df.to_sql(
        "hasil_preskriptif",
        engine,
        if_exists="append", 
        index=False
    )

    print("✅ SUKSES: Data Prescriptive berhasil dimuat ke Neon")

except Exception as e:
    print(f"❌ Error Load Prescriptive: {e}")
    # Handle jika table belum ada (first run)
    if "Not a Delta Table" in str(e):
        print("⚠️ Delta Table belum terbentuk (Run Pertama), abaikan.")
    else:
        raise e