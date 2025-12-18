import pandas as pd
import boto3
from botocore.client import Config
from sqlalchemy import create_engine
import io
import os

# ======================================================
# KONFIGURASI MINIO
# ======================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

RAW_BUCKET = "raw-zone"
PREFIX = "sheets/log_mandi/"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# ======================================================
# AMBIL CSV TERBARU DARI RAW ZONE
# ======================================================
def get_latest_csv(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:
        raise FileNotFoundError("Tidak ada file log_mandi di raw-zone")

    csv_files = [o for o in response["Contents"] if o["Key"].endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError("File CSV log_mandi tidak ditemukan")

    latest = sorted(csv_files, key=lambda x: x["LastModified"], reverse=True)[0]
    return latest["Key"]

key = get_latest_csv(RAW_BUCKET, PREFIX)
print(f"[RAW] log_mandi pakai → {key}")

# ======================================================
# BACA CSV KE DATAFRAME
# ======================================================
obj = s3.get_object(Bucket=RAW_BUCKET, Key=key)
df = pd.read_csv(io.BytesIO(obj["Body"].read()))

df['waktu_mandi'] = pd.to_datetime(df['waktu_mandi'], errors='coerce')

print("[CHECK] Kolom log_mandi:", list(df.columns))

# ======================================================
# KONEKSI NEONDB
# ======================================================
DATABASE_URL = (
    "postgresql://neondb_owner:npg_B7ysWvCoLix2"
    "@ep-still-surf-ad9wrml7-pooler.c-2.us-east-1.aws.neon.tech/neondb"
    "?sslmode=require&channel_binding=require"
)

engine = create_engine(DATABASE_URL)

# ======================================================
# LOAD KE NEONDB (APPEND)
# ======================================================
df.to_sql(
    "riwayat_mandi",
    engine,
    if_exists="replace",
    index=False
)

print("✅ Data log_mandi berhasil di-replace ke tabel riwayat_mandi")