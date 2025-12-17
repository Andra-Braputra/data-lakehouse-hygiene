import pandas as pd
from sqlalchemy import create_engine
import boto3
from botocore.client import Config
from datetime import datetime
import io
import os

DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://neondb_owner:npg_B7ysWvCoLix2@ep-still-surf-ad9wrml7-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require"
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

target_tables = ["aktivitas", "kategori"]

engine = create_engine(DATABASE_URL)
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

print(f"🚀 Memulai proses ingest pada {datetime.now()}...")

for name in target_tables:
    try:
        print(f"⏳ Mengambil data dari tabel: [Aktivitas].[{name}]...")

        query = f'SELECT * FROM "Aktivitas"."{name}"'
        df = pd.read_sql(query, engine)

        if df.empty:
            print(f"⚠️ Tabel {name} kosong, melewati proses upload.")
            continue

        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_buffer.seek(0)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        object_key = f"sql/{name}/{name}_{timestamp}.csv"

        s3.put_object(
            Bucket="raw-zone",
            Key=object_key,
            Body=csv_buffer
        )

        print(f"✅ Berhasil: {name} → {object_key}")

    except Exception as e:
        print(f"❌ Gagal memproses tabel {name}: {e}")

print(f"🏁 Semua proses selesai pada {datetime.now()}.")