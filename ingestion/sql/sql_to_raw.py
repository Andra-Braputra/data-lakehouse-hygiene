import pandas as pd
from sqlalchemy import create_engine
import boto3
from botocore.client import Config
from datetime import datetime
import io

# ======================================================
# KONFIGURASI NEON (HAPUS channel_binding)
# ======================================================
DATABASE_URL = (
    "postgresql://neondb_owner:npg_B7ysWvCoLix2@ep-still-surf-ad9wrml7-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
)

engine = create_engine(DATABASE_URL)

# ======================================================
# AMBIL DATA (FULL SNAPSHOT)
# ======================================================
query = """
SELECT
    waktu_mandi,
    tingkat_bau_badan
FROM log_mandi
ORDER BY waktu_mandi
"""

df = pd.read_sql(query, engine)

# ======================================================
# KONEKSI MINIO
# ======================================================
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# ======================================================
# SIMPAN KE RAW ZONE (TANPA FILE LOKAL)
# ======================================================
buffer = io.StringIO()
df.to_csv(buffer, index=False)

object_key = (
    f"sql/log_mandi/"
    f"log_mandi_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
)

s3.put_object(
    Bucket="raw-zone",
    Key=object_key,
    Body=buffer.getvalue()
)

print(f"[RAW SQL] berhasil disimpan → {object_key}")
