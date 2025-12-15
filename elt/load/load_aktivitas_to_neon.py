import pandas as pd
import boto3
from botocore.client import Config
from sqlalchemy import create_engine
import io

# ======================
# MINIO
# ======================
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

def read_csv(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

# ======================
# LOAD DARI CLEAN ZONE
# ======================
catatan = read_csv("clean-zone", "sheets/catatan_aktivitas.csv")
master = read_csv("clean-zone", "sheets/master_aktivitas.csv")

catatan["timestamp"] = pd.to_datetime(catatan["timestamp"])

# ======================
# JOIN AKTIVITAS
# ======================
df = catatan.merge(
    master,
    on="id_aktivitas",
    how="left"
)

df = df[[
    "timestamp",
    "id_aktivitas",
    "nama_aktivitas",
    "durasi_menit",
    "bobot_kotor",
    "bobot_bau"
]]


# ======================
# NEON POSTGRES
# ======================
DATABASE_URL = "postgresql://neondb_owner:npg_B7ysWvCoLix2@ep-still-surf-ad9wrml7-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
engine = create_engine(DATABASE_URL)

df.to_sql(
    "aktivitas_setelah_mandi",
    engine,
    if_exists="replace",
    index=False
)

print("✅ Aktivitas berhasil dimuat ke Neon")
print(df.head())
