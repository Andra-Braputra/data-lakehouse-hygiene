import pandas as pd
import boto3
from botocore.client import Config
from sqlalchemy import create_engine
import io

# ======================================================
# MINIO
# ======================================================
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin123"

CURATED_BUCKET = "curated-zone"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# ======================================================
# LOAD DATA DARI CURATED
# ======================================================
obj = s3.get_object(
    Bucket=CURATED_BUCKET,
    Key="prescriptive/hasil_preskriptif.csv"
)

df = pd.read_csv(io.BytesIO(obj["Body"].read()))

print("Data prescriptive:")
print(df)

# ======================================================
# NEON POSTGRES
# ======================================================
DATABASE_URL = (
    "postgresql://neondb_owner:npg_B7ysWvCoLix2@ep-still-surf-ad9wrml7-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
)

engine = create_engine(DATABASE_URL)

# ======================================================
# LOAD KE NEON
# ======================================================
df.to_sql(
    "hasil_preskriptif",
    engine,
    if_exists="append",   # append biar historis
    index=False
)

print("✅ DATA PRESCRIPTIVE BERHASIL DILOAD KE NEON")
print("Tabel: hasil_preskriptif")
