import json
import pandas as pd
import boto3
from botocore.client import Config
import io

MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin123"

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

def get_latest_json(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:
        raise FileNotFoundError(f"Tidak ada file di {prefix}")

    files = [obj for obj in response["Contents"] if obj["Key"].endswith(".json")]
    if not files:
        raise FileNotFoundError(f"Tidak ada JSON di {prefix}")

    latest = sorted(files, key=lambda x: x["LastModified"], reverse=True)[0]
    return latest["Key"]

# ===============================
# AMBIL RAW TERBARU
# ===============================
bmkg_key = get_latest_json(RAW_BUCKET, "api/bmkg/")
print(f"[RAW BMKG] pakai → {bmkg_key}")

obj = s3.get_object(Bucket=RAW_BUCKET, Key=bmkg_key)
raw = json.loads(obj["Body"].read())

# ===============================
# PARSE DATA
# ===============================
rows = []
for lokasi_data in raw.get("data", []):
    for cuaca_harian in lokasi_data.get("cuaca", []):
        for item in cuaca_harian:
            rows.append({
                "datetime": item.get("local_datetime"),
                "temperature": item.get("t"),
                "humidity": item.get("hu"),
                "weather_desc": item.get("weather_desc"),
                "wind_speed": item.get("ws")
            })

df = pd.DataFrame(rows)
df["datetime"] = pd.to_datetime(df["datetime"])
df = df.sort_values("datetime")

# ===============================
# SIMPAN CLEAN
# ===============================
buffer = io.StringIO()
df.to_csv(buffer, index=False)

s3.put_object(
    Bucket=CLEAN_BUCKET,
    Key="api/bmkg.csv",
    Body=buffer.getvalue()
)

print(f"[CLEAN BMKG] api/bmkg.csv ({len(df)} baris)")
