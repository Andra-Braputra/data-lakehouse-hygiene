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
aqi_key = get_latest_json(RAW_BUCKET, "api/aqicn/")
print(f"[RAW AQICN] pakai → {aqi_key}")

obj = s3.get_object(Bucket=RAW_BUCKET, Key=aqi_key)
raw = json.loads(obj["Body"].read())

if raw.get("status") != "ok":
    raise ValueError("Status AQICN tidak OK")

data = raw.get("data", {})

# ===============================
# NORMALISASI
# ===============================
df = pd.DataFrame([{
    "datetime": data.get("time", {}).get("s"),
    "aqi": data.get("aqi"),
    "pm25": data.get("iaqi", {}).get("pm25", {}).get("v"),
    "pm10": data.get("iaqi", {}).get("pm10", {}).get("v"),
    "dominant_pollutant": data.get("dominentpol")
}])

df["datetime"] = pd.to_datetime(df["datetime"])

# ===============================
# SIMPAN CLEAN
# ===============================
buffer = io.StringIO()
df.to_csv(buffer, index=False)

s3.put_object(
    Bucket=CLEAN_BUCKET,
    Key="api/aqi.csv",
    Body=buffer.getvalue()
)

print("[CLEAN AQICN] api/aqi.csv berhasil dibuat")
