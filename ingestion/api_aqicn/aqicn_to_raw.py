import requests
import boto3
from botocore.client import Config
from datetime import datetime
import json
import os

API_TOKEN = "aa8a605f3fa41c995a4160527119da7008c74494"
AQICN_URL = f"https://api.waqi.info/feed/A540724/?token={API_TOKEN}"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
RAW_BUCKET = "raw-zone"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

try:
    resp = requests.get(AQICN_URL)
    data = resp.json()

    # Path bersih tanpa timestamp
    key = "api/aqicn/aqicn.json"

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2),
        Metadata={
            "kategori_sumber": "api_aqicn",
            "format_file": "json",
            "nama_data": "aqicn_air_quality",
            "waktu_ingest": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    )

    print(f"[RAW AQICN] uploaded → {key} (Metadata updated)")

except Exception as e:
    print(f"❌ Error AQICN: {e}")
    raise e