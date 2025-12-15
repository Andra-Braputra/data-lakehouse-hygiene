import requests
import boto3
from botocore.client import Config
from datetime import datetime
import json

API_TOKEN = "aa8a605f3fa41c995a4160527119da7008c74494"
AQICN_URL = f"https://api.waqi.info/feed/A540724/?token={API_TOKEN}"

MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin123"
RAW_BUCKET = "raw-zone"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

resp = requests.get(AQICN_URL)
data = resp.json()

key = f"api/aqicn/aqicn_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

s3.put_object(
    Bucket=RAW_BUCKET,
    Key=key,
    Body=json.dumps(data, indent=2)
)

print(f"[RAW AQICN] uploaded → {key}")
