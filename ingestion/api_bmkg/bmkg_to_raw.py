import requests
import boto3
from botocore.client import Config
from datetime import datetime
import json
import io

BMKG_URL = "https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4=63.03.02.1001"

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

resp = requests.get(BMKG_URL)
data = resp.json()

key = f"api/bmkg/bmkg_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

s3.put_object(
    Bucket=RAW_BUCKET,
    Key=key,
    Body=json.dumps(data, indent=2)
)

print(f"[RAW BMKG] uploaded → {key}")
