import requests
import boto3
from botocore.client import Config
from datetime import datetime
import json
import os

BMKG_URL = "https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4=63.03.02.1001"

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

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

print("==============================")
print("▶ Ingest BMKG → Raw Zone")
print(f"  Target MinIO: {MINIO_ENDPOINT}")
print("==============================")

try:
    resp = requests.get(BMKG_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Path bersih: api/bmkg/bmkg.json
    key = "api/bmkg/bmkg.json"

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2),
        Metadata={
            "kategori_sumber": "api_bmkg",
            "format_file": "json",
            "nama_data": "prakiraan_cuaca",
            "waktu_ingest": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    )

    print(f"✅ [RAW BMKG] Berhasil upload → {key}")

except Exception as e:
    print(f"❌ GAGAL Ingest BMKG: {e}")
    raise e