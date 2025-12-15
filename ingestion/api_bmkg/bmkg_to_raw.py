import requests
import boto3
from botocore.client import Config
from datetime import datetime
import json
import os

# URL BMKG
BMKG_URL = "https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4=63.03.02.1001"

# ==========================================
# KONFIGURASI DINAMIS (PENTING!)
# ==========================================
# Jika di Airflow (Docker), dia akan pakai 'http://minio:9000'
# Jika di Laptop (Manual), dia pakai 'http://localhost:9000'
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
RAW_BUCKET = "raw-zone"

# Setup Client S3
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# Headers biar dianggap browser asli (Anti-Blokir)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

print("==============================")
print("▶ Ingest BMKG → Raw Zone")
print(f"  Target MinIO: {MINIO_ENDPOINT}")
print("==============================")

try:
    # 1. Request ke API BMKG
    print(f"🌍 Fetching data dari: {BMKG_URL}")
    resp = requests.get(BMKG_URL, headers=headers, timeout=30)
    resp.raise_for_status() # Error kalau status bukan 200 OK
    
    data = resp.json()

    # 2. Siapkan Key
    filename = f"bmkg_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    key = f"api/bmkg/{filename}"

    # 3. Upload ke MinIO
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2)
    )

    print(f"✅ [RAW BMKG] Berhasil upload → {key}")

except Exception as e:
    print(f"❌ GAGAL Ingest BMKG: {e}")
    # Raise error supaya Airflow sadar ini gagal (jadi Merah)
    raise e