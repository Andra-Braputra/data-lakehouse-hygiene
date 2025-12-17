import pandas as pd
import boto3
from botocore.client import Config
from datetime import datetime
import requests
import io
import csv
import os

# ======================================================
# GOOGLE SHEETS
# ======================================================
SHEET_ID = os.getenv("SHEET_ID", "1rzafmIPkUhwoWoa8C2sygm6ch86K53N-zPIgXaPV_wo")

URLS = {
    "aktivitas_manual": f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=aktivitas_manual",
    "log_mandi": f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=log_mandi",
    }

# ======================================================
# MINIO (SMART CONFIG)
# ======================================================
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

# ======================================================
# CSV READER AMAN
# ======================================================
def read_sheet_csv(url):
    raw = requests.get(url).text
    raw = raw.replace("\ufeff", "").replace("\r\n", "\n")

    try:
        dialect = csv.Sniffer().sniff(raw[:2000])
        df = pd.read_csv(
            io.StringIO(raw),
            sep=dialect.delimiter,
            quotechar='"',
            engine="python",
            on_bad_lines="skip"
        )
        if df.shape[1] > 1:
            return df
    except Exception:
        pass

    df = pd.read_csv(
        io.StringIO(raw),
        sep=",",
        engine="python",
        on_bad_lines="skip"
    )

    if df.shape[1] <= 1:
        raise ValueError("CSV gagal diparse (masih 1 kolom)")

    return df

# ======================================================
# INGEST (DIRECT UPLOAD)
# ======================================================
print("==============================")
print("▶ Ingest Google Sheets → Raw Zone")
print(f"  Target MinIO: {MINIO_ENDPOINT}")
print("==============================")

for name, url in URLS.items():
    try:
        df = read_sheet_csv(url)
        print(f"[CHECK] {name} columns → {list(df.columns)}")

        filename = f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        key = f"sheets/{name}/{filename}"

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        s3.put_object(
            Bucket=RAW_BUCKET,
            Key=key,
            Body=csv_buffer.getvalue()
        )
        # -------------------------------------

        print(f"[RAW] {name} → {key}")

    except Exception as e:
        print(f"❌ Gagal ingest {name}: {e}")
        # Jangan raise error dulu biar sheet lain tetap dicoba
        # Tapi kalau mau strict (gagal satu gagal semua), uncomment baris bawah:
        # raise e

print("✅ INGESTION SHEETS SELESAI")