import pandas as pd
import boto3
from botocore.client import Config
from datetime import datetime
import requests
import io
import csv

# ======================================================
# GOOGLE SHEETS
# ======================================================
SHEET_ID = "1rzafmIPkUhwoWoa8C2sygm6ch86K53N-zPIgXaPV_wo"

URLS = {
    "catatan_aktivitas": f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=aktivitas_manual",
    "master_aktivitas": f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=master_aktivitas",
    "preferensi": f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=preferensi",
}

# ======================================================
# MINIO
# ======================================================
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
# INGEST
# ======================================================
print("==============================")
print("▶ Ingest Google Sheets → Raw Zone")
print("==============================")

for name, url in URLS.items():
    df = read_sheet_csv(url)

    print(f"[CHECK] {name} columns → {list(df.columns)}")

    filename = f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    key = f"sheets/{name}/{filename}"

    df.to_csv(filename, index=False)
    s3.upload_file(filename, RAW_BUCKET, key)

    print(f"[RAW] {name} → {key}")

print("✅ INGESTION SHEETS SELESAI")
