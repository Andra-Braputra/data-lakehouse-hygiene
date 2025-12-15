import pandas as pd
import boto3
from botocore.client import Config
import io
import os
from deltalake.writer import write_deltalake

# ======================================================
# MINIO
# ======================================================
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

storage_options = {
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin123",
    "AWS_ENDPOINT_URL": "http://localhost:9000",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",  # Wajib untuk MinIO
    "AWS_REGION": "us-east-1",
}

# ======================================================
# HELPER
# ======================================================
def get_latest_csv(bucket, prefix):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [f for f in resp.get("Contents", []) if f["Key"].endswith(".csv")]
    if not files:
        raise FileNotFoundError(prefix)
    return sorted(files, key=lambda x: x["LastModified"], reverse=True)[0]["Key"]

def read_csv(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

def normalize_columns(df):
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace("\ufeff", "", regex=False)
    )
    return df

def save_to_delta(df, bucket, table_name):
    # Path format: s3://bucket/folder
    path = f"s3://{bucket}/{table_name}"
    
    print(f"   💾 Menyimpan ke Delta Table: {path} ...")
    
    write_deltalake(
        path,
        df,
        mode="overwrite",  # Atau 'append' jika ingin menumpuk
        storage_options=storage_options
    )

# ======================================================
# LOAD RAW
# ======================================================
print("==============================")
print("▶ Clean Google Sheets")
print("==============================")

catatan_key = get_latest_csv(RAW_BUCKET, "sheets/catatan_aktivitas/")
master_key = get_latest_csv(RAW_BUCKET, "sheets/master_aktivitas/")
preferensi_key = get_latest_csv(RAW_BUCKET, "sheets/preferensi/")

print(f"[RAW] catatan    → {catatan_key}")
print(f"[RAW] master     → {master_key}")
print(f"[RAW] preferensi → {preferensi_key}")

catatan = normalize_columns(read_csv(RAW_BUCKET, catatan_key))
master = normalize_columns(read_csv(RAW_BUCKET, master_key))
preferensi = normalize_columns(read_csv(RAW_BUCKET, preferensi_key))

print("Kolom catatan:", list(catatan.columns))

# ======================================================
# CLEAN CATATAN AKTIVITAS
# ======================================================
# auto detect kolom waktu
time_candidates = ["timestamp", "waktu", "tanggal", "datetime"]
time_col = next((c for c in time_candidates if c in catatan.columns), None)

if not time_col:
    raise ValueError("Kolom waktu tidak ditemukan di catatan aktivitas")

catatan = catatan.rename(columns={time_col: "timestamp"})
catatan["timestamp"] = pd.to_datetime(catatan["timestamp"], errors="coerce")
catatan["durasi_menit"] = pd.to_numeric(catatan["durasi_menit"], errors="coerce")
catatan = catatan.dropna(subset=["timestamp", "id_aktivitas"])

# ======================================================
# CLEAN MASTER AKTIVITAS (VERSI FINAL)
# ======================================================

required_cols = ["id_aktivitas", "nama_aktivitas", "bobot_kotor", "bobot_bau"]

missing = [c for c in required_cols if c not in master.columns]
if missing:
    raise ValueError(f"Kolom master_aktivitas kurang: {missing}")

master["bobot_kotor"] = pd.to_numeric(master["bobot_kotor"], errors="coerce")
master["bobot_bau"] = pd.to_numeric(master["bobot_bau"], errors="coerce")

master = master.dropna(subset=["id_aktivitas", "bobot_kotor", "bobot_bau"])


# ======================================================
# CLEAN PREFERENSI (SUDAH FIX STRUKTUR)
# ======================================================
preferensi = preferensi[["parameter", "nilai"]]
preferensi["nilai"] = pd.to_numeric(preferensi["nilai"], errors="coerce")
preferensi = preferensi.dropna()

# ======================================================
# SIMPAN CLEAN
# ======================================================
# Perhatikan nama file tidak pakai .csv lagi, tapi jadi folder tabel
save_to_delta(catatan, CLEAN_BUCKET, "sheets/catatan_aktivitas")
save_to_delta(master, CLEAN_BUCKET, "sheets/master_aktivitas")
save_to_delta(preferensi, CLEAN_BUCKET, "sheets/preferensi")

print("✅ CLEAN SHEETS SELESAI (Saved as Delta Lake)")
