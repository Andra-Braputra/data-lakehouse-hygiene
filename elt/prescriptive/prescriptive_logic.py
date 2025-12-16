import pandas as pd
import boto3
from botocore.client import Config
import io
from datetime import datetime, timedelta  # <--- Tambah timedelta
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import os

# ======================================================
# KONFIGURASI ENV & MINIO
# ======================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

CLEAN_BUCKET = "clean-zone"
CURATED_BUCKET = "curated-zone"

# Config Delta Lake
storage_options = {
    "AWS_ACCESS_KEY_ID": ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
}

# ======================================================
# HELPER FUNCTIONS
# ======================================================
def read_data_delta(bucket, path):
    """Membaca folder Delta Table ke Pandas DataFrame"""
    full_path = f"s3://{bucket}/{path}"
    print(f"📂 Membaca Delta: {full_path}")
    try:
        dt = DeltaTable(full_path, storage_options=storage_options)
        return dt.to_pandas()
    except Exception as e:
        print(f"⚠️ Gagal baca Delta '{path}': {e}")
        return pd.DataFrame()

# ======================================================
# LOAD DATA (SEMUA DARI CLEAN ZONE)
# ======================================================
print("--- LOAD DATA (PANDAS ENGINE) ---")

try:
    catatan = read_data_delta(CLEAN_BUCKET, "sheets/catatan_aktivitas")
    master = read_data_delta(CLEAN_BUCKET, "sheets/master_aktivitas")
    preferensi = read_data_delta(CLEAN_BUCKET, "sheets/preferensi")
    mandi = read_data_delta(CLEAN_BUCKET, "sql/log_mandi")
    bmkg = read_data_delta(CLEAN_BUCKET, "api/bmkg")
    aqi = read_data_delta(CLEAN_BUCKET, "api/aqi")
except Exception as e:
    print(f"❌ Error fatal membaca data: {e}")
    raise e

# ======================================================
# DATA PREPARATION & TIMEZONE FIX (MANUAL)
# ======================================================
# Docker biasanya UTC. Kita paksa tambah 8 jam (WITA) atau 7 jam (WIB)
# Ganti angka 8 dengan 7 jika kamu di Jakarta/WIB
waktu_sekarang = datetime.utcnow() + timedelta(hours=8) 

print(f"🕒 Waktu Run (WITA): {waktu_sekarang}")

# Parse Waktu
catatan["timestamp"] = pd.to_datetime(catatan["timestamp"], errors="coerce")

if not mandi.empty and "waktu_mandi" in mandi.columns:
    mandi["waktu_mandi"] = pd.to_datetime(mandi["waktu_mandi"], errors="coerce")
else:
    mandi = pd.DataFrame(columns=["waktu_mandi"])

# Ambil Data Cuaca Terkini
bmkg_latest = None
if not bmkg.empty:
    bmkg["datetime"] = pd.to_datetime(bmkg["datetime"], errors="coerce")
    bmkg_latest = bmkg.sort_values("datetime").iloc[-1]

aqi_latest = None
if not aqi.empty:
    aqi["datetime"] = pd.to_datetime(aqi["datetime"], errors="coerce")
    aqi_latest = aqi.sort_values("datetime").iloc[-1]

# ======================================================
# 1️⃣ LOGIKA WINDOW AKTIVITAS
# ======================================================
# Cari kapan terakhir mandi
if mandi.empty or mandi["waktu_mandi"].dropna().empty:
    print("⚠️ Belum pernah mandi, ambil aktivitas paling awal.")
    waktu_mandi_terakhir = catatan["timestamp"].min()
else:
    waktu_mandi_terakhir = mandi["waktu_mandi"].max()

# Handle NaT (Not a Time)
if pd.isna(waktu_mandi_terakhir):
    waktu_mandi_terakhir = waktu_sekarang

print(f"🚿 Mandi terakhir: {waktu_mandi_terakhir}")

# Filter aktivitas SETELAH mandi terakhir
aktivitas_window = catatan[catatan["timestamp"] > waktu_mandi_terakhir]
print(f"📦 Jumlah aktivitas baru: {len(aktivitas_window)}")

# ======================================================
# 2️⃣ PERHITUNGAN SKOR
# ======================================================
# --- A. Skor Kekotoran (Fisik) ---
if aktivitas_window.empty:
    skor_kekotoran_dasar = 0
else:
    df_kotor = aktivitas_window.merge(master, on="id_aktivitas", how="left")
    df_kotor["bobot_kotor"] = df_kotor["bobot_kotor"].fillna(0)
    # Rumus: Durasi * Bobot
    df_kotor["skor_kotor"] = (df_kotor["durasi_menit"] * df_kotor["bobot_kotor"])
    skor_kekotoran_dasar = min(df_kotor["skor_kotor"].sum() / 100, 10)

# Faktor Keringat (Suhu & Lembap)
temp_val = bmkg_latest["temperature"] if bmkg_latest is not None else 30
humid_val = bmkg_latest["humidity"] if bmkg_latest is not None else 80

faktor_keringat = ((temp_val / 35) * 0.6 + (humid_val / 100) * 0.4)
faktor_keringat = max(0.5, min(faktor_keringat, 1.5))

skor_kekotoran = round(skor_kekotoran_dasar * faktor_keringat, 2)

# --- B. Skor Bau ---
jam_sejak_mandi = (waktu_sekarang - waktu_mandi_terakhir).total_seconds() / 3600

if not aktivitas_window.empty:
    aktivitas_bau = aktivitas_window.merge(master, on="id_aktivitas", how="left")
    jumlah_aktivitas = len(aktivitas_bau)
else:
    jumlah_aktivitas = 0

faktor_lembap = humid_val / 100
skor_bau = (jam_sejak_mandi * 0.3 + jumlah_aktivitas * 0.5 + faktor_lembap * 2)
skor_bau = round(min(skor_bau, 10), 2)

# --- C. Skor AQI ---
aqi_val = aqi_latest["aqi"] if aqi_latest is not None else 0
skor_aqi = round(aqi_val / 50, 2)

# ======================================================
# 3️⃣ SKOR FINAL & KEPUTUSAN
# ======================================================
def get_pref(name, default):
    try:
        row = preferensi.loc[preferensi["parameter"] == name, "nilai"]
        return float(row.iloc[0]) if not row.empty else default
    except:
        return default

bobot_kotor_user = get_pref("bobot_kotor", 0.4)
bobot_bau_user = get_pref("bobot_bau", 0.3)
bobot_aqi_user = get_pref("bobot_aqi", 0.3)
threshold = get_pref("threshold_mandi", 6)

skor_final = round(
    skor_kekotoran * bobot_kotor_user +
    skor_bau * bobot_bau_user +
    skor_aqi * bobot_aqi_user, 2
)

if skor_final >= threshold:
    rekomendasi = "SEGERA MANDI"
elif skor_final >= threshold - 2:
    rekomendasi = "MANDI BISA DITUNDA"
else:
    rekomendasi = "TIDAK PERLU MANDI"

# ======================================================
# 4️⃣ SIMPAN KE CURATED (DELTA LAKE)
# ======================================================
hasil = pd.DataFrame([{
    "generated_at": waktu_sekarang, # <--- Sudah WITA (UTC+8)
    "waktu_mandi_terakhir": waktu_mandi_terakhir,
    "skor_kekotoran": skor_kekotoran,
    "skor_bau": skor_bau,
    "skor_aqi": skor_aqi,
    "skor_final": skor_final,
    "rekomendasi": rekomendasi,
    # Data Konteks (Untuk Grafik)
    "suhu_aktual": temp_val,
    "kelembaban_aktual": humid_val,
    "aqi_aktual": aqi_val
}])

path_curated = f"s3://{CURATED_BUCKET}/prescriptive/hasil_preskriptif"
print(f"💾 Menyimpan ke Curated Delta: {path_curated}")

write_deltalake(
    path_curated,
    hasil,
    mode="append", 
    storage_options=storage_options
)

print("✅ PRESCRIPTIVE FINAL SELESAI")
print(hasil.T)