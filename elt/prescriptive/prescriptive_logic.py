import pandas as pd
import boto3
from botocore.client import Config
import io
from datetime import datetime
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import os

# ======================================================
# 1. KONFIGURASI & KONEKSI
# ======================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

CLEAN_BUCKET = "clean-zone"
CURATED_BUCKET = "curated-zone"

storage_options = {
    "AWS_ACCESS_KEY_ID": ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
}

def read_data_delta(bucket, path):
    full_path = f"s3://{bucket}/{path}"
    try:
        dt = DeltaTable(full_path, storage_options=storage_options)
        return dt.to_pandas()
    except Exception as e:
        print(f"   ⚠️ Gagal baca Delta '{path}': {e}")
        return pd.DataFrame()

# ======================================================
# 2. LOAD CLEAN DATA (Tanpa Tabel Preferensi)
# ======================================================
print("\n--- [START] LOAD CLEAN DATA ---")

catatan = read_data_delta(CLEAN_BUCKET, "sheets/catatan_aktivitas")
master = read_data_delta(CLEAN_BUCKET, "sql/aktivitas_joined_master")
mandi = read_data_delta(CLEAN_BUCKET, "sheets/log_mandi")
bmkg = read_data_delta(CLEAN_BUCKET, "api/bmkg")
aqi = read_data_delta(CLEAN_BUCKET, "api/aqi")

catatan["timestamp"] = pd.to_datetime(catatan["timestamp"], errors="coerce")
if not mandi.empty:
    mandi["timestamp"] = pd.to_datetime(mandi["timestamp"], errors="coerce")

bmkg_latest = bmkg.sort_values("datetime").iloc[-1] if not bmkg.empty else None
aqi_latest = aqi.sort_values("datetime").iloc[-1] if not aqi.empty else None

# ======================================================
# 3. WINDOW AKTIVITAS
# ======================================================
waktu_mandi_terakhir = mandi["timestamp"].max() if not mandi.empty else catatan["timestamp"].min()
if pd.isna(waktu_mandi_terakhir): 
    waktu_mandi_terakhir = datetime.now()

print(f"🚿 Mandi terakhir: {waktu_mandi_terakhir}")
aktivitas_window = catatan[catatan["timestamp"] > waktu_mandi_terakhir].copy()

# ======================================================
# 4. SKOR KEKOTORAN (Indoor & Outdoor)
# ======================================================
skor_kekotoran = 0
if not aktivitas_window.empty and not master.empty:
    df_kotor = aktivitas_window.merge(master, on="id_aktivitas", how="left")
    
    temp_factor = (bmkg_latest["temperature"] / 25) if bmkg_latest is not None else 1.0
    aqi_factor = (aqi_latest["aqi"] / 50) if aqi_latest is not None else 1.0
    faktor_outdoor = max(1.0, (temp_factor * 0.6) + (aqi_factor * 0.4))

    def hitung_per_baris(row):
        base = row["durasi_menit"] * (row["skor_met"] / 10)
        kategori = str(row.get("nama_kategori", "")).lower()
        return base * faktor_outdoor if "outdoor" in kategori else base

    df_kotor["skor_individu"] = df_kotor.apply(hitung_per_baris, axis=1)
    skor_kekotoran = round(min(df_kotor["skor_individu"].sum() / 15, 10), 2)

# ======================================================
# 5. SKOR BAU OTOMATIS
# ======================================================
jam_sejak_mandi = (datetime.now() - waktu_mandi_terakhir).total_seconds() / 3600
skor_waktu = jam_sejak_mandi * 0.2
total_menit = aktivitas_window["durasi_menit"].sum() if not aktivitas_window.empty else 0
skor_aktivitas = total_menit * 0.05
kelembapan = (bmkg_latest["humidity"] / 100) if bmkg_latest is not None else 0.5
skor_bau = round(min(skor_waktu + skor_aktivitas + (kelembapan * 2), 10), 2)

# ======================================================
# 6. SKOR AQI
# ======================================================
skor_aqi = round(min(aqi_latest["aqi"] / 50, 10), 2) if aqi_latest is not None else 0

# ======================================================
# 7. FINAL REKOMENDASI (HARDCODED PREFERENCES)
# ======================================================
# Nilai preferensi sekarang di-hardcode di sini
B_KOTOR = 0.4
B_BAU = 0.4
B_AQI = 0.2
THRESHOLD = 6.5

skor_final = round((skor_kekotoran * B_KOTOR) + (skor_bau * B_BAU) + (skor_aqi * B_AQI), 2)

# Logika Rekomendasi yang lebih masuk akal
if skor_final >= THRESHOLD:
    rekomendasi = "WAJIB MANDI SEKARANG"
    penjelasan = "Skor kebersihan kritis. Tubuh sudah sangat kotor dan berbau."
elif skor_final >= THRESHOLD - 1.0:
    rekomendasi = "SANGAT DISARANKAN"
    penjelasan = "Kondisi mulai tidak nyaman. Disarankan segera bilas tubuh."
elif skor_final >= THRESHOLD - 2.5:
    rekomendasi = "MANDI BISA DITUNDA"
    penjelasan = "Kondisi masih oke, tapi perhatikan akumulasi aktivitas."
else:
    rekomendasi = "MASIH SEGAR"
    penjelasan = "Tubuh dalam kondisi prima. Tetap pertahankan higinitas."

# Safety Catch untuk parameter ekstrem
if skor_bau >= 9.0:
    rekomendasi = "WAJIB MANDI (Faktor Bau)"
    penjelasan = "Meskipun skor rata-rata aman, bau badan sudah mencapai ambang batas."

# ======================================================
# 8. SIMPAN KE CURATED ZONE
# ======================================================
hasil_df = pd.DataFrame([{
    "waktu_mandi_terakhir": waktu_mandi_terakhir,
    "jam_sejak_mandi": round(jam_sejak_mandi, 1),
    "skor_kekotoran": skor_kekotoran,
    "skor_bau": skor_bau,
    "skor_aqi": skor_aqi,
    "skor_final": skor_final,
    "rekomendasi": rekomendasi,
    "penjelasan": penjelasan,
    "generated_at": datetime.now()
}])

write_deltalake(
    f"s3://{CURATED_BUCKET}/prescriptive_hygiene",
    hasil_df,
    mode="overwrite",
    schema_mode="overwrite",
    storage_options=storage_options
)

print(f"✅ PROSES SELESAI: {rekomendasi} (Skor: {skor_final})")