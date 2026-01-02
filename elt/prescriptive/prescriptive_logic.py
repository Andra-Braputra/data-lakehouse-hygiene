import pandas as pd
import boto3
from botocore.client import Config
import io
from datetime import datetime, timedelta
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

# Inisialisasi Boto3 Client untuk MinIO
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

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
        # Menghapus timezone (tz-naive) agar sinkron dengan jam lokal
        df = dt.to_pandas()
        for col in df.select_dtypes(include=['datetime64[ns, UTC]', 'datetimetz']).columns:
            df[col] = df[col].dt.tz_localize(None)
        return df
    except Exception as e:
        print(f"   ⚠️ Gagal baca Delta '{path}': {e}")
        return pd.DataFrame()

# ======================================================
# 2. LOAD DATA & FIX TIMEZONE
# ======================================================
print("\n--- [START] LOAD CLEAN DATA ---")

# Menggunakan jam lokal (WITA = UTC+8). 
# Jika datetime.now() kamu masih jam 10, kita paksa tambah 8 jam.
waktu_sekarang = datetime.utcnow() + timedelta(hours=8)

catatan = read_data_delta(CLEAN_BUCKET, "sheets/catatan_aktivitas")
master = read_data_delta(CLEAN_BUCKET, "sql/aktivitas_joined_master")
mandi = read_data_delta(CLEAN_BUCKET, "sheets/log_mandi")
bmkg = read_data_delta(CLEAN_BUCKET, "api/bmkg")
aqi = read_data_delta(CLEAN_BUCKET, "api/aqi")

# Pastikan semua kolom timestamp bersifat naive (tanpa TZ) agar tidak minus saat dikurangi
for df in [catatan, mandi]:
    if not df.empty and "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)

bmkg_latest = bmkg.sort_values("datetime").iloc[-1] if not bmkg.empty else None
aqi_latest = aqi.sort_values("datetime").iloc[-1] if not aqi.empty else None

# ======================================================
# 3. WINDOW AKTIVITAS
# ======================================================
waktu_mandi_terakhir = mandi["timestamp"].max() if not mandi.empty else catatan["timestamp"].min()

if pd.isna(waktu_mandi_terakhir): 
    waktu_mandi_terakhir = waktu_sekarang - timedelta(hours=6)

# Mencegah nilai minus jika data log mandi 'lebih depan' dari jam sistem
if waktu_mandi_terakhir > waktu_sekarang:
    waktu_mandi_terakhir = waktu_sekarang

print(f"🚿 Mandi terakhir: {waktu_mandi_terakhir}")
print(f"🕒 Waktu sistem: {waktu_sekarang}")

aktivitas_window = catatan[catatan["timestamp"] > waktu_mandi_terakhir].copy()

# ======================================================
# 4. SKOR KEKOTORAN
# ======================================================
skor_kekotoran = 0
if not aktivitas_window.empty and not master.empty:
    df_kotor = aktivitas_window.merge(master, on="id_aktivitas", how="left")
    
    temp_factor = (bmkg_latest["temperature"] / 25) if bmkg_latest is not None else 1.0
    aqi_factor = (aqi_latest["aqi"] / 50) if aqi_latest is not None else 1.0
    faktor_outdoor = max(1.0, (temp_factor * 0.6) + (aqi_factor * 0.4))

    def hitung_per_baris(row):
        base = row["durasi_menit"] * (row.get("skor_met", 1.0) / 10)
        kategori = str(row.get("nama_kategori", "")).lower()
        return base * faktor_outdoor if "outdoor" in kategori else base

    df_kotor["skor_individu"] = df_kotor.apply(hitung_per_baris, axis=1)
    skor_kekotoran = round(min(df_kotor["skor_individu"].sum() / 15, 10), 2)

# ======================================================
# 5. SKOR BAU BADAN (DISESUAIKAN TANPA BOBOT_BAU)
# ======================================================

# 1. Hitung jam sejak mandi (Safety check agar tidak minus)
jam_sejak_mandi = (waktu_sekarang - waktu_mandi_terakhir).total_seconds() / 3600
if jam_sejak_mandi < 0: jam_sejak_mandi = 0

# 2. Filter aktivitas yang memicu bau
# Kita asumsikan aktivitas bau adalah yang Skor MET > 3 (Aktivitas sedang-berat)
# atau yang mengandung kata 'outdoor' di kategorinya.
aktivitas_analisis = aktivitas_window.merge(master, on="id_aktivitas", how="left")

def filter_bau(row):
    # Kriteria 1: MET Tinggi (biasanya > 3.0 mulai berkeringat)
    met_tinggi = row.get("skor_met", 0) > 3.0
    # Kriteria 2: Lokasi Outdoor
    is_outdoor = "outdoor" in str(row.get("nama_kategori", "")).lower()
    return met_tinggi or is_outdoor

# Terapkan filter
aktivitas_bau = aktivitas_analisis[aktivitas_analisis.apply(filter_bau, axis=1)]
jumlah_aktivitas_bau = len(aktivitas_bau)

# 3. Faktor kelembapan (Max skor kontribusi = 2.0)
faktor_lembap = bmkg_latest["humidity"] / 100 if bmkg_latest is not None else 0.5

# 4. Rumus Final Skor Bau (Disesuaikan)
skor_bau = (
    (jam_sejak_mandi * 0.3) +        # 10 jam = 3.0 poin
    (jumlah_aktivitas_bau * 0.7) +    # 1 aktivitas berkeringat = 0.7 poin
    (faktor_lembap * 2)               # Kelembapan lingkungan
)

# Batasi maksimal 10
skor_bau = round(min(skor_bau, 10), 2)

# ======================================================
# 6. SKOR AQI & FINAL
# ======================================================
skor_aqi = round(min(aqi_latest["aqi"] / 50, 10), 2) if aqi_latest is not None else 0

B_KOTOR, B_BAU, B_AQI, THRESHOLD = 0.4, 0.4, 0.2, 6.0
skor_final = round((skor_kekotoran * B_KOTOR) + (skor_bau * B_BAU) + (skor_aqi * B_AQI), 2)

# Logika Rekomendasi yang lebih masuk akal
if skor_final >= THRESHOLD:
    rekomendasi = "WAJIB MANDI SEKARANG"
    penjelasan = "Skor kebersihan kritis. Tubuh sudah sangat kotor dan berbau."
elif skor_final >= THRESHOLD - 1.0:
    rekomendasi = "SANGAT DISARANKAN"
    penjelasan = "Kondisi mulai tidak nyaman. Disarankan segera bilas tubuh."
elif skor_final >= THRESHOLD - 2.0:
    rekomendasi = "MANDI BISA DITUNDA"
    penjelasan = "Kondisi masih oke, tapi perhatikan akumulasi aktivitas."
else:
    rekomendasi = "Tidak Perlu Mandi"
    penjelasan = "Tubuh dalam kondisi prima. Tetap pertahankan higinitas."

# Safety Catch untuk parameter ekstrem
if skor_bau >= 9.0:
    rekomendasi = "WAJIB MANDI (Faktor Bau)"
    penjelasan = "Meskipun skor rata-rata aman, bau badan sudah mencapai ambang batas."

# ======================================================
# 8. SIMPAN MENGGUNAKAN IO & BOTO3 (Backup/Log CSV)
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
    "generated_at": waktu_sekarang
}])

# Contoh penggunaan Boto3 + IO untuk simpan CSV sebagai log tambahan
csv_buffer = io.StringIO()
hasil_df.to_csv(csv_buffer, index=False)
s3_client.put_object(
    Bucket=CURATED_BUCKET, 
    Key="logs/last_calculation.csv", 
    Body=csv_buffer.getvalue()
)

# Tetap simpan ke Delta Lake untuk kebutuhan dashboard
write_deltalake(
    f"s3://{CURATED_BUCKET}/prescriptive_hygiene",
    hasil_df,
    mode="overwrite",
    schema_mode="overwrite",
    storage_options=storage_options
)

print(f"✅ SELESAI: {rekomendasi} | Skor Bau: {skor_bau} | Selisih Jam: {round(jam_sejak_mandi, 2)}")