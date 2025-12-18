import pandas as pd
from sqlalchemy import create_engine
from deltalake import DeltaTable
import os

# ======================
# KONFIGURASI DINAMIS
# ======================
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://neondb_owner:npg_B7ysWvCoLix2@ep-still-surf-ad9wrml7-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require"
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
CLEAN_BUCKET = "clean-zone"

storage_options = {
    "AWS_ACCESS_KEY_ID": ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
}

def read_delta(path):
    """Membaca Delta Table dari MinIO"""
    full_path = f"s3://{CLEAN_BUCKET}/{path}"
    try:
        dt = DeltaTable(full_path, storage_options=storage_options)
        return dt.to_pandas()
    except Exception as e:
        print(f"⚠️ Gagal membaca Delta '{path}': {e}")
        return pd.DataFrame()

# ======================
# MAIN PROCESS
# ======================
print("--- START: LOADING FULL HISTORY TO NEON ---")

try:
    # 1. Baca Data dari Clean Zone (Catatan Aktivitas & Joined Master SQL)
    # Mengambil semua riwayat tanpa filter waktu mandi terakhir
    df_catatan = read_delta("sheets/catatan_aktivitas")
    df_master = read_delta("sql/aktivitas_joined_master") # Menggunakan data master hasil join SQL
    
    if df_catatan.empty:
        print("⚠️ Data catatan aktivitas kosong.")
        exit(0)

    # 2. Transformasi & Join
    df_catatan["timestamp"] = pd.to_datetime(df_catatan["timestamp"])

    # Menggabungkan semua riwayat catatan dengan master data aktivitas
    df_final = df_catatan.merge(
        df_master,
        on="id_aktivitas",
        how="left"
    )

    # 3. Pembersihan Kolom (Mengikuti data baru: skor_met dan nama_kategori)
    # Sesuai saran Anda: skor bau dihapus, tambah kategori indoor/outdoor (nama_kategori)
    df_final = df_final[[
        "timestamp",
        "id_aktivitas",
        "nama_aktivitas",
        "durasi_menit",
        "skor_met",       # Pengganti bobot kotor/bau
        "nama_kategori"   # Berisi informasi Indoor/Outdoor
    ]]

    print(f"📊 Total riwayat aktivitas yang akan dimuat: {len(df_final)} baris")

    # 4. Load ke Neon (Tabel Baru: riwayat_aktivitas_dashboard)
    print("🔌 Connecting to Neon DB...")
    engine = create_engine(DATABASE_URL)
    
    # Menggunakan 'replace' agar tabel di dashboard selalu sinkron dengan riwayat penuh di Lakehouse
    df_final.to_sql(
        "riwayat_aktivitas_dashboard",
        engine,
        if_exists="replace",
        index=False
    )
    
    print("✅ SUKSES: Tabel [riwayat_aktivitas_dashboard] berhasil diperbarui.")
    print(df_final.head())

except Exception as e:
    print(f"❌ GAGAL Load Riwayat to Neon: {e}")
    raise e