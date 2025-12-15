import pandas as pd
import boto3
from botocore.client import Config
import io
from datetime import datetime

# ======================================================
# KONFIGURASI MINIO
# ======================================================
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin123"

CLEAN_BUCKET = "clean-zone"
CURATED_BUCKET = "curated-zone"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# ======================================================
# HELPER
# ======================================================
def read_csv(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

def write_csv(df, bucket, key):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

# ======================================================
# LOAD CLEAN DATA
# ======================================================
catatan = read_csv(CLEAN_BUCKET, "sheets/catatan_aktivitas.csv")
master = read_csv(CLEAN_BUCKET, "sheets/master_aktivitas.csv")
preferensi = read_csv(CLEAN_BUCKET, "sheets/preferensi.csv")

try:
    mandi = read_csv(CLEAN_BUCKET, "sql/log_mandi.csv")
except:
    mandi = pd.DataFrame(columns=["waktu_mandi"])

try:
    bmkg = read_csv(CLEAN_BUCKET, "api/bmkg.csv")
except:
    bmkg = pd.DataFrame()

try:
    aqi = read_csv(CLEAN_BUCKET, "api/aqi.csv")
except:
    aqi = pd.DataFrame()

# ======================================================
# PARSE WAKTU
# ======================================================
catatan["timestamp"] = pd.to_datetime(catatan["timestamp"], errors="coerce")

if not mandi.empty:
    mandi["waktu_mandi"] = pd.to_datetime(mandi["waktu_mandi"], errors="coerce")

bmkg_latest = None
if not bmkg.empty:
    bmkg["datetime"] = pd.to_datetime(bmkg["datetime"], errors="coerce")
    bmkg_latest = bmkg.sort_values("datetime").iloc[-1]

aqi_latest = None
if not aqi.empty:
    aqi["datetime"] = pd.to_datetime(aqi["datetime"], errors="coerce")
    aqi_latest = aqi.sort_values("datetime").iloc[-1]

# ======================================================
# 1️⃣ WINDOW AKTIVITAS
# ======================================================
if mandi.empty or mandi["waktu_mandi"].dropna().empty:
    waktu_mandi_terakhir = catatan["timestamp"].min()
else:
    waktu_mandi_terakhir = mandi["waktu_mandi"].max()

aktivitas_window = catatan[catatan["timestamp"] > waktu_mandi_terakhir]

# ======================================================
# 2️⃣ SKOR KEKOTORAN (FISIK)
# ======================================================
if aktivitas_window.empty:
    skor_kekotoran_dasar = 0
else:
    df_kotor = aktivitas_window.merge(master, on="id_aktivitas", how="left")
    df_kotor["skor_kotor"] = (
        df_kotor["durasi_menit"] * df_kotor["bobot_kotor"]
    )
    skor_kekotoran_dasar = min(df_kotor["skor_kotor"].sum() / 100, 10)

# faktor keringat (BMKG)
if bmkg_latest is not None:
    faktor_keringat = (
        (bmkg_latest["temperature"] / 35) * 0.6
        + (bmkg_latest["humidity"] / 100) * 0.4
    )
    faktor_keringat = max(0.5, min(faktor_keringat, 1.5))
else:
    faktor_keringat = 1

skor_kekotoran = round(skor_kekotoran_dasar * faktor_keringat, 2)

# ======================================================
# 3️⃣ SKOR BAU BADAN (MODEL TERPISAH)
# ======================================================
jam_sejak_mandi = (
    (datetime.now() - waktu_mandi_terakhir).total_seconds() / 3600
)

aktivitas_bau = aktivitas_window.merge(master, on="id_aktivitas", how="left")
aktivitas_bau = aktivitas_bau[aktivitas_bau["bobot_bau"] <= 2]

jumlah_aktivitas_bau = len(aktivitas_bau)

faktor_lembap = bmkg_latest["humidity"] / 100 if bmkg_latest is not None else 0.5

skor_bau = (
    jam_sejak_mandi * 0.3
    + jumlah_aktivitas_bau * 0.5
    + faktor_lembap * 2
)

skor_bau = round(min(skor_bau, 10), 2)

# ======================================================
# 4️⃣ SKOR AQI
# ======================================================
skor_aqi = round(aqi_latest["aqi"] / 50, 2) if aqi_latest is not None else 0

# ======================================================
# 5️⃣ PREFERENSI
# ======================================================
def pref(name, default):
    row = preferensi.loc[preferensi["parameter"] == name, "nilai"]
    return float(row.iloc[0]) if not row.empty else default

bobot_kotor = pref("bobot_kotor", 0.4)
bobot_bau = pref("bobot_bau", 0.3)
bobot_aqi = pref("bobot_aqi", 0.3)
threshold = pref("threshold_mandi", 6)

# ======================================================
# 6️⃣ SKOR FINAL
# ======================================================
skor_final = round(
    skor_kekotoran * bobot_kotor
    + skor_bau * bobot_bau
    + skor_aqi * bobot_aqi,
    2
)

if skor_final >= threshold:
    rekomendasi = "SEGERA MANDI"
elif skor_final >= threshold - 2:
    rekomendasi = "MANDI BISA DITUNDA"
else:
    rekomendasi = "TIDAK PERLU MANDI"

# ======================================================
# 7️⃣ SIMPAN KE CURATED
# ======================================================
hasil = pd.DataFrame([{
    "waktu_mandi_terakhir": waktu_mandi_terakhir,
    "skor_kekotoran": skor_kekotoran,
    "skor_bau": skor_bau,
    "skor_aqi": skor_aqi,
    "skor_final": skor_final,
    "rekomendasi": rekomendasi,
    "generated_at": datetime.now()
}])

write_csv(hasil, CURATED_BUCKET, "prescriptive/hasil_preskriptif.csv")

print("✅ PRESCRIPTIVE FINAL BERHASIL (AUDIT CLEAN)")
print(hasil)
