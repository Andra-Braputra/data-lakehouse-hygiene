import subprocess
import sys

# ==================================================
# HELPER UNTUK JALANKAN SCRIPT
# ==================================================
def run(step_name, command):
    print(f"\n==============================")
    print(f"▶ {step_name}")
    print(f"==============================")
    result = subprocess.run(command, shell=True)

    if result.returncode != 0:
        print(f"\n❌ GAGAL di step: {step_name}")
        sys.exit(1)

    print(f"✅ SELESAI: {step_name}")


# ==================================================
# INGESTION → RAW ZONE
# ==================================================
run(
    "Ingest Google Sheets → Raw Zone",
    "python ingestion/sheets/sheets_to_raw.py"
)

run(
    "Ingest SQL (Neon) → Raw Zone",
    "python ingestion/sql/sql_to_raw.py"
)

run(
    "Ingest BMKG API → Raw Zone",
    "python ingestion/api_bmkg/bmkg_to_raw.py"
)

run(
    "Ingest AQICN API → Raw Zone",
    "python ingestion/api_aqicn/aqicn_to_raw.py"
)


# ==================================================
# CLEAN → CLEAN ZONE (AUTO-DETECT TERBARU)
# ==================================================
run(
    "Clean Google Sheets",
    "python elt/clean_sheets.py"
)

run(
    "Clean BMKG",
    "python elt/clean_bmkg.py"
)

run(
    "Clean AQICN",
    "python elt/clean_aqicn.py"
)

# (opsional tapi direkomendasikan)
run(
    "Clean SQL",
    "python elt/clean_sql.py"
)


# ==================================================
# PRESCRIPTIVE ANALYTICS → CURATED ZONE
# ==================================================
run(
    "Prescriptive Analytics (Decision Logic)",
    "python elt/prescriptive/prescriptive_logic.py"
)


# ==================================================
# LOAD KE SQL (NEON) → DASHBOARD
# ==================================================
run(
    "Load Prescriptive Result → Neon",
    "python elt/load/load_prescriptive_to_sql.py"
)

run(
    "Load Aktivitas Setelah Mandi → Neon",
    "python elt/load/load_aktivitas_to_neon.py"
)

print("\n🎉 PIPELINE SELESAI TANPA ERROR")
print("📊 Dashboard Metabase otomatis ter-update")
